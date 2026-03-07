# run_soccer_pipeline.ps1  - SlateIQ Soccer Pipeline

param(
    [switch]$SkipFetch,
    [string]$LeagueId = "",
    [int]$NTeams      = 15
)

$ErrorActionPreference = "Stop"
$PipelineDir = $PSScriptRoot
$Python      = "py"

function Run-Step {
    param([string]$Label, [string]$Script, [string[]]$StepArgs)
    $tag = "[ SlateIQ-Soccer-$Label ]"
    Write-Host ""
    Write-Host "$tag Starting..." -ForegroundColor Cyan
    & $Python (Join-Path $PipelineDir $Script) @StepArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Host "$tag FAILED (exit $LASTEXITCODE) - aborting." -ForegroundColor Red
        exit $LASTEXITCODE
    }
    Write-Host "$tag OK" -ForegroundColor Green
}

Set-Location $PipelineDir

# S1
if ($SkipFetch) {
    Write-Host "[ SlateIQ-Soccer-S1 ] SKIPPED" -ForegroundColor Yellow
    if (-not (Test-Path "s1_soccer_props.csv")) {
        Write-Host "[ SlateIQ-Soccer-S1 ] ERROR: s1_soccer_props.csv not found." -ForegroundColor Red
        exit 1
    }
} else {
    $s1args = [System.Collections.Generic.List[string]]@("--output", "s1_soccer_props.csv")
    if ($LeagueId -ne "") { $s1args.Add("--league_id"); $s1args.Add($LeagueId) }
    Run-Step "S1" "step1_fetch_prizepicks_soccer.py" $s1args.ToArray()
}

# S2
Run-Step "S2" "step2_attach_picktypes_soccer.py" @("--input", "s1_soccer_props.csv", "--output", "s2_soccer_picktypes.csv", "--idcache", "soccer_espn_id_cache.csv", "--rostercache", "soccer_roster_cache.csv")

# S3
Run-Step "S3" "step3_attach_defense_soccer.py" @("--input", "s2_soccer_picktypes.csv", "--defense", "soccer_defense_summary.csv", "--output", "s3_soccer_defense.csv")

# S4
Run-Step "S4" "step4_attach_player_stats_soccer.py" @("--input", "s3_soccer_defense.csv", "--cache", "soccer_stats_cache.csv", "--output", "s4_soccer_stats.csv", "--workers", "6")

# S5
Run-Step "S5" "step5_add_line_hit_rates_soccer.py" @("--input", "s4_soccer_stats.csv", "--output", "s5_soccer_hit_rates.csv")

# S6
Run-Step "S6" "step6_team_role_context_soccer.py" @("--input", "s5_soccer_hit_rates.csv", "--output", "s6_soccer_role_context.csv")

# S7
Run-Step "S7" "step7_rank_props_soccer.py" @("--input", "s6_soccer_role_context.csv", "--output", "s7_soccer_ranked.xlsx", "--n_teams", "$NTeams")

# S8
Run-Step "S8" "step8_add_direction_context_soccer.py" @("--input", "s7_soccer_ranked.xlsx", "--output", "s8_soccer_direction.csv", "--xlsx", "s8_soccer_direction_clean.xlsx")

# S9
Run-Step "S9" "step9_build_tickets_soccer.py" @("--input", "s8_soccer_direction_clean.xlsx", "--output", "s9_soccer_tickets.xlsx")

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host " SlateIQ Soccer Pipeline COMPLETE" -ForegroundColor Green
Write-Host "  s8_soccer_direction_clean.xlsx" -ForegroundColor Green
Write-Host "  s9_soccer_tickets.xlsx" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
