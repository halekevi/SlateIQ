# run_soccer_pipeline.ps1  -  SlateIQ Soccer Pipeline
#
# Usage (run from Soccer\ root):
#   .\scripts\run_soccer_pipeline.ps1
#   .\scripts\run_soccer_pipeline.ps1 -SkipFetch
#   .\scripts\run_soccer_pipeline.ps1 -LeagueId 1234
#   .\scripts\run_soccer_pipeline.ps1 -NTeams 20

param(
    [switch]$SkipFetch,
    [string]$LeagueId = "",
    [int]$NTeams      = 15
)

$ErrorActionPreference = "Stop"

# ── Resolve paths ─────────────────────────────────────────────────────────────
# Support running from Soccer\ root OR from Soccer\scripts\
$ScriptDir   = $PSScriptRoot
$SoccerRoot  = if ((Split-Path $ScriptDir -Leaf) -eq "scripts") { Split-Path $ScriptDir -Parent } else { $ScriptDir }
$ScriptsDir  = Join-Path $SoccerRoot "scripts"
$OutputsDir  = Join-Path $SoccerRoot "outputs"
$CacheDir    = Join-Path $SoccerRoot "cache"
$Python      = "py"

if (-not (Test-Path $OutputsDir)) { New-Item -ItemType Directory -Path $OutputsDir -Force | Out-Null }

function Run-Step {
    param([string]$Label, [string]$Script, [string[]]$StepArgs)
    $tag     = "[ SlateIQ-Soccer-$Label ]"
    $fullPath = Join-Path $ScriptsDir $Script
    Write-Host ""
    Write-Host "$tag Starting..." -ForegroundColor Cyan
    Write-Host "        CMD: $Python `"$fullPath`" $($StepArgs -join ' ')" -ForegroundColor DarkGray
    & $Python $fullPath @StepArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Host "$tag FAILED (exit $LASTEXITCODE) - aborting." -ForegroundColor Red
        exit $LASTEXITCODE
    }
    Write-Host "$tag OK" -ForegroundColor Green
}

Set-Location $SoccerRoot

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  SlateIQ Soccer Pipeline" -ForegroundColor Cyan
Write-Host "  Root: $SoccerRoot" -ForegroundColor DarkGray
Write-Host "========================================" -ForegroundColor Cyan

# ── S1: Fetch PrizePicks ──────────────────────────────────────────────────────
if ($SkipFetch) {
    Write-Host ""
    Write-Host "[ SlateIQ-Soccer-S1 ] SKIPPED (--SkipFetch)" -ForegroundColor Yellow
    if (-not (Test-Path (Join-Path $OutputsDir "step1_soccer_props.csv"))) {
        Write-Host "[ SlateIQ-Soccer-S1 ] ERROR: outputs\step1_soccer_props.csv not found." -ForegroundColor Red
        exit 1
    }
} else {
    $s1args = [System.Collections.Generic.List[string]]@("--output", "$OutputsDir\step1_soccer_props.csv")
    if ($LeagueId -ne "") { $s1args.Add("--league_id"); $s1args.Add($LeagueId) }
    Run-Step "S1" "step1_fetch_prizepicks_soccer.py" $s1args.ToArray()
}

# ── S2: Attach Pick Types + ESPN IDs ─────────────────────────────────────────
Run-Step "S2" "step2_attach_picktypes_soccer.py" @(
    "--input",       "$OutputsDir\step1_soccer_props.csv",
    "--output",      "$OutputsDir\step2_soccer_picktypes.csv",
    "--idcache",     "$CacheDir\soccer_espn_id_cache.csv",
    "--rostercache", "$CacheDir\soccer_roster_cache.csv"
)

# ── S3: Attach Defense ────────────────────────────────────────────────────────
Run-Step "S3" "step3_attach_defense_soccer.py" @(
    "--input",   "$OutputsDir\step2_soccer_picktypes.csv",
    "--defense", "$CacheDir\soccer_defense_summary.csv",
    "--output",  "$OutputsDir\step3_soccer_with_defense.csv"
)

# ── S4: Attach Player Stats ───────────────────────────────────────────────────
Run-Step "S4" "step4_attach_player_stats_soccer.py" @(
    "--input",   "$OutputsDir\step3_soccer_with_defense.csv",
    "--cache",   "$CacheDir\soccer_stats_cache.csv",
    "--output",  "$OutputsDir\step4_soccer_with_stats.csv",
    "--workers", "6"
)

# ── S5: Line Hit Rates (L5 + L10) ─────────────────────────────────────────────
# --compute10 is now default=True in the script, but passed explicitly here
# to be unambiguous and forward-compatible.
Run-Step "S5" "step5_add_line_hit_rates_soccer.py" @(
    "--input",     "$OutputsDir\step4_soccer_with_stats.csv",
    "--output",    "$OutputsDir\step5_soccer_hit_rates.csv",
    "--compute10"
)

# ── S6: Team Role Context ─────────────────────────────────────────────────────
Run-Step "S6" "step6_team_role_context_soccer.py" @(
    "--input",  "$OutputsDir\step5_soccer_hit_rates.csv",
    "--output", "$OutputsDir\step6_soccer_role_context.csv"
)

# ── S7: Rank Props ────────────────────────────────────────────────────────────
Run-Step "S7" "step7_rank_props_soccer.py" @(
    "--input",   "$OutputsDir\step6_soccer_role_context.csv",
    "--output",  "$OutputsDir\step7_soccer_ranked.xlsx",
    "--n_teams", "$NTeams"
)

# ── S8: Direction Context + Clean XLSX ───────────────────────────────────────
# --xlsx produces the clean formatted workbook (step8_soccer_direction_clean.xlsx)
# which is what run_pipeline.ps1 and combined_slate_tickets.py consume.
Run-Step "S8" "step8_add_direction_context_soccer.py" @(
    "--input",  "$OutputsDir\step7_soccer_ranked.xlsx",
    "--sheet",  "ALL",
    "--output", "$OutputsDir\step8_soccer_direction.csv",
    "--xlsx",   "$OutputsDir\step8_soccer_direction_clean.xlsx"
)

# ── S9 DISABLED ───────────────────────────────────────────────────────────────
# Tickets are generated by combined_slate_tickets.py in run_pipeline.ps1
# Re-enable with: Run-Step "S9" "step9_build_tickets_soccer.py" @(...)

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  SlateIQ Soccer Pipeline COMPLETE" -ForegroundColor Green
Write-Host "  $OutputsDir\step7_soccer_ranked.xlsx" -ForegroundColor Green
Write-Host "  $OutputsDir\step8_soccer_direction_clean.xlsx" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
