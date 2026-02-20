param(
    [switch]$SkipNBA,
    [switch]$SkipCBB,
    [switch]$NBAOnly,
    [switch]$CBBOnly
)

$ErrorActionPreference = "Stop"
$StartTime = Get-Date
$Date = Get-Date -Format "yyyy-MM-dd"
$Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  PROP PIPELINE MASTER RUN  |  $Timestamp" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

$Root   = Split-Path -Parent $MyInvocation.MyCommand.Path
$NBADir = Join-Path $Root "NbaPropPipelineA"
$CBBDir = Join-Path $Root "CBB2"
$OutDir = Join-Path $Root "outputs\$Date"

if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir | Out-Null }
Write-Host "Output folder: $OutDir" -ForegroundColor DarkGray
Write-Host ""

function Run-Step {
    param([string]$Label, [string]$Dir, [string]$Cmd)
    Write-Host "  --> $Label" -ForegroundColor Yellow
    Push-Location $Dir
    try {
        Invoke-Expression "py -3.14 $Cmd"
        if ($LASTEXITCODE -ne 0) { throw "Exit code $LASTEXITCODE" }
        Write-Host "      OK" -ForegroundColor Green
    } catch {
        Pop-Location
        throw
    }
    Pop-Location
}

# ── NBA ───────────────────────────────────────────────────────────────────────
$RunNBA = (-not $SkipNBA) -and (-not $CBBOnly)
if ($RunNBA) {
    Write-Host "[ NBA - NbaPropPipelineA ]" -ForegroundColor Magenta
    Write-Host ""
    try {
        Run-Step "Step 1 - Fetch PrizePicks" $NBADir ".\step1_fetch_prizepicks_api.py --league_id 7 --game_mode pickem --per_page 250 --max_pages 5 --sleep 2.0 --cooldown_seconds 90 --max_cooldowns 3 --jitter_seconds 10.0 --output step1_pp_props_today.csv"
        Run-Step "Step 2 - Attach Pick Types"  $NBADir ".\step2_attach_picktypes.py --input step1_pp_props_today.csv --output step2_with_picktypes.csv"
        Run-Step "Step 3 - Attach Defense"     $NBADir ".\step3_attach_defense.py --input step2_with_picktypes.csv --defense .\defense_team_summary.csv --output step3_with_defense.csv"
        Run-Step "Step 4 - Attach Player Stats" $NBADir ".\step4_attach_player_stats.py --input step3_with_defense.csv --output step4_with_stats.csv --season 2025-26 --cache-dir .\_nba_cache --timeout 120 --retries 6 --sleep 3.0"
        Run-Step "Step 5 - Line Hit Rates"     $NBADir ".\step5_add_line_hit_rates.py --input step4_with_stats.csv --output step5_with_hit_rates.csv"
        Run-Step "Step 6 - Team Role Context"  $NBADir ".\step6_team_role_context.py --input step5_with_hit_rates.csv --output step6_with_team_role_context.csv"
        Run-Step "Step 7 - Rank Props"         $NBADir ".\step7_rank_props.py --input step6_with_team_role_context.csv --output step7_ranked_props.xlsx"
        Run-Step "Step 8 - Direction Context"  $NBADir ".\step8_add_direction_context.py --input step7_ranked_props.xlsx --output step8_all_direction_clean.xlsx"
        Run-Step "Step 9 - Build Tickets"      $NBADir ".\step9_build_tickets.py --input step8_all_direction_clean.xlsx --output best_tickets.xlsx --min_hit_rate 0.8 --legs 2,3,4"
        Copy-Item "$NBADir\step8_all_direction_clean.xlsx" "$OutDir\nba_ranked_$Date.xlsx" -Force -ErrorAction SilentlyContinue
        Copy-Item "$NBADir\best_tickets.xlsx"              "$OutDir\nba_tickets_$Date.xlsx" -Force -ErrorAction SilentlyContinue
        Write-Host ""
        Write-Host "  NBA done. Outputs saved to $OutDir" -ForegroundColor Green
    } catch {
        Write-Host "  NBA FAILED: $_" -ForegroundColor Red
        Write-Host "  Continuing to CBB..." -ForegroundColor Yellow
    }
    Write-Host ""
}

# ── CBB ───────────────────────────────────────────────────────────────────────
$RunCBB = (-not $SkipCBB) -and (-not $NBAOnly)
if ($RunCBB) {
    Write-Host "[ CBB - CBB2 ]" -ForegroundColor Magenta
    Write-Host ""
    try {
        Run-Step "Step 1 - Fetch PrizePicks CBB"  $CBBDir ".\pp_cbb_scraper.py --out step1_cbb.csv"
        Run-Step "Step 2 - Normalize"             $CBBDir ".\cbb_step2_normalize.py --input step1_cbb.csv --output step2_cbb.csv"
        Run-Step "Step 3 - Attach ESPN IDs"       $CBBDir ".\step5_attach_espn_ids.py --input step2_cbb.csv --output step3_cbb.csv"
        Run-Step "Step 4 - Attach Boxscore Stats" $CBBDir ".\cbb_step5b_attach_boxscore_stats.py --input step3_cbb.csv --output step5b_cbb.csv"
        Run-Step "Step 5 - Rank Props"            $CBBDir ".\cbb_step6_rank_props.py --input step5b_cbb.csv --output step6_ranked_cbb.xlsx"
        Run-Step "Step 6 - Build Tickets" $CBBDir ".\cbb_step7_build_tickets.py --input step6_ranked_cbb.xlsx --output cbb_tickets.xlsx --legs 3,4,5,6"
        Copy-Item "$CBBDir\step6_ranked_cbb.xlsx" "$OutDir\cbb_ranked_$Date.xlsx" -Force -ErrorAction SilentlyContinue
        Copy-Item "$CBBDir\cbb_tickets.xlsx"      "$OutDir\cbb_tickets_$Date.xlsx" -Force -ErrorAction SilentlyContinue
        Write-Host ""
        Write-Host "  CBB done. Outputs saved to $OutDir" -ForegroundColor Green
    } catch {
        Write-Host "  CBB FAILED: $_" -ForegroundColor Red
    }
    Write-Host ""
}

$Elapsed = (Get-Date) - $StartTime
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  DONE  |  $($Elapsed.ToString('mm\:ss'))  |  $OutDir" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""
