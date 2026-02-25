param(
    [switch]$SkipNBA,
    [switch]$SkipCBB,
    [switch]$SkipCombined,
    [switch]$NBAOnly,
    [switch]$CBBOnly,
    [switch]$CombinedOnly,
    [switch]$RefreshNBACache,

    # ✅ NEW: control combined filtering without editing the script
    [string]$Tiers = "A,B,C,D",
    [int]$MaxTickets = 3
)

# --- Force UTF-8 for this session (prevents UnicodeEncodeError + UI capture decode issues) ---
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8
try { chcp 65001 | Out-Null } catch { }
# -------------------------------------------------------------------------------------------

$ErrorActionPreference = "Stop"
$StartTime = Get-Date
$Date = Get-Date -Format "yyyy-MM-dd"
$Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  PROP PIPELINE MASTER RUN  |  $Timestamp" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$NBADir = Join-Path $Root "NbaPropPipelineA"
$CBBDir = Join-Path $Root "CBB2"
$OutDir = Join-Path $Root "outputs\$Date"

if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir | Out-Null }
Write-Host "Output folder: $OutDir" -ForegroundColor DarkGray
Write-Host ""

function Run-Step {
    param(
        [string]$Label,
        [string]$Dir,
        [string]$Cmd
    )

    Write-Host "  --> $Label" -ForegroundColor Yellow
    Push-Location $Dir
    try {
        Invoke-Expression "py -3.14 $Cmd"
        if ($LASTEXITCODE -ne 0) { throw "Exit code $LASTEXITCODE" }
        Write-Host "      OK" -ForegroundColor Green
    }
    finally {
        Pop-Location
    }
}

# Track which pipelines succeeded for combined step
$NBASuccess = $false
$CBBSuccess = $false

# ── NBA ───────────────────────────────────────────────────────────────────────
$RunNBA = (-not $SkipNBA) -and (-not $CBBOnly) -and (-not $CombinedOnly)
if ($RunNBA) {
    Write-Host "[ NBA - NbaPropPipelineA ]" -ForegroundColor Magenta
    Write-Host ""
    try {
        Run-Step "Step 1 - Fetch PrizePicks"      $NBADir ".\step1_fetch_prizepicks_api.py --league_id 7 --game_mode pickem --per_page 250 --max_pages 5 --sleep 2.0 --cooldown_seconds 90 --max_cooldowns 3 --jitter_seconds 10.0 --output step1_pp_props_today.csv"
        Run-Step "Step 2 - Attach Pick Types"     $NBADir ".\step2_attach_picktypes.py --input step1_pp_props_today.csv --output step2_with_picktypes.csv"
        Run-Step "Step 3 - Attach Defense"        $NBADir ".\step3_attach_defense.py --input step2_with_picktypes.csv --defense .\defense_team_summary.csv --output step3_with_defense.csv"

        # RefreshNBACache deletes the ESPN flat-file cache + id map
        if ($RefreshNBACache) {
            Write-Host "  --> Refresh ESPN cache (delete nba_espn_boxscore_cache.csv + idmap)" -ForegroundColor Yellow
            $EspnCache = Join-Path $NBADir "nba_espn_boxscore_cache.csv"
            $IdMap = Join-Path $NBADir "nba_to_espn_id_map.csv"
            Remove-Item $EspnCache -Force -ErrorAction SilentlyContinue
            Remove-Item $IdMap     -Force -ErrorAction SilentlyContinue
            Write-Host "      OK" -ForegroundColor Green
        }

        Run-Step "Step 4 - Attach Player Stats (ESPN)" $NBADir ".\step4_attach_player_stats_espn_cache.py --slate step3_with_defense.csv --out step4_with_stats.csv --season 2025-26 --date $Date --days 35 --cache nba_espn_boxscore_cache.csv --idmap nba_to_espn_id_map.csv --n 10 --sleep 0.8 --retries 4 --connect-timeout 8 --timeout 30 --debug-misses no_espn_player_debug.csv"
        Run-Step "Step 5 - Line Hit Rates"        $NBADir ".\step5_add_line_hit_rates.py --input step4_with_stats.csv --output step5_with_hit_rates.csv"
        Run-Step "Step 6 - Team Role Context"     $NBADir ".\step6_team_role_context.py --input step5_with_hit_rates.csv --output step6_with_team_role_context.csv"
        Run-Step "Step 7 - Rank Props"            $NBADir ".\step7_rank_props.py --input step6_with_team_role_context.csv --output step7_ranked_props.xlsx"

        # Force ALL sheet for consistency
        Run-Step "Step 8 - Direction Context"     $NBADir ".\step8_add_direction_context.py --input step7_ranked_props.xlsx --sheet ALL --output step8_all_direction_clean.xlsx"

        Run-Step "Step 9 - Build Tickets"         $NBADir ".\step9_build_tickets.py --input step8_all_direction_clean.xlsx --output best_tickets.xlsx --min_hit_rate 0.8 --legs 2,3,4"

        Copy-Item "$NBADir\step8_all_direction_clean.xlsx" "$OutDir\nba_ranked_$Date.xlsx" -Force -ErrorAction SilentlyContinue
        Copy-Item "$NBADir\best_tickets.xlsx"              "$OutDir\nba_tickets_$Date.xlsx" -Force -ErrorAction SilentlyContinue

        # Copy ESPN debug misses to output dir for review
        $MissesFile = Join-Path $NBADir "no_espn_player_debug.csv"
        if (Test-Path $MissesFile) {
            Copy-Item $MissesFile "$OutDir\nba_espn_misses_$Date.csv" -Force -ErrorAction SilentlyContinue
        }

        $NBASuccess = $true
        Write-Host ""
        Write-Host "  NBA done. Outputs saved to $OutDir" -ForegroundColor Green
    }
    catch {
        Write-Host "  NBA FAILED: $_" -ForegroundColor Red
    }
    Write-Host ""
}

# ── CBB ───────────────────────────────────────────────────────────────────────
$RunCBB = (-not $SkipCBB) -and (-not $NBAOnly) -and (-not $CombinedOnly)
if ($RunCBB) {
    Write-Host "[ CBB - CBB2 ]" -ForegroundColor Magenta
    Write-Host ""
    try {
        if (Test-Path (Join-Path $CBBDir "pp_cbb_scraper.py")) {
            Run-Step "Step 1 - Fetch PrizePicks (CBB)" $CBBDir ".\pp_cbb_scraper.py --out step1_cbb.csv"
        }

        if (Test-Path (Join-Path $CBBDir "cbb_step2_normalize.py")) {
            if (-not (Test-Path (Join-Path $CBBDir "step1_cbb.csv"))) {
                Write-Host "  WARNING: step1_cbb.csv missing; skipping normalize step." -ForegroundColor Yellow
            }
            else {
                Run-Step "Step 2 - Normalize (CBB)" $CBBDir ".\cbb_step2_normalize.py --input step1_cbb.csv --output step2_cbb.csv"
            }
        }

        # NOTE: This is still called "Step 3" in your runner, even if the file name says step5_attach_espn_ids.py
        if (Test-Path (Join-Path $CBBDir "step5_attach_espn_ids.py")) {
            Run-Step "Step 3 - Attach ESPN IDs (CBB)" $CBBDir ".\step5_attach_espn_ids.py --input step2_cbb.csv --output step3_cbb.csv"
        }
        else {
            if (Test-Path (Join-Path $CBBDir "step2_cbb.csv")) {
                Copy-Item (Join-Path $CBBDir "step2_cbb.csv") (Join-Path $CBBDir "step3_cbb.csv") -Force
                Write-Host "  --> Step 3 - Attach ESPN IDs (CBB) (skipped; copied step2->step3)" -ForegroundColor Yellow
                Write-Host "      OK" -ForegroundColor Green
            }
            else {
                throw "Missing step2_cbb.csv and no step5_attach_espn_ids.py present."
            }
        }

        Run-Step "Step 5b - Attach Boxscore Stats" $CBBDir ".\cbb_step5b_attach_boxscore_stats.py --input step3_cbb.csv --output step5b_cbb.csv"
        Run-Step "Step 6 - Rank Props"             $CBBDir ".\cbb_step6_rank_props.py --input step5b_cbb.csv --output step6_ranked_cbb.xlsx"
        Run-Step "Step 7 - Build Tickets"          $CBBDir ".\cbb_step7_build_tickets.py --input step6_ranked_cbb.xlsx --output cbb_tickets.xlsx --legs 3,4,5,6"

        if (Test-Path (Join-Path $CBBDir "attach_cbb_athlete_ids_FIXED.py")) {
            Run-Step "Step 5b+ - Attach Athlete IDs" $CBBDir ".\attach_cbb_athlete_ids_FIXED.py --input step5b_cbb.csv --master ncaa_mbb_athletes_master.csv --output step5b_cbb_with_ids.csv"
        }

        Copy-Item "$CBBDir\step6_ranked_cbb.xlsx"   "$OutDir\cbb_ranked_$Date.xlsx" -Force -ErrorAction SilentlyContinue
        Copy-Item "$CBBDir\cbb_tickets.xlsx"        "$OutDir\cbb_tickets_$Date.xlsx" -Force -ErrorAction SilentlyContinue
        Copy-Item "$CBBDir\step5b_cbb_with_ids.csv" "$OutDir\cbb_full_slate_$Date.csv" -Force -ErrorAction SilentlyContinue

        $CBBSuccess = $true
        Write-Host ""
        Write-Host "  CBB done. Outputs saved to $OutDir" -ForegroundColor Green
    }
    catch {
        Write-Host "  CBB FAILED: $_" -ForegroundColor Red
    }
    Write-Host ""
}

# ── COMBINED SLATE + TICKETS ──────────────────────────────────────────────────
$RunCombined = (-not $SkipCombined) -and (-not $NBAOnly) -and (-not $CBBOnly)

# For CombinedOnly mode, check if source files exist from previous runs
if ($CombinedOnly) {
    $NBASuccess = Test-Path "$NBADir\step8_all_direction_clean.xlsx"
    $CBBSuccess = Test-Path "$CBBDir\step6_ranked_cbb.xlsx"
    if (-not $NBASuccess) { Write-Host "  WARNING: NBA slate not found at $NBADir\step8_all_direction_clean.xlsx" -ForegroundColor Yellow }
    if (-not $CBBSuccess) { Write-Host "  WARNING: CBB slate not found at $CBBDir\step6_ranked_cbb.xlsx" -ForegroundColor Yellow }
}

# Only run combined when BOTH are present
if ($RunCombined -and $NBASuccess -and $CBBSuccess) {
    Write-Host "[ COMBINED SLATE + TICKETS ]" -ForegroundColor Magenta
    Write-Host ""

    $CombinedOut = "$Root\combined_slate_tickets_$Date.xlsx"
    $NBASlate = "$NBADir\step8_all_direction_clean.xlsx"
    $CBBSlate = "$CBBDir\step6_ranked_cbb.xlsx"

    # ✅ Avoid PermissionError from Excel/OneDrive locks by copying inputs first
    $NBASlateCopy = Join-Path $OutDir "nba_slate_for_combined_$Date.xlsx"
    $CBBSlateCopy = Join-Path $OutDir "cbb_slate_for_combined_$Date.xlsx"
    Copy-Item $NBASlate $NBASlateCopy -Force -ErrorAction Stop
    Copy-Item $CBBSlate $CBBSlateCopy -Force -ErrorAction Stop

    Write-Host "  --> Combined Slate + Tickets (NBA + CBB)" -ForegroundColor Yellow
    try {
        # ✅ IMPORTANT: tiers must be passed as ONE argument string
        py -3.14 "$Root\combined_slate_tickets.py" `
            --nba "$NBASlateCopy" `
            --cbb "$CBBSlateCopy" `
            --date $Date `
            --output "$CombinedOut" `
            --tiers "$Tiers" `
            --max-tickets $MaxTickets

        if ($LASTEXITCODE -ne 0) { throw "Exit code $LASTEXITCODE" }

        Copy-Item $CombinedOut "$OutDir\combined_slate_tickets_$Date.xlsx" -Force -ErrorAction SilentlyContinue
        Write-Host "      OK" -ForegroundColor Green
        Write-Host ""
        Write-Host "  Combined done. Saved to $CombinedOut" -ForegroundColor Green
    }
    catch {
        Write-Host "  COMBINED FAILED: $_" -ForegroundColor Red
    }
    Write-Host ""
}
elseif ($RunCombined) {
    Write-Host "[ COMBINED ] Skipped - requires BOTH NBA + CBB outputs." -ForegroundColor DarkGray
    Write-Host ""
}

$Elapsed = (Get-Date) - $StartTime
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ("  DONE  |  {0}  |  {1}" -f $Elapsed.ToString("mm\:ss"), $OutDir) -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

# Usage examples:
# .\run_daily.ps1                            # Full run (NBA + CBB + Combined)
# .\run_daily.ps1 -NBAOnly                   # NBA pipeline only
# .\run_daily.ps1 -NBAOnly -RefreshNBACache  # Force rebuild ESPN cache from scratch
# .\run_daily.ps1 -CBBOnly                   # CBB pipeline only
# .\run_daily.ps1 -CombinedOnly              # Re-run combined step using existing outputs
# .\run_daily.ps1 -CombinedOnly -Tiers "A,B,C,D" -MaxTickets 3