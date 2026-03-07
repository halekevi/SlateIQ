# ============================================================
#  PROP PIPELINE  -  Master Run Script  [MULTI-SPORT]
#
#  Usage:
#    .\run_pipeline.ps1                        # NBA + CBB + Combined
#    .\run_pipeline.ps1 -NBAOnly               # NBA only
#    .\run_pipeline.ps1 -CBBOnly               # CBB only
#    .\run_pipeline.ps1 -NHLOnly               # NHL only
#    .\run_pipeline.ps1 -MLBOnly               # MLB only
#    .\run_pipeline.ps1 -SoccerOnly            # Soccer only
#    .\run_pipeline.ps1 -WNBAOnly              # WNBA only (season-gated)
#    .\run_pipeline.ps1 -CombinedOnly          # Re-run combined using existing outputs
#    .\run_pipeline.ps1 -IncludeNHL            # NBA + CBB + NHL + Combined
#    .\run_pipeline.ps1 -IncludeMLB            # NBA + CBB + MLB + Combined
#    .\run_pipeline.ps1 -IncludeSoccer         # NBA + CBB + Soccer + Combined
#    .\run_pipeline.ps1 -IncludeNHL -IncludeMLB -IncludeSoccer   # All sports
#    .\run_pipeline.ps1 -SkipNBA -IncludeNHL -IncludeSoccer      # CBB + NHL + Soccer (NBA already ran)
#    .\run_pipeline.ps1 -SkipFetch             # Skip step1 fetch, re-run steps 2-9 for all sports
#    .\run_pipeline.ps1 -SkipFetch -IncludeNHL -IncludeSoccer   # Steps 2-9 for CBB+NHL+Soccer+NBA
#    .\run_pipeline.ps1 -RefreshCache          # Wipe + rebuild ESPN cache before NBA
#    .\run_pipeline.ps1 -ForceAll              # Re-run everything regardless
#    .\run_pipeline.ps1 -CacheAgeDays 7        # Auto-wipe cache if older than N days
# ============================================================
param(
    [string]$Date = "",                           # explicit date parameter (e.g., "2026-03-02")
    [string]$OddsApiKey = "",                     # The Odds API key (free at the-odds-api.com)
    [switch]$NBAOnly,
    [switch]$CBBOnly,
    [switch]$NHLOnly,
    [switch]$MLBOnly,
    [switch]$SoccerOnly,
    [switch]$WNBAOnly,
    [switch]$CombinedOnly,
    [switch]$IncludeNHL,
    [switch]$IncludeMLB,
    [switch]$IncludeSoccer,
    [switch]$SkipNBA,
    [switch]$SkipFetch,
    [switch]$RefreshCache,
    [switch]$ForceAll,
    [int]$CacheAgeDays = 7
)

$ErrorActionPreference = "Continue"

# ⭐ NEW: Use provided date or default to today
if (-not $Date) {
    $Date = Get-Date -Format "yyyy-MM-dd"
    Write-Host "  [Date] No date specified, using today: $Date" -ForegroundColor DarkGray
} else {
    # Accept the date as-is (user is responsible for format)
    # Just validate it's a reasonable date string
    if ($Date -match "^\d{4}-\d{2}-\d{2}$|^\d{1,2}/\d{1,2}/\d{4}$|^\d{1,2}-\d{1,2}-\d{4}$") {
        Write-Host "  [Date] Using specified date: $Date" -ForegroundColor Cyan
    } else {
        Write-Host "  [Date] ERROR: Invalid date format '$Date'." -ForegroundColor Red
        Write-Host "         Use formats like: 2026-03-02 or 03/02/2026" -ForegroundColor Red
        exit 1
    }
}

$StartTime = Get-Date

# -- Paths --------------------------------------------------------------------
$Root      = Split-Path -Parent $MyInvocation.MyCommand.Definition
$NBADir    = Join-Path $Root "NBA"
$CBBDir    = Join-Path $Root "CBB"
$NHLDir    = Join-Path $Root "NHL"
$MLBDir    = Join-Path $Root "MLB"
$SoccerDir = Join-Path $Root "Soccer"
$WNBADir   = Join-Path $Root "WNBA"
$OutDir    = Join-Path $Root "outputs\$Date"
$WebOutDir = Join-Path $Root "ui_runner\templates"

if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Force -Path $OutDir | Out-Null }

# -- Encoding -----------------------------------------------------------------
$env:PYTHONUTF8       = "1"
$env:PYTHONIOENCODING = "utf-8"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
try { chcp 65001 | Out-Null } catch { }

# -- Activate venv ------------------------------------------------------------
if (Test-Path (Join-Path $Root ".venv\Scripts\Activate.ps1")) {
    & (Join-Path $Root ".venv\Scripts\Activate.ps1")
}

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  PROP PIPELINE  -- $Date -- $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

# -- Helper: auto-wipe ESPN cache if stale ------------------------------------
function Check-AutoRefreshCache {
    $cacheFile = Join-Path $NBADir "nba_espn_boxscore_cache.csv"
    if (Test-Path $cacheFile) {
        $age = (Get-Date) - (Get-Item $cacheFile).LastWriteTime
        if ($age.TotalDays -gt $CacheAgeDays) {
            Write-Host "  [Cache] ESPN cache is $([math]::Round($age.TotalDays,1)) days old (threshold: $CacheAgeDays). Auto-wiping..." -ForegroundColor Yellow
            Remove-Item (Join-Path $NBADir "nba_espn_boxscore_cache.csv") -Force -ErrorAction SilentlyContinue
            Remove-Item (Join-Path $NBADir "nba_to_espn_id_map.csv")      -Force -ErrorAction SilentlyContinue
            Write-Host "  [Cache] Wiped. Will rebuild fresh." -ForegroundColor Green
        } else {
            Write-Host "  [Cache] ESPN cache is $([math]::Round($age.TotalDays,1)) days old -- keeping." -ForegroundColor DarkGray
        }
    }
}

# -- Helper: run one step -----------------------------------------------------
function Run-Step {
    param(
        [string]$Label,
        [string]$Dir,
        [string]$Script,
        [string]$Arguments = ""
    )
    Write-Host "  --> $Label" -ForegroundColor Yellow
    Push-Location $Dir
    try {
        $cmd = if ($Arguments) { "py -3.14 `"$Script`" $Arguments" } else { "py -3.14 `"$Script`"" }
        Write-Host "        CMD: $cmd" -ForegroundColor DarkGray
        $output = Invoke-Expression $cmd 2>&1
        $exit   = $LASTEXITCODE
        foreach ($line in $output) { Write-Host "        $line" -ForegroundColor DarkGray }
        if ($exit -ne 0) { Write-Host "      FAILED (exit $exit)" -ForegroundColor Red; return $false }
        Write-Host "      OK" -ForegroundColor Green; return $true
    } catch {
        Write-Host "      EXCEPTION: $_" -ForegroundColor Red; return $false
    } finally {
        Pop-Location
    }
}

# -- Helper: build a background job script block for a full sport pipeline ----
function New-PipelineJob {
    param([string]$Tag, [string]$Dir, [scriptblock]$Steps)
    return Start-Job -ScriptBlock {
        param($Tag, $Dir, $Steps, $Date)
        $env:PYTHONUTF8       = "1"
        $env:PYTHONIOENCODING = "utf-8"

        function Run-Step-Job {
            param([string]$Label, [string]$Dir, [string]$Script, [string]$Arguments = "")
            Write-Output "[$Tag] --> $Label"
            Push-Location $Dir
            try {
                $cmd = if ($Arguments) { "py -3.14 `"$Script`" $Arguments" } else { "py -3.14 `"$Script`"" }
                Write-Output "        CMD: $cmd"
                $output = Invoke-Expression $cmd 2>&1
                $exit   = $LASTEXITCODE
                foreach ($line in $output) { Write-Output "        $line" }
                if ($exit -ne 0) { Write-Output "[$Tag] FAILED: $Label (exit $exit)"; return $false }
                Write-Output "[$Tag] OK: $Label"; return $true
            } catch {
                Write-Output "[$Tag] EXCEPTION in $Label`: $_"; return $false
            } finally { Pop-Location }
        }

        & $Steps
    } -ArgumentList $Tag, $Dir, $Steps, $Date
}

# =============================================================================
#  SINGLE-SPORT ONLY MODES
# =============================================================================

# ---- NHL ONLY ---------------------------------------------------------------
if ($NHLOnly) {
    Write-Host "[ NHL PIPELINE ]" -ForegroundColor Magenta
    Write-Host ""
    $ok = $true
    if (-not $SkipFetch) { if ($ok) { $ok = Run-Step "NHL Step 1 - Fetch PrizePicks" $NHLDir ".\\step1_fetch_prizepicks_nhl.py" "--output step1_nhl_props.csv" } } else { Write-Host "  [NHL] Skipping step1 fetch -- using existing step1_nhl_props.csv" -ForegroundColor DarkGray }
    if ($ok) { $ok = Run-Step "NHL Step 2 - Attach Pick Types"  $NHLDir ".\\step2_attach_picktypes_nhl.py"       "--input step1_nhl_props.csv --output step2_nhl_picktypes.csv" }
    if ($ok) { $ok = Run-Step "NHL Step 3 - Attach Defense"     $NHLDir ".\\step3_attach_defense_nhl.py"         "--input step2_nhl_picktypes.csv --output step3_nhl_with_defense.csv" }
    if ($ok) { $ok = Run-Step "NHL Step 4 - Player Stats"       $NHLDir ".\\step4_attach_player_stats_nhl.py"    "--input step3_nhl_with_defense.csv --cache nhl_stats_cache.csv --output step4_nhl_with_stats.csv --max-games 30" }
    if ($ok) { $ok = Run-Step "NHL Step 5 - Line Hit Rates"     $NHLDir ".\\step5_add_line_hit_rates_nhl.py"     "--input step4_nhl_with_stats.csv --output step5_nhl_hit_rates.csv" }
    if ($ok) { $ok = Run-Step "NHL Step 6 - Team Role Context"  $NHLDir ".\\step6_team_role_context_nhl.py"      "--input step5_nhl_hit_rates.csv --output step6_nhl_role_context.csv" }
    if ($ok) { $ok = Run-Step "NHL Step 7 - Rank Props"         $NHLDir ".\\step7_rank_props_nhl.py"             "--input step6_nhl_role_context.csv --output step7_nhl_ranked.xlsx" }
    if ($ok) { $ok = Run-Step "NHL Step 8 - Direction Context"  $NHLDir ".\\step8_add_direction_context_nhl.py"  "--input step7_nhl_ranked.xlsx --output step8_nhl_direction_clean.xlsx" }
    if ($ok) { $ok = Run-Step "NHL Step 9 - Build Tickets"         $NHLDir ".\\step9_build_tickets_nhl.py"            "--input step8_nhl_direction_clean.xlsx --output step9_nhl_tickets.xlsx" }
    Write-Host ""
    if ($ok) { Write-Host "  NHL complete." -ForegroundColor Green } else { Write-Host "  NHL FAILED." -ForegroundColor Red }
    Write-Host ""
    $Elapsed = (Get-Date) - $StartTime
    Write-Host "======================================================" -ForegroundColor Cyan
    Write-Host ("  DONE  -- Elapsed: {0}" -f $Elapsed.ToString("mm\:ss")) -ForegroundColor Cyan
    Write-Host "======================================================" -ForegroundColor Cyan
    exit
}

# ---- MLB ONLY ---------------------------------------------------------------
if ($MLBOnly) {
    Write-Host "[ MLB PIPELINE ]" -ForegroundColor Magenta
    Write-Host ""
    $ok = $true
    if (-not $SkipFetch) { if ($ok) { $ok = Run-Step "MLB Step 1 - Fetch PrizePicks" $MLBDir ".\\step1_fetch_prizepicks_mlb.py" "--output step1_mlb_props.csv" } } else { Write-Host "  [MLB] Skipping step1 fetch -- using existing step1_mlb_props.csv" -ForegroundColor DarkGray }
    if ($ok) { $ok = Run-Step "MLB Step 2 - Attach Pick Types"  $MLBDir ".\\step2_attach_picktypes_mlb.py"       "--input step1_mlb_props.csv --output step2_mlb_picktypes.csv" }
    if ($ok) { $ok = Run-Step "MLB Step 3 - Attach Defense"     $MLBDir ".\\step3_attach_defense_mlb.py"         "--input step2_mlb_picktypes.csv --defense mlb_defense_summary.csv --output step3_mlb_with_defense.csv" }
    if ($ok) { $ok = Run-Step "MLB Step 4 - Player Stats"       $MLBDir ".\\step4_attach_player_stats_mlb.py"    "--input step3_mlb_with_defense.csv --cache mlb_stats_cache.csv --output step4_mlb_with_stats.csv --season 2025" }
    if ($ok) { $ok = Run-Step "MLB Step 5 - Line Hit Rates"     $MLBDir ".\\step5_add_line_hit_rates_mlb.py"     "--input step4_mlb_with_stats.csv --output step5_mlb_hit_rates.csv" }
    if ($ok) { $ok = Run-Step "MLB Step 6 - Team Role Context"  $MLBDir ".\\step6_team_role_context_mlb.py"      "--input step5_mlb_hit_rates.csv --output step6_mlb_role_context.csv" }
    if ($ok) { $ok = Run-Step "MLB Step 7 - Rank Props"         $MLBDir ".\\step7_rank_props_mlb.py"             "--input step6_mlb_role_context.csv --output step7_mlb_ranked.xlsx" }
    if ($ok) { $ok = Run-Step "MLB Step 8 - Direction Context"  $MLBDir ".\\step8_add_direction_context_mlb.py"  "--input step7_mlb_ranked.xlsx --output step8_mlb_direction.csv" }
    Write-Host ""
    if ($ok) { Write-Host "  MLB complete." -ForegroundColor Green } else { Write-Host "  MLB FAILED." -ForegroundColor Red }
    Write-Host ""
    $Elapsed = (Get-Date) - $StartTime
    Write-Host "======================================================" -ForegroundColor Cyan
    Write-Host ("  DONE  -- Elapsed: {0}" -f $Elapsed.ToString("mm\:ss")) -ForegroundColor Cyan
    Write-Host "======================================================" -ForegroundColor Cyan
    exit
}

# ---- SOCCER ONLY ------------------------------------------------------------
if ($SoccerOnly) {
    Write-Host "[ SOCCER PIPELINE ]" -ForegroundColor Magenta
    Write-Host ""
    $ok = $true
    if (-not $SkipFetch) { if ($ok) { $ok = Run-Step "Soccer Step 1 - Fetch PrizePicks" $SoccerDir ".\\step1_fetch_prizepicks_soccer.py" "--output step1_soccer_props.csv" } } else { Write-Host "  [Soccer] Skipping step1 fetch -- using existing step1_soccer_props.csv" -ForegroundColor DarkGray }
    if ($ok) { $ok = Run-Step "Soccer Step 2 - Attach Pick Types"  $SoccerDir ".\\step2_attach_picktypes_soccer.py"       "--input step1_soccer_props.csv --output step2_soccer_picktypes.csv" }
    if ($ok) { $ok = Run-Step "Soccer Step 3 - Attach Defense"     $SoccerDir ".\\step3_attach_defense_soccer.py"         "--input step2_soccer_picktypes.csv --defense soccer_defense_summary.csv --output step3_soccer_with_defense.csv" }
    if ($ok) { $ok = Run-Step "Soccer Step 4 - Player Stats"       $SoccerDir ".\\step4_attach_player_stats_soccer.py"    "--input step3_soccer_with_defense.csv --cache soccer_stats_cache.csv --output step4_soccer_with_stats.csv" }
    if ($ok) { $ok = Run-Step "Soccer Step 5 - Line Hit Rates"     $SoccerDir ".\\step5_add_line_hit_rates_soccer.py"     "--input step4_soccer_with_stats.csv --output step5_soccer_hit_rates.csv" }
    if ($ok) { $ok = Run-Step "Soccer Step 6 - Team Role Context"  $SoccerDir ".\\step6_team_role_context_soccer.py"      "--input step5_soccer_hit_rates.csv --output step6_soccer_role_context.csv" }
    if ($ok) { $ok = Run-Step "Soccer Step 7 - Rank Props"         $SoccerDir ".\\step7_rank_props_soccer.py"             "--input step6_soccer_role_context.csv --output step7_soccer_ranked.xlsx" }
    if ($ok) { $ok = Run-Step "Soccer Step 8 - Direction Context"  $SoccerDir ".\\step8_add_direction_context_soccer.py"  "--input step7_soccer_ranked.xlsx --output step8_soccer_direction_clean.xlsx" }
    # Soccer S9 disabled -- tickets generated in combined_slate_tickets.py
    # if ($ok) { $ok = Run-Step "Soccer Step 9 - Build Tickets" $SoccerDir ".\\step9_build_tickets_soccer.py" "--input step8_soccer_direction_clean.xlsx --output step9_soccer_tickets.xlsx" }
    Write-Host ""
    if ($ok) { Write-Host "  Soccer complete." -ForegroundColor Green } else { Write-Host "  Soccer FAILED." -ForegroundColor Red }
    Write-Host ""
    $Elapsed = (Get-Date) - $StartTime
    Write-Host "======================================================" -ForegroundColor Cyan
    Write-Host ("  DONE  -- Elapsed: {0}" -f $Elapsed.ToString("mm\:ss")) -ForegroundColor Cyan
    Write-Host "======================================================" -ForegroundColor Cyan
    exit
}

# ---- WNBA ONLY (delegates to dedicated script) ------------------------------
if ($WNBAOnly) {
    Write-Host "[ WNBA PIPELINE ]" -ForegroundColor Magenta
    Write-Host "  Delegating to run_wnba_pipeline.ps1 ..." -ForegroundColor DarkGray
    Write-Host ""
    & (Join-Path $Root "run_wnba_pipeline.ps1") -Date $Date
    exit
}

# =============================================================================
#  NBA-ONLY MODE
# =============================================================================
if ($NBAOnly) {
    Write-Host "[ NBA PIPELINE ]" -ForegroundColor Magenta
    Write-Host ""

    if (Test-Path (Join-Path $NBADir "RUN_COMPLETE.flag")) { Remove-Item (Join-Path $NBADir "RUN_COMPLETE.flag") -Force }

    if ($RefreshCache) {
        Write-Host "  [Cache] Wiping ESPN cache files..." -ForegroundColor Yellow
        Remove-Item (Join-Path $NBADir "nba_espn_boxscore_cache.csv") -Force -ErrorAction SilentlyContinue
        Remove-Item (Join-Path $NBADir "nba_to_espn_id_map.csv")      -Force -ErrorAction SilentlyContinue
        Write-Host "  [Cache] Done." -ForegroundColor Green
        Write-Host ""
    } else {
        Check-AutoRefreshCache
    }

    $ok = $true
    if (-not $SkipFetch) { if ($ok) { $ok = Run-Step "NBA Step 1 - Fetch PrizePicks" $NBADir ".\\step1_fetch_prizepicks_api.py" "--league_id 7 --game_mode pickem --per_page 250 --max_pages 5 --sleep 2.0 --cooldown_seconds 90 --max_cooldowns 3 --jitter_seconds 10.0 --output step1_pp_props_today.csv" } } else { Write-Host "  [NBA] Skipping step1 fetch -- using existing step1_pp_props_today.csv" -ForegroundColor DarkGray }
    if ($ok) { $ok = Run-Step "NBA Step 2 - Attach Pick Types"       $NBADir ".\\step2_attach_picktypes.py"                "--input step1_pp_props_today.csv --output step2_with_picktypes.csv" }
    if ($ok) { $ok = Run-Step "NBA Step 3 - Attach Defense"          $NBADir ".\\step3_attach_defense.py"                  "--input step2_with_picktypes.csv --defense .\\defense_team_summary.csv --output step3_with_defense.csv" }
    if ($ok) { $ok = Run-Step "NBA Step 4 - Player Stats (ESPN)"     $NBADir ".\\step4_attach_player_stats_espn_cache.py"  "--slate step3_with_defense.csv --out step4_with_stats.csv --season 2025-26 --date $Date --days 35 --cache nba_espn_boxscore_cache.csv --idmap nba_to_espn_id_map.csv --n 10 --sleep 0.8 --retries 4 --connect-timeout 8 --timeout 30 --debug-misses no_espn_player_debug.csv" }
    if ($ok) { $ok = Run-Step "NBA Step 5 - Line Hit Rates"          $NBADir ".\\step5_add_line_hit_rates.py"              "--input step4_with_stats.csv --output step5_with_hit_rates.csv" }
    if ($ok) { $ok = Run-Step "NBA Step 6 - Team Role Context"       $NBADir ".\\step6_team_role_context.py"               "--input step5_with_hit_rates.csv --output step6_with_team_role_context.csv" }
    if ($ok) { $ok = Run-Step "NBA Step 6a - Opponent H2H Stats"     $NBADir ".\\step6a_attach_opponent_stats_NBA.py"       "--input step6_with_team_role_context.csv --output step6a_with_opp_stats.csv --cache nba_espn_boxscore_cache.csv --opp-cache s6a_nba_opp_stats_cache.csv" }
    if ($ok) { $ok = Run-Step "NBA Step 6b - Game Context (Vegas)"   $NBADir ".\\step6b_attach_game_context.py"            "--input step6a_with_opp_stats.csv --output step6b_with_game_context.csv --api_key `"$OddsApiKey`" --date $Date --cache `"game_context_cache_$Date.csv`"" }
    if ($ok) { $ok = Run-Step "NBA Step 6c - Schedule Flags (B2B)"   $NBADir ".\\step6c_schedule_flags.py"                 "--input step6b_with_game_context.csv --output step6c_with_schedule_flags.csv --date $Date --cache `"schedule_cache_$Date.csv`"" }
    if ($ok) { $ok = Run-Step "NBA Step 7 - Rank Props"              $NBADir ".\\step7_rank_props.py"                      "--input step6c_with_schedule_flags.csv --output step7_ranked_props.xlsx" }
    if ($ok) { $ok = Run-Step "NBA Step 8 - Direction Context"       $NBADir ".\\step8_add_direction_context.py"           "--input step7_ranked_props.xlsx --sheet ALL --output step8_all_direction.csv" }
    # NBA S9 disabled -- tickets generated in combined_slate_tickets.py
    # if ($ok) { $ok = Run-Step "NBA Step 9 - Build Tickets" $NBADir ".\\step9_build_tickets.py" "--input step8_all_direction_clean.xlsx --output best_tickets.xlsx --min_hit_rate 0.6 --legs 2,3,4" }

    if ($ok) { New-Item -ItemType File -Force -Path (Join-Path $NBADir "RUN_COMPLETE.flag") | Out-Null }
    Write-Host ""
    if ($ok) { Write-Host "  NBA complete." -ForegroundColor Green } else { Write-Host "  NBA FAILED." -ForegroundColor Red }
    Write-Host ""
    $Elapsed = (Get-Date) - $StartTime
    Write-Host "======================================================" -ForegroundColor Cyan
    Write-Host ("  DONE  -- Elapsed: {0}" -f $Elapsed.ToString("mm\:ss")) -ForegroundColor Cyan
    Write-Host "======================================================" -ForegroundColor Cyan
    exit
}

# =============================================================================
#  CBB-ONLY MODE
# =============================================================================
if ($CBBOnly) {
    Write-Host "[ CBB PIPELINE ]" -ForegroundColor Magenta
    Write-Host ""
    $ok = $true
    if (-not $SkipFetch) { if ($ok) { $ok = Run-Step "CBB Step 1 - Fetch PrizePicks" $CBBDir ".\\pp_cbb_scraper.py" "--out step1_cbb.csv" } } else { Write-Host "  [CBB] Skipping step1 fetch -- using existing step1_cbb.csv" -ForegroundColor DarkGray }
    if ($ok) { $ok = Run-Step "CBB Step 2 - Normalize"               $CBBDir ".\\cbb_step2_normalize.py"                   "--input step1_cbb.csv --output step2_cbb.csv" }
    if ($ok) { $ok = Run-Step "CBB Step 3 - Attach Defense Rankings" $CBBDir ".\\cbb_step3b_attach_def_rankings.py"        "--input step2_cbb.csv --defense cbb_def_rankings.csv --output step3b_with_def_rankings_cbb.csv" }
    if ($ok) { $ok = Run-Step "CBB Step 4 - Attach ESPN IDs"         $CBBDir ".\\step5_attach_espn_ids.py"                 "--input step3b_with_def_rankings_cbb.csv --output step3_cbb.csv --master data/ncaa_mbb_athletes_master.csv" }
    if ($ok) { $ok = Run-Step "CBB Step 5 - Boxscore Stats"          $CBBDir ".\\cbb_step5b_attach_boxscore_stats.py"      "--input step3_cbb.csv --output step5b_cbb.csv" }
    if ($ok) { $ok = Run-Step "CBB Step 6 - Rank Props"              $CBBDir ".\\cbb_step6_rank_props.py"                  "--input step5b_cbb.csv --output step6_ranked_cbb.xlsx" }
    Write-Host ""
    if ($ok) { Write-Host "  CBB complete." -ForegroundColor Green } else { Write-Host "  CBB FAILED." -ForegroundColor Red }
    Write-Host ""
    $Elapsed = (Get-Date) - $StartTime
    Write-Host "======================================================" -ForegroundColor Cyan
    Write-Host ("  DONE  -- Elapsed: {0}" -f $Elapsed.ToString("mm\:ss")) -ForegroundColor Cyan
    Write-Host "======================================================" -ForegroundColor Cyan
    exit
}

# =============================================================================
#  FULL PARALLEL RUN (NBA + CBB always; optional NHL / MLB / Soccer)
# =============================================================================
$NBASuccess    = $false
$CBBSuccess    = $false
$NHLSuccess    = $false
$MLBSuccess    = $false
$SoccerSuccess = $false

if ($CombinedOnly) {
    $NBASuccess = Test-Path (Join-Path $NBADir "step8_all_direction_clean.xlsx")
    $CBBSuccess = Test-Path (Join-Path $CBBDir "step6_ranked_cbb.xlsx")
    if (-not $NBASuccess) { Write-Host "  WARNING: NBA slate not found" -ForegroundColor Yellow }
    if (-not $CBBSuccess) { Write-Host "  WARNING: CBB slate not found" -ForegroundColor Yellow }
} else {
    # Cache handling for NBA
    if ($RefreshCache) {
        Write-Host "  [Cache] Wiping ESPN cache files..." -ForegroundColor Yellow
        Remove-Item (Join-Path $NBADir "nba_espn_boxscore_cache.csv") -Force -ErrorAction SilentlyContinue
        Remove-Item (Join-Path $NBADir "nba_to_espn_id_map.csv")      -Force -ErrorAction SilentlyContinue
        Write-Host "  [Cache] Done." -ForegroundColor Green
        Write-Host ""
    } else {
        Check-AutoRefreshCache
    }

    if (Test-Path (Join-Path $NBADir "RUN_COMPLETE.flag")) { Remove-Item (Join-Path $NBADir "RUN_COMPLETE.flag") -Force }

    # Announce what we're running
    $runList = @("NBA","CBB")
    if ($IncludeNHL)    { $runList += "NHL" }
    if ($IncludeMLB)    { $runList += "MLB" }
    if ($IncludeSoccer) { $runList += "Soccer" }
    Write-Host "[ PARALLEL PIPELINE: $($runList -join ' + ') ]" -ForegroundColor Magenta
    Write-Host ""
    Write-Host "  Starting pipelines simultaneously..." -ForegroundColor Cyan
    Write-Host ""

    # -"--"- NBA Job -"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"-
    $NBAJob = Start-Job -ScriptBlock {
        param($NBADir, $Date, $SkipFetch)
        $env:PYTHONUTF8 = "1"; $env:PYTHONIOENCODING = "utf-8"
        function Run-Step-Job {
            param([string]$Label,[string]$Dir,[string]$Script,[string]$Arguments="")
            Write-Output "[NBA] --> $Label"
            Push-Location $Dir
            try {
                $cmd = if ($Arguments) { "py -3.14 `"$Script`" $Arguments" } else { "py -3.14 `"$Script`"" }
                Write-Output "        CMD: $cmd"
                $output = Invoke-Expression $cmd 2>&1; $exit = $LASTEXITCODE
                foreach ($line in $output) { Write-Output "        $line" }
                if ($exit -ne 0) { Write-Output "[NBA] FAILED: $Label (exit $exit)"; return $false }
                Write-Output "[NBA] OK: $Label"; return $true
            } catch { Write-Output "[NBA] EXCEPTION in $Label`: $_"; return $false
            } finally { Pop-Location }
        }
        $ok = $true
        if (-not $using:SkipFetch) { if ($ok) { $ok = Run-Step-Job "NBA Step 1 - Fetch PrizePicks" $NBADir ".\\step1_fetch_prizepicks_api.py" "--league_id 7 --game_mode pickem --per_page 250 --max_pages 5 --sleep 2.0 --cooldown_seconds 90 --max_cooldowns 3 --jitter_seconds 10.0 --output step1_pp_props_today.csv" } } else { Write-Output "[NBA] Skipping step1 fetch -- using existing step1_pp_props_today.csv" }
        if ($ok) { $ok = Run-Step-Job "NBA Step 2 - Attach Pick Types"       $NBADir ".\\step2_attach_picktypes.py"                "--input step1_pp_props_today.csv --output step2_with_picktypes.csv" }
        if ($ok) { $ok = Run-Step-Job "NBA Step 3 - Attach Defense"          $NBADir ".\\step3_attach_defense.py"                  "--input step2_with_picktypes.csv --defense .\\defense_team_summary.csv --output step3_with_defense.csv" }
        if ($ok) { $ok = Run-Step-Job "NBA Step 4 - Player Stats (ESPN)"     $NBADir ".\\step4_attach_player_stats_espn_cache.py"  "--slate step3_with_defense.csv --out step4_with_stats.csv --season 2025-26 --date $Date --days 35 --cache nba_espn_boxscore_cache.csv --idmap nba_to_espn_id_map.csv --n 10 --sleep 0.8 --retries 4 --connect-timeout 8 --timeout 30 --debug-misses no_espn_player_debug.csv" }
        if ($ok) { $ok = Run-Step-Job "NBA Step 5 - Line Hit Rates"          $NBADir ".\\step5_add_line_hit_rates.py"              "--input step4_with_stats.csv --output step5_with_hit_rates.csv" }
        if ($ok) { $ok = Run-Step-Job "NBA Step 6 - Team Role Context"       $NBADir ".\\step6_team_role_context.py"               "--input step5_with_hit_rates.csv --output step6_with_team_role_context.csv" }
        if ($ok) { $ok = Run-Step-Job "NBA Step 6a - Opponent H2H Stats"     $NBADir ".\\step6a_attach_opponent_stats_NBA.py"       "--input step6_with_team_role_context.csv --output step6a_with_opp_stats.csv --cache nba_espn_boxscore_cache.csv --opp-cache s6a_nba_opp_stats_cache.csv" }
        if ($ok) { $ok = Run-Step-Job "NBA Step 6b - Game Context (Vegas)"   $NBADir ".\\step6b_attach_game_context.py"            "--input step6a_with_opp_stats.csv --output step6b_with_game_context.csv --api_key `"$using:OddsApiKey`" --date $Date --cache `"game_context_cache_$Date.csv`"" }
        if ($ok) { $ok = Run-Step-Job "NBA Step 6c - Schedule Flags (B2B)"   $NBADir ".\\step6c_schedule_flags.py"                 "--input step6b_with_game_context.csv --output step6c_with_schedule_flags.csv --date $Date --cache `"schedule_cache_$Date.csv`"" }
        if ($ok) { $ok = Run-Step-Job "NBA Step 7 - Rank Props"              $NBADir ".\\step7_rank_props.py"                      "--input step6c_with_schedule_flags.csv --output step7_ranked_props.xlsx" }
        if ($ok) { $ok = Run-Step-Job "NBA Step 8 - Direction Context"       $NBADir ".\\step8_add_direction_context.py"           "--input step7_ranked_props.xlsx --sheet ALL --output step8_all_direction.csv" }
        # NBA S9 disabled -- tickets generated in combined_slate_tickets.py
        # if ($ok) { $ok = Run-Step-Job "NBA Step 9 - Build Tickets" $NBADir ".\\step9_build_tickets.py" "--input step8_all_direction_clean.xlsx --output best_tickets.xlsx --min_hit_rate 0.6 --legs 2,3,4" }
        return $ok
    } -ArgumentList $NBADir, $Date, $SkipFetch

    # -"--"- CBB Job -"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"-
    $CBBJob = Start-Job -ScriptBlock {
        param($CBBDir, $SkipFetch)
        $env:PYTHONUTF8 = "1"; $env:PYTHONIOENCODING = "utf-8"
        function Run-Step-Job {
            param([string]$Label,[string]$Dir,[string]$Script,[string]$Arguments="")
            Write-Output "[CBB] --> $Label"
            Push-Location $Dir
            try {
                $cmd = if ($Arguments) { "py -3.14 `"$Script`" $Arguments" } else { "py -3.14 `"$Script`"" }
                Write-Output "        CMD: $cmd"
                $output = Invoke-Expression $cmd 2>&1; $exit = $LASTEXITCODE
                foreach ($line in $output) { Write-Output "        $line" }
                if ($exit -ne 0) { Write-Output "[CBB] FAILED: $Label (exit $exit)"; return $false }
                Write-Output "[CBB] OK: $Label"; return $true
            } catch { Write-Output "[CBB] EXCEPTION in $Label`: $_"; return $false
            } finally { Pop-Location }
        }
        $ok = $true
        if (-not $using:SkipFetch) { if ($ok) { $ok = Run-Step-Job "CBB Step 1 - Fetch PrizePicks" $CBBDir ".\\pp_cbb_scraper.py" "--out step1_cbb.csv" } } else { Write-Output "[CBB] Skipping step1 fetch -- using existing step1_cbb.csv" }
        if ($ok) { $ok = Run-Step-Job "CBB Step 2 - Normalize"               $CBBDir ".\\cbb_step2_normalize.py"                   "--input step1_cbb.csv --output step2_cbb.csv" }
        if ($ok) { $ok = Run-Step-Job "CBB Step 3 - Attach Defense Rankings" $CBBDir ".\\cbb_step3b_attach_def_rankings.py"        "--input step2_cbb.csv --defense cbb_def_rankings.csv --output step3b_with_def_rankings_cbb.csv" }
        if ($ok) { $ok = Run-Step-Job "CBB Step 4 - Attach ESPN IDs"         $CBBDir ".\\step5_attach_espn_ids.py"                 "--input step3b_with_def_rankings_cbb.csv --output step3_cbb.csv --master data/ncaa_mbb_athletes_master.csv" }
        if ($ok) { $ok = Run-Step-Job "CBB Step 5 - Boxscore Stats"          $CBBDir ".\\cbb_step5b_attach_boxscore_stats.py"      "--input step3_cbb.csv --output step5b_cbb.csv" }
        if ($ok) { $ok = Run-Step-Job "CBB Step 6 - Rank Props"              $CBBDir ".\\cbb_step6_rank_props.py"                  "--input step5b_cbb.csv --output step6_ranked_cbb.xlsx" }
        return $ok
    } -ArgumentList $CBBDir, $SkipFetch

    # -"--"- Optional: NHL Job -"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"-
    $NHLJob = $null
    if ($IncludeNHL) {
        $NHLJob = Start-Job -ScriptBlock {
            param($NHLDir, $SkipFetch)
            $env:PYTHONUTF8 = "1"; $env:PYTHONIOENCODING = "utf-8"
            function Run-Step-Job {
                param([string]$Label,[string]$Dir,[string]$Script,[string]$Arguments="")
                Write-Output "[NHL] --> $Label"
                Push-Location $Dir
                try {
                    $cmd = if ($Arguments) { "py -3.14 `"$Script`" $Arguments" } else { "py -3.14 `"$Script`"" }
                    $output = Invoke-Expression $cmd 2>&1; $exit = $LASTEXITCODE
                    foreach ($line in $output) { Write-Output "        $line" }
                    if ($exit -ne 0) { Write-Output "[NHL] FAILED: $Label (exit $exit)"; return $false }
                    Write-Output "[NHL] OK: $Label"; return $true
                } catch { Write-Output "[NHL] EXCEPTION: $_"; return $false
                } finally { Pop-Location }
            }
            $ok = $true
            if (-not $using:SkipFetch) { if ($ok) { $ok = Run-Step-Job "NHL Step 1 - Fetch PrizePicks" $NHLDir ".\\step1_fetch_prizepicks_nhl.py" "--output step1_nhl_props.csv" } } else { Write-Output "[NHL] Skipping step1 fetch -- using existing step1_nhl_props.csv" }
            if ($ok) { $ok = Run-Step-Job "NHL Step 2 - Attach Pick Types"  $NHLDir ".\\step2_attach_picktypes_nhl.py"       "--input step1_nhl_props.csv --output step2_nhl_picktypes.csv" }
            if ($ok) { $ok = Run-Step-Job "NHL Step 3 - Attach Defense"     $NHLDir ".\\step3_attach_defense_nhl.py"         "--input step2_nhl_picktypes.csv --output step3_nhl_with_defense.csv" }
            if ($ok) { $ok = Run-Step-Job "NHL Step 4 - Player Stats"       $NHLDir ".\\step4_attach_player_stats_nhl.py"    "--input step3_nhl_with_defense.csv --cache nhl_stats_cache.csv --output step4_nhl_with_stats.csv --max-games 30" }
            if ($ok) { $ok = Run-Step-Job "NHL Step 5 - Line Hit Rates"     $NHLDir ".\\step5_add_line_hit_rates_nhl.py"     "--input step4_nhl_with_stats.csv --output step5_nhl_hit_rates.csv" }
            if ($ok) { $ok = Run-Step-Job "NHL Step 6 - Team Role Context"  $NHLDir ".\\step6_team_role_context_nhl.py"      "--input step5_nhl_hit_rates.csv --output step6_nhl_role_context.csv" }
            if ($ok) { $ok = Run-Step-Job "NHL Step 7 - Rank Props"         $NHLDir ".\\step7_rank_props_nhl.py"             "--input step6_nhl_role_context.csv --output step7_nhl_ranked.xlsx" }
            if ($ok) { $ok = Run-Step-Job "NHL Step 8 - Direction Context"  $NHLDir ".\\step8_add_direction_context_nhl.py"  "--input step7_nhl_ranked.xlsx --output step8_nhl_direction_clean.xlsx" }
            if ($ok) { $ok = Run-Step-Job "NHL Step 9 - Build Tickets"         $NHLDir ".\\step9_build_tickets_nhl.py"            "--input step8_nhl_direction_clean.xlsx --output step9_nhl_tickets.xlsx" }
            return $ok
        } -ArgumentList $NHLDir, $SkipFetch
    }

    # -"--"- Optional: MLB Job -"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"-
    $MLBJob = $null
    if ($IncludeMLB) {
        $MLBJob = Start-Job -ScriptBlock {
            param($MLBDir, $SkipFetch)
            $env:PYTHONUTF8 = "1"; $env:PYTHONIOENCODING = "utf-8"
            function Run-Step-Job {
                param([string]$Label,[string]$Dir,[string]$Script,[string]$Arguments="")
                Write-Output "[MLB] --> $Label"
                Push-Location $Dir
                try {
                    $cmd = if ($Arguments) { "py -3.14 `"$Script`" $Arguments" } else { "py -3.14 `"$Script`"" }
                    $output = Invoke-Expression $cmd 2>&1; $exit = $LASTEXITCODE
                    foreach ($line in $output) { Write-Output "        $line" }
                    if ($exit -ne 0) { Write-Output "[MLB] FAILED: $Label (exit $exit)"; return $false }
                    Write-Output "[MLB] OK: $Label"; return $true
                } catch { Write-Output "[MLB] EXCEPTION: $_"; return $false
                } finally { Pop-Location }
            }
            $ok = $true
            if (-not $using:SkipFetch) { if ($ok) { $ok = Run-Step-Job "MLB Step 1 - Fetch PrizePicks" $MLBDir ".\\step1_fetch_prizepicks_mlb.py" "--output step1_mlb_props.csv" } } else { Write-Output "[MLB] Skipping step1 fetch -- using existing step1_mlb_props.csv" }
            if ($ok) { $ok = Run-Step-Job "MLB Step 2 - Attach Pick Types"  $MLBDir ".\\step2_attach_picktypes_mlb.py"       "--input step1_mlb_props.csv --output step2_mlb_picktypes.csv" }
            if ($ok) { $ok = Run-Step-Job "MLB Step 3 - Attach Defense"     $MLBDir ".\\step3_attach_defense_mlb.py"         "--input step2_mlb_picktypes.csv --defense mlb_defense_summary.csv --output step3_mlb_with_defense.csv" }
            if ($ok) { $ok = Run-Step-Job "MLB Step 4 - Player Stats"       $MLBDir ".\\step4_attach_player_stats_mlb.py"    "--input step3_mlb_with_defense.csv --cache mlb_stats_cache.csv --output step4_mlb_with_stats.csv --season 2025" }
            if ($ok) { $ok = Run-Step-Job "MLB Step 5 - Line Hit Rates"     $MLBDir ".\\step5_add_line_hit_rates_mlb.py"     "--input step4_mlb_with_stats.csv --output step5_mlb_hit_rates.csv" }
            if ($ok) { $ok = Run-Step-Job "MLB Step 6 - Team Role Context"  $MLBDir ".\\step6_team_role_context_mlb.py"      "--input step5_mlb_hit_rates.csv --output step6_mlb_role_context.csv" }
            if ($ok) { $ok = Run-Step-Job "MLB Step 7 - Rank Props"         $MLBDir ".\\step7_rank_props_mlb.py"             "--input step6_mlb_role_context.csv --output step7_mlb_ranked.xlsx" }
            if ($ok) { $ok = Run-Step-Job "MLB Step 8 - Direction Context"  $MLBDir ".\\step8_add_direction_context_mlb.py"  "--input step7_mlb_ranked.xlsx --output step8_mlb_direction.csv" }
            return $ok
        } -ArgumentList $MLBDir, $SkipFetch
    }

    # -"--"- Optional: Soccer Job -"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"-
    $SoccerJob = $null
    if ($IncludeSoccer) {
        $SoccerJob = Start-Job -ScriptBlock {
            param($SoccerDir, $SkipFetch)
            $env:PYTHONUTF8 = "1"; $env:PYTHONIOENCODING = "utf-8"
            function Run-Step-Job {
                param([string]$Label,[string]$Dir,[string]$Script,[string]$Arguments="")
                Write-Output "[SOCCER] --> $Label"
                Push-Location $Dir
                try {
                    $cmd = if ($Arguments) { "py -3.14 `"$Script`" $Arguments" } else { "py -3.14 `"$Script`"" }
                    $output = Invoke-Expression $cmd 2>&1; $exit = $LASTEXITCODE
                    foreach ($line in $output) { Write-Output "        $line" }
                    if ($exit -ne 0) { Write-Output "[SOCCER] FAILED: $Label (exit $exit)"; return $false }
                    Write-Output "[SOCCER] OK: $Label"; return $true
                } catch { Write-Output "[SOCCER] EXCEPTION: $_"; return $false
                } finally { Pop-Location }
            }
            $ok = $true
            if (-not $using:SkipFetch) { if ($ok) { $ok = Run-Step-Job "Soccer Step 1 - Fetch PrizePicks" $SoccerDir ".\\step1_fetch_prizepicks_soccer.py" "--output step1_soccer_props.csv" } } else { Write-Output "[Soccer] Skipping step1 fetch -- using existing step1_soccer_props.csv" }
            if ($ok) { $ok = Run-Step-Job "Soccer Step 2 - Attach Pick Types"  $SoccerDir ".\\step2_attach_picktypes_soccer.py"       "--input step1_soccer_props.csv --output step2_soccer_picktypes.csv" }
            if ($ok) { $ok = Run-Step-Job "Soccer Step 3 - Attach Defense"     $SoccerDir ".\\step3_attach_defense_soccer.py"         "--input step2_soccer_picktypes.csv --defense soccer_defense_summary.csv --output step3_soccer_with_defense.csv" }
            if ($ok) { $ok = Run-Step-Job "Soccer Step 4 - Player Stats"       $SoccerDir ".\\step4_attach_player_stats_soccer.py"    "--input step3_soccer_with_defense.csv --cache soccer_stats_cache.csv --output step4_soccer_with_stats.csv" }
            if ($ok) { $ok = Run-Step-Job "Soccer Step 5 - Line Hit Rates"     $SoccerDir ".\\step5_add_line_hit_rates_soccer.py"     "--input step4_soccer_with_stats.csv --output step5_soccer_hit_rates.csv" }
            if ($ok) { $ok = Run-Step-Job "Soccer Step 6 - Team Role Context"  $SoccerDir ".\\step6_team_role_context_soccer.py"      "--input step5_soccer_hit_rates.csv --output step6_soccer_role_context.csv" }
            if ($ok) { $ok = Run-Step-Job "Soccer Step 7 - Rank Props"         $SoccerDir ".\\step7_rank_props_soccer.py"             "--input step6_soccer_role_context.csv --output step7_soccer_ranked.xlsx" }
            if ($ok) { $ok = Run-Step-Job "Soccer Step 8 - Direction Context"  $SoccerDir ".\\step8_add_direction_context_soccer.py"  "--input step7_soccer_ranked.xlsx --output step8_soccer_direction_clean.xlsx" }
            # Soccer S9 disabled -- tickets generated in combined_slate_tickets.py
            # if ($ok) { $ok = Run-Step-Job "Soccer Step 9 - Build Tickets" $SoccerDir ".\\step9_build_tickets_soccer.py" "--input step8_soccer_direction_clean.xlsx --output step9_soccer_tickets.xlsx" }
            return $ok
        } -ArgumentList $SoccerDir, $SkipFetch
    }

    # -"--"- Wait for all jobs, stream output -"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"--"-
    $allJobs = @($NBAJob, $CBBJob) + @($NHLJob, $MLBJob, $SoccerJob | Where-Object { $_ -ne $null })

    Write-Host "  [Waiting for all pipelines to finish...]" -ForegroundColor DarkGray
    Write-Host ""

    while (($allJobs | Where-Object { $_.State -eq 'Running' }).Count -gt 0) {
        foreach ($job in $allJobs) { $out = Receive-Job $job -ErrorAction SilentlyContinue; foreach ($line in $out) { Write-Host "    $line" -ForegroundColor DarkGray } }
        Start-Sleep -Milliseconds 500
    }
    foreach ($job in $allJobs) { $out = Receive-Job $job -ErrorAction SilentlyContinue; foreach ($line in $out) { Write-Host "    $line" -ForegroundColor DarkGray } }

    # -- Determine success from output files ----------------------------------
    $NBASuccess = Test-Path (Join-Path $NBADir "step8_all_direction_clean.xlsx")
    $CBBSuccess = Test-Path (Join-Path $CBBDir "step6_ranked_cbb.xlsx")
    if ($IncludeNHL)    { $NHLSuccess    = Test-Path (Join-Path $NHLDir    "step8_nhl_direction_clean.xlsx") }
    if ($IncludeMLB)    { $MLBSuccess    = Test-Path (Join-Path $MLBDir    "step8_mlb_direction_clean.xlsx") }
    if ($IncludeSoccer) { $SoccerSuccess = Test-Path (Join-Path $SoccerDir "step8_soccer_direction_clean.xlsx") }

    Remove-Job $allJobs -Force -ErrorAction SilentlyContinue

    if ($NBASuccess) { New-Item -ItemType File -Force -Path (Join-Path $NBADir "RUN_COMPLETE.flag") | Out-Null }

    # -- Status report --------------------------------------------------------
    Write-Host ""
    @(
        @{ Name="NBA";    Ok=$NBASuccess;    Always=$true },
        @{ Name="CBB";    Ok=$CBBSuccess;    Always=$true },
        @{ Name="NHL";    Ok=$NHLSuccess;    Always=$IncludeNHL.IsPresent },
        @{ Name="MLB";    Ok=$MLBSuccess;    Always=$IncludeMLB.IsPresent },
        @{ Name="Soccer"; Ok=$SoccerSuccess; Always=$IncludeSoccer.IsPresent }
    ) | Where-Object { $_.Always } | ForEach-Object {
        if ($_.Ok) { Write-Host "  $($_.Name) complete." -ForegroundColor Green }
        else        { Write-Host "  $($_.Name) FAILED."  -ForegroundColor Red   }
    }
    Write-Host ""
}

# =============================================================================
#  COMBINED SLATE + TICKETS  (NBA + CBB required)
# =============================================================================
if (-not $NBAOnly -and -not $CBBOnly -and -not $NHLOnly -and -not $MLBOnly -and -not $SoccerOnly -and -not $WNBAOnly) {
    if ($NBASuccess -and $CBBSuccess) {
        Write-Host "[ COMBINED SLATE + TICKETS ]" -ForegroundColor Magenta
        Write-Host ""

        $CombinedOut = Join-Path $OutDir "combined_slate_tickets_$Date.xlsx"

        $CombinedArgs  = "--nba `"$NBADir\step8_all_direction_clean.xlsx`""
        $CombinedArgs += " --cbb `"$CBBDir\step6_ranked_cbb.xlsx`""
        if ($IncludeNHL -and (Test-Path "$NHLDir\step8_nhl_direction_clean.xlsx")) {
            $CombinedArgs += " --nhl `"$NHLDir\step8_nhl_direction_clean.xlsx`""
        }
        if ($IncludeSoccer -and (Test-Path "$SoccerDir\step8_soccer_direction_clean.xlsx")) {
            $CombinedArgs += " --soccer `"$SoccerDir\step8_soccer_direction_clean.xlsx`""
        }
        $CombinedArgs += " --date $Date --output `"$CombinedOut`" --tiers A,B,C,D --max-tickets 3 --write-web --web-outdir `"$WebOutDir`""

        $ok = Run-Step "Combined Slate + Tickets" $Root `
            ".\scripts\combined_slate_tickets.py" `
            $CombinedArgs

        if ($ok) {
            Copy-Item $CombinedOut (Join-Path $Root "combined_slate_tickets_$Date.xlsx") -Force -ErrorAction SilentlyContinue
            Write-Host "  Saved -> $CombinedOut" -ForegroundColor Green
        }
        Write-Host ""
    } else {
        Write-Host "[ COMBINED ] Skipped -- requires both NBA + CBB to succeed." -ForegroundColor DarkGray
        Write-Host ""
    }
}

# =============================================================================
#  SUMMARY
# =============================================================================
$Elapsed = (Get-Date) - $StartTime
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ("  DONE  -- Elapsed: {0}" -f $Elapsed.ToString("mm\:ss")) -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

# -- Auto-push templates to GitHub -------------------------------------------
if ($NBASuccess -and $CBBSuccess -and -not $NBAOnly -and -not $CBBOnly -and -not $NHLOnly -and -not $MLBOnly -and -not $SoccerOnly -and -not $WNBAOnly) {
    Write-Host "[ GIT ] Pushing updated templates to GitHub..." -ForegroundColor Cyan
    Push-Location $Root
    try {
        git add "ui_runner/templates/tickets_latest.html" "ui_runner/templates/tickets_latest.json" 2>&1 | Out-Null
        $msg = "chore: pipeline update $Date $(Get-Date -Format 'HH:mm')"
        $commitOut = git commit -m $msg 2>&1
        if ($LASTEXITCODE -eq 0) {
            $pushOut = git push origin main 2>&1
            foreach ($line in $pushOut) { Write-Host "    $line" -ForegroundColor DarkGray }
            Write-Host "  OK - Pushed to GitHub" -ForegroundColor Green
            "$Date $(Get-Date -Format 'HH:mm:ss') - PUSHED: $msg" | Out-File -FilePath (Join-Path $Root "git_push_log.txt") -Append -Encoding utf8
        } else {
            Write-Host "  (no changes to push)" -ForegroundColor DarkGray
            "$Date $(Get-Date -Format 'HH:mm:ss') - NO CHANGES" | Out-File -FilePath (Join-Path $Root "git_push_log.txt") -Append -Encoding utf8
        }
    } catch {
        Write-Host "  Git push failed: $_" -ForegroundColor Yellow
        "$Date $(Get-Date -Format 'HH:mm:ss') - PUSH FAILED: $_" | Out-File -FilePath (Join-Path $Root "git_push_log.txt") -Append -Encoding utf8
    } finally {
        Pop-Location
    }
    Write-Host ""
}





