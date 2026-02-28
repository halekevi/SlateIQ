# ============================================================
#  PROP PIPELINE  -  Master Run Script  [OPTIMIZED]
#
#  Usage:
#    .\run_pipeline.ps1                  # Full run (NBA + CBB + Combined)
#    .\run_pipeline.ps1 -NBAOnly         # NBA pipeline only
#    .\run_pipeline.ps1 -CBBOnly         # CBB pipeline only
#    .\run_pipeline.ps1 -CombinedOnly    # Re-run combined using existing outputs
#    .\run_pipeline.ps1 -RefreshCache    # Wipe + rebuild ESPN cache before NBA
#    .\run_pipeline.ps1 -ForceAll        # Re-run everything regardless
#    .\run_pipeline.ps1 -CacheAgeDays 7  # Auto-wipe cache if older than N days (default: 7)
# ============================================================
param(
    [switch]$NBAOnly,
    [switch]$CBBOnly,
    [switch]$CombinedOnly,
    [switch]$RefreshCache,
    [switch]$ForceAll,
    [int]$CacheAgeDays = 7
)

$ErrorActionPreference = "Continue"
$Date      = Get-Date -Format "yyyy-MM-dd"
$StartTime = Get-Date

# -- Paths --------------------------------------------------------------------
$Root    = Split-Path -Parent $MyInvocation.MyCommand.Definition
$NBADir  = "$Root\NbaPropPipelineA"
$CBBDir  = "$Root\cbb2"
$OutDir  = "$Root\outputs\$Date"

# Ensure dated output folder exists
if (-not (Test-Path $OutDir)) {
    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
}

# -- Encoding -----------------------------------------------------------------
$env:PYTHONUTF8       = "1"
$env:PYTHONIOENCODING = "utf-8"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
try { chcp 65001 | Out-Null } catch { }

# -- Activate venv ------------------------------------------------------------
if (Test-Path "$Root\.venv\Scripts\Activate.ps1") {
    & "$Root\.venv\Scripts\Activate.ps1"
}

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  PROP PIPELINE  |  $Date  |  $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

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
        if ($Arguments) {
            $argArray = $Arguments -split ' '
            $output = & py -3.14 $Script @argArray 2>&1
        } else {
            $output = & py -3.14 $Script 2>&1
        }
        $exit = $LASTEXITCODE
        $output | ForEach-Object { Write-Host "      | $_" -ForegroundColor DarkGray }
        if ($exit -ne 0) {
            Write-Host "      FAILED (exit $exit)" -ForegroundColor Red
            return $false
        }
        Write-Host "      OK" -ForegroundColor Green
        return $true
    } catch {
        Write-Host "      EXCEPTION: $_" -ForegroundColor Red
        return $false
    } finally {
        Pop-Location
    }
}

# -- Helper: auto-wipe ESPN cache if older than $CacheAgeDays ----------------
function Check-AutoRefreshCache {
    $cacheFile = "$NBADir\nba_espn_boxscore_cache.csv"
    if (Test-Path $cacheFile) {
        $age = (Get-Date) - (Get-Item $cacheFile).LastWriteTime
        if ($age.TotalDays -gt $CacheAgeDays) {
            Write-Host "  [Cache] ESPN cache is $([math]::Round($age.TotalDays, 1)) days old (threshold: $CacheAgeDays). Auto-wiping..." -ForegroundColor Yellow
            Remove-Item "$NBADir\nba_espn_boxscore_cache.csv" -Force -ErrorAction SilentlyContinue
            Remove-Item "$NBADir\nba_to_espn_id_map.csv"      -Force -ErrorAction SilentlyContinue
            Write-Host "  [Cache] Wiped. Will rebuild fresh." -ForegroundColor Green
        } else {
            Write-Host "  [Cache] ESPN cache is $([math]::Round($age.TotalDays, 1)) days old - keeping." -ForegroundColor DarkGray
        }
    }
}

# =============================================================================
#  PARALLEL NBA + CBB PIPELINE
# =============================================================================
$NBASuccess = $false
$CBBSuccess = $false

if ($CombinedOnly) {
    # Just check existing outputs
    $NBASuccess = Test-Path "$NBADir\step8_all_direction_clean.xlsx"
    $CBBSuccess = Test-Path "$CBBDir\step6_ranked_cbb.xlsx"
    if (-not $NBASuccess) { Write-Host "  WARNING: NBA slate not found at $NBADir\step8_all_direction_clean.xlsx" -ForegroundColor Yellow }
    if (-not $CBBSuccess) { Write-Host "  WARNING: CBB slate not found at $CBBDir\step6_ranked_cbb.xlsx" -ForegroundColor Yellow }

} elseif ($NBAOnly) {
    # ---- NBA only (serial) --------------------------------------------------
    Write-Host "[ NBA PIPELINE ]" -ForegroundColor Magenta
    Write-Host ""

    if (Test-Path "$NBADir\RUN_COMPLETE.flag") { Remove-Item "$NBADir\RUN_COMPLETE.flag" -Force }
    if ($RefreshCache) {
        Write-Host "  [Cache] Wiping ESPN cache files..." -ForegroundColor Yellow
        Remove-Item "$NBADir\nba_espn_boxscore_cache.csv" -Force -ErrorAction SilentlyContinue
        Remove-Item "$NBADir\nba_to_espn_id_map.csv"      -Force -ErrorAction SilentlyContinue
        Write-Host "  [Cache] Done." -ForegroundColor Green
        Write-Host ""
    } else {
        Check-AutoRefreshCache
    }

    $ok = $true
    if ($ok) { $ok = Run-Step "NBA Step 1 - Fetch PrizePicks"       $NBADir ".\step1_fetch_prizepicks_api.py"          "--league_id 7 --game_mode pickem --per_page 250 --max_pages 5 --sleep 2.0 --cooldown_seconds 90 --max_cooldowns 3 --jitter_seconds 10.0 --output step1_pp_props_today.csv" }
    if ($ok) { $ok = Run-Step "NBA Step 2 - Attach Pick Types"       $NBADir ".\step2_attach_picktypes.py"              "--input step1_pp_props_today.csv --output step2_with_picktypes.csv" }
    if ($ok) { $ok = Run-Step "NBA Step 3 - Attach Defense"          $NBADir ".\step3_attach_defense.py"                "--input step2_with_picktypes.csv --defense .\defense_team_summary.csv --output step3_with_defense.csv" }
    if ($ok) { $ok = Run-Step "NBA Step 4 - Player Stats (ESPN)"     $NBADir ".\step4_attach_player_stats_espn_cache.py" "--slate step3_with_defense.csv --out step4_with_stats.csv --season 2025-26 --date $Date --days 35 --cache nba_espn_boxscore_cache.csv --idmap nba_to_espn_id_map.csv --n 10 --sleep 0.8 --retries 4 --connect-timeout 8 --timeout 30 --debug-misses no_espn_player_debug.csv" }
    if ($ok) { $ok = Run-Step "NBA Step 5 - Line Hit Rates"          $NBADir ".\step5_add_line_hit_rates.py"            "--input step4_with_stats.csv --output step5_with_hit_rates.csv" }
    if ($ok) { $ok = Run-Step "NBA Step 6 - Team Role Context"       $NBADir ".\step6_team_role_context.py"             "--input step5_with_hit_rates.csv --output step6_with_team_role_context.csv" }
    if ($ok) { $ok = Run-Step "NBA Step 7 - Rank Props"              $NBADir ".\step7_rank_props.py"                    "--input step6_with_team_role_context.csv --output step7_ranked_props.xlsx" }
    if ($ok) { $ok = Run-Step "NBA Step 8 - Direction Context"       $NBADir ".\step8_add_direction_context.py"         "--input step7_ranked_props.xlsx --sheet ALL --output step8_all_direction_clean.xlsx" }
    if ($ok) { $ok = Run-Step "NBA Step 9 - Build Tickets"           $NBADir ".\step9_build_tickets.py"                 "--input step8_all_direction_clean.xlsx --output best_tickets.xlsx --min_hit_rate 0.8 --legs 2,3,4" }

    if ($ok) {
        New-Item -ItemType File -Force -Path "$NBADir\RUN_COMPLETE.flag" | Out-Null
        $NBASuccess = $true
        Write-Host ""
        Write-Host "  NBA complete." -ForegroundColor Green
    } else {
        Write-Host ""
        Write-Host "  NBA FAILED." -ForegroundColor Red
    }
    Write-Host ""

} elseif ($CBBOnly) {
    # ---- CBB only (serial) --------------------------------------------------
    Write-Host "[ CBB PIPELINE ]" -ForegroundColor Magenta
    Write-Host ""

    $ok = $true
    if ($ok) { $ok = Run-Step "CBB Step 1 - Fetch PrizePicks"        $CBBDir ".\pp_cbb_scraper.py"                      "--out step1_cbb.csv" }
    if ($ok) { $ok = Run-Step "CBB Step 2 - Normalize"               $CBBDir ".\cbb_step2_normalize.py"                 "--input step1_cbb.csv --output step2_cbb.csv" }
    if ($ok) { $ok = Run-Step "CBB Step 3 - Attach Defense Rankings" $CBBDir ".\cbb_step3b_attach_def_rankings.py"      "--input step2_cbb.csv --defense cbb_def_rankings.csv --output step3b_with_def_rankings_cbb.csv" }
    if ($ok) { $ok = Run-Step "CBB Step 4 - Attach ESPN IDs"         $CBBDir ".\step5_attach_espn_ids.py"               "--input step3b_with_def_rankings_cbb.csv --output step3_cbb.csv --master data/ncaa_mbb_athletes_master.csv" }
    if ($ok) { $ok = Run-Step "CBB Step 5 - Boxscore Stats"          $CBBDir ".\cbb_step5b_attach_boxscore_stats.py"    "--input step3_cbb.csv --output step5b_cbb.csv" }
    if ($ok) { $ok = Run-Step "CBB Step 6 - Rank Props"              $CBBDir ".\cbb_step6_rank_props.py"                "--input step5b_cbb.csv --output step6_ranked_cbb.xlsx" }

    if ($ok) {
        $CBBSuccess = $true
        Write-Host ""
        Write-Host "  CBB complete." -ForegroundColor Green
    } else {
        Write-Host ""
        Write-Host "  CBB FAILED." -ForegroundColor Red
    }
    Write-Host ""

} else {
    # ---- FULL RUN: NBA + CBB in PARALLEL ------------------------------------
    Write-Host "[ PARALLEL NBA + CBB PIPELINE ]" -ForegroundColor Magenta
    Write-Host ""

    if (Test-Path "$NBADir\RUN_COMPLETE.flag") { Remove-Item "$NBADir\RUN_COMPLETE.flag" -Force }

    # Auto-wipe ESPN cache before kicking off jobs
    if ($RefreshCache) {
        Write-Host "  [Cache] Wiping ESPN cache files..." -ForegroundColor Yellow
        Remove-Item "$NBADir\nba_espn_boxscore_cache.csv" -Force -ErrorAction SilentlyContinue
        Remove-Item "$NBADir\nba_to_espn_id_map.csv"      -Force -ErrorAction SilentlyContinue
        Write-Host "  [Cache] Done." -ForegroundColor Green
        Write-Host ""
    } else {
        Check-AutoRefreshCache
    }

    Write-Host "  Starting NBA and CBB pipelines simultaneously..." -ForegroundColor Cyan
    Write-Host ""

    # -- NBA Job --------------------------------------------------------------
    $NBAJob = Start-Job -ScriptBlock {
        param($NBADir, $Date)
        $env:PYTHONUTF8 = "1"; $env:PYTHONIOENCODING = "utf-8"

        function Run-Step-Job {
            param([string]$Label, [string]$Dir, [string]$Script, [string]$Arguments = "")
            Write-Output "[NBA] --> $Label"
            Push-Location $Dir
            try {
                if ($Arguments) { $argArray = $Arguments -split ' '; $output = & py -3.14 $Script @argArray 2>&1 }
                else             { $output = & py -3.14 $Script 2>&1 }
                $exit = $LASTEXITCODE
                $output | ForEach-Object { Write-Output "      | $_" }
                if ($exit -ne 0) { Write-Output "[NBA] FAILED: $Label (exit $exit)"; return $false }
                Write-Output "[NBA] OK: $Label"; return $true
            } catch { Write-Output "[NBA] EXCEPTION in $Label`: $_"; return $false
            } finally { Pop-Location }
        }

        $ok = $true
        if ($ok) { $ok = Run-Step-Job "NBA Step 1 - Fetch PrizePicks"        $NBADir ".\step1_fetch_prizepicks_api.py"           "--league_id 7 --game_mode pickem --per_page 250 --max_pages 5 --sleep 2.0 --cooldown_seconds 90 --max_cooldowns 3 --jitter_seconds 10.0 --output step1_pp_props_today.csv" }
        if ($ok) { $ok = Run-Step-Job "NBA Step 2 - Attach Pick Types"        $NBADir ".\step2_attach_picktypes.py"               "--input step1_pp_props_today.csv --output step2_with_picktypes.csv" }
        if ($ok) { $ok = Run-Step-Job "NBA Step 3 - Attach Defense"           $NBADir ".\step3_attach_defense.py"                 "--input step2_with_picktypes.csv --defense .\defense_team_summary.csv --output step3_with_defense.csv" }
        if ($ok) { $ok = Run-Step-Job "NBA Step 4 - Player Stats (ESPN)"      $NBADir ".\step4_attach_player_stats_espn_cache.py" "--slate step3_with_defense.csv --out step4_with_stats.csv --season 2025-26 --date $Date --days 35 --cache nba_espn_boxscore_cache.csv --idmap nba_to_espn_id_map.csv --n 10 --sleep 0.8 --retries 4 --connect-timeout 8 --timeout 30 --debug-misses no_espn_player_debug.csv" }
        if ($ok) { $ok = Run-Step-Job "NBA Step 5 - Line Hit Rates"           $NBADir ".\step5_add_line_hit_rates.py"             "--input step4_with_stats.csv --output step5_with_hit_rates.csv" }
        if ($ok) { $ok = Run-Step-Job "NBA Step 6 - Team Role Context"        $NBADir ".\step6_team_role_context.py"              "--input step5_with_hit_rates.csv --output step6_with_team_role_context.csv" }
        if ($ok) { $ok = Run-Step-Job "NBA Step 7 - Rank Props"               $NBADir ".\step7_rank_props.py"                     "--input step6_with_team_role_context.csv --output step7_ranked_props.xlsx" }
        if ($ok) { $ok = Run-Step-Job "NBA Step 8 - Direction Context"        $NBADir ".\step8_add_direction_context.py"          "--input step7_ranked_props.xlsx --sheet ALL --output step8_all_direction_clean.xlsx" }
        if ($ok) { $ok = Run-Step-Job "NBA Step 9 - Build Tickets"            $NBADir ".\step9_build_tickets.py"                  "--input step8_all_direction_clean.xlsx --output best_tickets.xlsx --min_hit_rate 0.8 --legs 2,3,4" }

        return $ok
    } -ArgumentList $NBADir, $Date

    # -- CBB Job --------------------------------------------------------------
    $CBBJob = Start-Job -ScriptBlock {
        param($CBBDir)
        $env:PYTHONUTF8 = "1"; $env:PYTHONIOENCODING = "utf-8"

        function Run-Step-Job {
            param([string]$Label, [string]$Dir, [string]$Script, [string]$Arguments = "")
            Write-Output "[CBB] --> $Label"
            Push-Location $Dir
            try {
                if ($Arguments) { $argArray = $Arguments -split ' '; $output = & py -3.14 $Script @argArray 2>&1 }
                else             { $output = & py -3.14 $Script 2>&1 }
                $exit = $LASTEXITCODE
                $output | ForEach-Object { Write-Output "      | $_" }
                if ($exit -ne 0) { Write-Output "[CBB] FAILED: $Label (exit $exit)"; return $false }
                Write-Output "[CBB] OK: $Label"; return $true
            } catch { Write-Output "[CBB] EXCEPTION in $Label`: $_"; return $false
            } finally { Pop-Location }
        }

        $ok = $true
        if ($ok) { $ok = Run-Step-Job "CBB Step 1 - Fetch PrizePicks"        $CBBDir ".\pp_cbb_scraper.py"                   "--out step1_cbb.csv" }
        if ($ok) { $ok = Run-Step-Job "CBB Step 2 - Normalize"               $CBBDir ".\cbb_step2_normalize.py"              "--input step1_cbb.csv --output step2_cbb.csv" }
        if ($ok) { $ok = Run-Step-Job "CBB Step 3 - Attach Defense Rankings" $CBBDir ".\cbb_step3b_attach_def_rankings.py"   "--input step2_cbb.csv --defense cbb_def_rankings.csv --output step3b_with_def_rankings_cbb.csv" }
        if ($ok) { $ok = Run-Step-Job "CBB Step 4 - Attach ESPN IDs"         $CBBDir ".\step5_attach_espn_ids.py"            "--input step3b_with_def_rankings_cbb.csv --output step3_cbb.csv --master data/ncaa_mbb_athletes_master.csv" }
        if ($ok) { $ok = Run-Step-Job "CBB Step 5 - Boxscore Stats"          $CBBDir ".\cbb_step5b_attach_boxscore_stats.py" "--input step3_cbb.csv --output step5b_cbb.csv" }
        if ($ok) { $ok = Run-Step-Job "CBB Step 6 - Rank Props"              $CBBDir ".\cbb_step6_rank_props.py"             "--input step5b_cbb.csv --output step6_ranked_cbb.xlsx" }

        return $ok
    } -ArgumentList $CBBDir

    # -- Wait for both jobs, stream output ------------------------------------
    Write-Host "  [Waiting for NBA + CBB to finish...]" -ForegroundColor DarkGray
    Write-Host ""

    while ($NBAJob.State -eq 'Running' -or $CBBJob.State -eq 'Running') {
        # Stream any available output
        Receive-Job $NBAJob -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
        Receive-Job $CBBJob -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
        Start-Sleep -Milliseconds 500
    }

    # Drain remaining output
    Receive-Job $NBAJob -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }
    Receive-Job $CBBJob -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "  $_" -ForegroundColor DarkGray }

    $NBASuccess = ($NBAJob.State -eq 'Completed') -and (Receive-Job $NBAJob -ErrorAction SilentlyContinue) -ne $false
    $CBBSuccess = ($CBBJob.State -eq 'Completed') -and (Receive-Job $CBBJob -ErrorAction SilentlyContinue) -ne $false

    # Fallback: check output files directly as the most reliable success signal
    if (Test-Path "$NBADir\step8_all_direction_clean.xlsx") { $NBASuccess = $true }
    if (Test-Path "$CBBDir\step6_ranked_cbb.xlsx")          { $CBBSuccess = $true }

    Remove-Job $NBAJob, $CBBJob -Force

    if ($NBASuccess) {
        New-Item -ItemType File -Force -Path "$NBADir\RUN_COMPLETE.flag" | Out-Null
        Write-Host "  NBA complete." -ForegroundColor Green
    } else {
        Write-Host "  NBA FAILED." -ForegroundColor Red
    }

    if ($CBBSuccess) {
        Write-Host "  CBB complete." -ForegroundColor Green
    } else {
        Write-Host "  CBB FAILED." -ForegroundColor Red
    }
    Write-Host ""
}

# =============================================================================
#  COMBINED SLATE + TICKETS
# =============================================================================
if (-not $NBAOnly -and -not $CBBOnly) {
    if ($NBASuccess -and $CBBSuccess) {
        Write-Host "[ COMBINED SLATE + TICKETS ]" -ForegroundColor Magenta
        Write-Host ""

        $CombinedOut = "$OutDir\combined_slate_tickets_$Date.xlsx"

        $ok = Run-Step "Combined Slate + Tickets" $Root `
            ".\scripts\scripts\combined_slate_tickets.py" `
            "--nba `"$NBADir\step8_all_direction_clean.xlsx`" --cbb `"$CBBDir\step6_ranked_cbb.xlsx`" --date $Date --output `"$CombinedOut`" --tiers A,B,C,D --max-tickets 3 --write-web --web-outdir `"C:\Users\halek\OneDrive\Desktop\Vision Board\NbaPropPipelines\NBA-Pipelines\ui_runner\docs`""

        if ($ok) {
            # Also copy to root for any legacy references
            Copy-Item $CombinedOut "$Root\combined_slate_tickets_$Date.xlsx" -Force -ErrorAction SilentlyContinue
            Write-Host "  Saved -> $CombinedOut" -ForegroundColor Green
            Write-Host "  Copied -> $Root\combined_slate_tickets_$Date.xlsx" -ForegroundColor DarkGray
        }
        Write-Host ""
    } else {
        Write-Host "[ COMBINED ] Skipped - requires both NBA + CBB to succeed." -ForegroundColor DarkGray
        Write-Host ""
    }
}

# =============================================================================
#  SUMMARY
# =============================================================================
$Elapsed = (Get-Date) - $StartTime
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ("  DONE  |  Elapsed: {0}" -f $Elapsed.ToString("mm\:ss")) -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

# -- Auto-push docs to GitHub Pages -------------------------------------------
if ($NBASuccess -and $CBBSuccess -and -not $NBAOnly -and -not $CBBOnly) {
    Write-Host "[ GIT ] Pushing docs to GitHub Pages..." -ForegroundColor Cyan
    Push-Location $Root
    try {
        git add "ui_runner/docs/tickets_latest.html" "ui_runner/docs/tickets_latest.json" 2>&1 | Out-Null
        $msg = "chore: pipeline update $Date $(Get-Date -Format 'HH:mm')"
        $commitOut = git commit -m $msg 2>&1
        if ($LASTEXITCODE -eq 0) {
            $pushOut = git push origin main 2>&1
            $pushOut | ForEach-Object { Write-Host "  | $_" -ForegroundColor DarkGray }
            Write-Host "  OK - Pushed to GitHub Pages" -ForegroundColor Green
            # Log push result
            "$Date $(Get-Date -Format 'HH:mm:ss') - PUSHED: $msg" | Out-File -FilePath "$Root\git_push_log.txt" -Append -Encoding utf8
        } else {
            Write-Host "  (no changes to push)" -ForegroundColor DarkGray
            "$Date $(Get-Date -Format 'HH:mm:ss') - NO CHANGES" | Out-File -FilePath "$Root\git_push_log.txt" -Append -Encoding utf8
        }
    } catch {
        Write-Host "  Git push failed: $_" -ForegroundColor Yellow
        "$Date $(Get-Date -Format 'HH:mm:ss') - PUSH FAILED: $_" | Out-File -FilePath "$Root\git_push_log.txt" -Append -Encoding utf8
    } finally {
        Pop-Location
    }
    Write-Host ""
}

