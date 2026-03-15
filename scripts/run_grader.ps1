# ============================================================
#  SLATEIQ GRADER SCRIPT  -  ADVANCED MULTI-SPORT [UPDATED]
#
#  Features:
#    - Advanced unified grader (all sports)
#    - Sport-specific graders (NHL, Soccer with opponent analysis)
#    - Opponent-specific recommendations
#    - Visual analytics & HTML dashboards
#    - Confidence scoring (0-100)
#
#  Usage:
#    .\run_grader.ps1                    # Grade yesterday (default)
#    .\run_grader.ps1 -Date 2026-02-26   # Grade specific date
#    .\run_grader.ps1 -NBAOnly           # NBA advanced grader only
#    .\run_grader.ps1 -NHLOnly           # NHL advanced grader only
#    .\run_grader.ps1 -SoccerOnly        # Soccer advanced grader only
#    .\run_grader.ps1 -AdvancedMode      # Use new advanced graders (all sports)
#    .\run_grader.ps1 -LegacyMode        # Use legacy graders (backward compat)
# ============================================================
param(
    [string]$Date = "",
    [switch]$NBAOnly,
    [switch]$CBBOnly,
    [switch]$NHLOnly,
    [switch]$SoccerOnly,
    [switch]$AdvancedMode = $true,  # Default: use advanced graders
    [switch]$LegacyMode             # Optional: use old graders
)

$ErrorActionPreference = "Continue"
# Always resolve to repo root regardless of where the script lives
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$Root = if ((Split-Path -Leaf $ScriptDir) -eq "scripts") { Split-Path -Parent $ScriptDir } else { $ScriptDir }
$NBADir     = "$Root\NBA"
$CBBDir     = "$Root\CBB"
$NHLDir     = "$Root\NHL"
$SoccerDir  = "$Root\Soccer"
$OutRoot = "$Root\outputs"
$GraderDir = "$Root\grader"  # NEW: advanced graders location
$ScriptsDir = "$Root\scripts"

# ── Unified actuals fetcher ──
$FetchActuals = "$Root\scripts\fetch_actuals.py"

# ── Advanced graders (NEW) ──
$UnifiedGrader   = "$GraderDir\unified_grader_with_analytics.py"
$NHLAdvGrader    = "$GraderDir\nhl_grader_advanced.py"
$SoccerAdvGrader = "$GraderDir\soccer_grader_advanced.py"

# ── Legacy graders (backward compat) ──
$LegacySlateGrader = "$Root\scripts\grading\slate_grader.py"

# ── Soccer-specific grader ──
$SoccerGraderScript = "$SoccerDir\scripts\soccer_grader.py"

if (-not $Date) { $Date = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd") }

# Force legacy mode if advanced graders don't exist
if ((-not (Test-Path $UnifiedGrader)) -or (-not (Test-Path $NHLAdvGrader))) {
    Write-Host "  NOTE: Advanced graders not found. Using legacy mode." -ForegroundColor Yellow
    $LegacyMode = $true
    $AdvancedMode = $false
}

$env:PYTHONUTF8 = "1"; $env:PYTHONIOENCODING = "utf-8"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
try { chcp 65001 | Out-Null } catch { }

if ((Test-Path "$Root\.venv\Scripts\Activate.ps1") -and (-not $env:VIRTUAL_ENV)) {
    & "$Root\.venv\Scripts\Activate.ps1"
}

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  SlateIQ ADVANCED GRADER  |  $Date  |  $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Cyan
if ($AdvancedMode -and -not $LegacyMode) {
    Write-Host "  Mode: ADVANCED (opponent analysis, confidence scoring)" -ForegroundColor Green
} else {
    Write-Host "  Mode: LEGACY (basic HIT/MISS grading)" -ForegroundColor Yellow
}
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

# -- Dated output folder --
$DateDir = "$OutRoot\$Date"
if (-not (Test-Path $DateDir)) { New-Item -ItemType Directory -Force -Path $DateDir | Out-Null }

# -- Helper function for running Python scripts --
function Run-Py {
    param([string]$Label, [string]$Dir, [string]$Script, [string[]]$PyArgs)
    Write-Host "  --> $Label" -ForegroundColor Yellow
    Push-Location $Dir
    try {
        $output = & py -3.14 $Script @PyArgs 2>&1
        $exit = $LASTEXITCODE
        $output | ForEach-Object { Write-Host "      | $_" -ForegroundColor DarkGray }
        if ($exit -ne 0) { Write-Host "      FAILED (exit $exit)" -ForegroundColor Red; return $false }
        Write-Host "      OK" -ForegroundColor Green; return $true
    }
    catch {
        Write-Host "      EXCEPTION: $_" -ForegroundColor Red; return $false
    }
    finally { Pop-Location }
}

# -- Locate combined tickets file --
$TicketsFile = "$DateDir\combined_slate_tickets_$Date.xlsx"
if (-not (Test-Path $TicketsFile)) { $TicketsFile = "$Root\combined_slate_tickets_$Date.xlsx" }

if (Test-Path $TicketsFile) {
    Write-Host "  Tickets file: $TicketsFile" -ForegroundColor DarkGray
} else {
    Write-Host "  WARNING: combined_slate_tickets_$Date.xlsx not found" -ForegroundColor Yellow
    $TicketsFile = ""
}
Write-Host ""

# -- Output paths --
$NBAActuals        = "$DateDir\actuals_nba_$Date.csv"
$CBBActuals        = "$DateDir\actuals_cbb_$Date.csv"
$NHLActuals        = "$DateDir\actuals_nhl_$Date.csv"
$SoccerActuals     = "$DateDir\actuals_soccer_$Date.csv"
$NBAGraded         = "$DateDir\graded_nba_$Date.xlsx"
$CBBGraded         = "$DateDir\graded_cbb_$Date.xlsx"
$NHLGraded         = "$DateDir\graded_nhl_$Date.xlsx"
$SoccerGraded      = "$DateDir\graded_soccer_$Date.xlsx"
$NHLRecommendations = "$DateDir\nhl_pick_recommendations_$Date.csv"
$SoccerRecommendations = "$DateDir\soccer_pick_recommendations_$Date.csv"

# =============================================================================
#  STEP 0 — APPEND YESTERDAY'S BOXSCORES TO REFERENCE DB
#  Runs first, always. Keeps slateiq_ref.db current so pipeline reads
#  fresh stats without any live ESPN calls during pipeline runs.
# =============================================================================
Write-Host "[ STEP 0: BOXSCORE REFERENCE DB ]" -ForegroundColor Magenta
Write-Host ""

$BuildBoxscoreRef = "$ScriptsDir\build_boxscore_ref.py"
if (Test-Path $BuildBoxscoreRef) {
    # Determine which sports to append based on flags
    $RefSports = @()
    if (-not $CBBOnly -and -not $NHLOnly -and -not $SoccerOnly) { $RefSports += "nba" }
    if (-not $NBAOnly -and -not $NHLOnly -and -not $SoccerOnly) { $RefSports += "cbb" }
    if ($NHLOnly -or (-not $NBAOnly -and -not $CBBOnly -and -not $SoccerOnly)) { $RefSports += "nhl" }
    if ($SoccerOnly -or (-not $NBAOnly -and -not $CBBOnly -and -not $NHLOnly)) { $RefSports += "soccer" }

    if ($RefSports.Count -gt 0) {
        $refArgs = @("--date", $Date, "--sports") + $RefSports
        Run-Py "Append boxscores to DB ($($RefSports -join ', '))" $Root $BuildBoxscoreRef $refArgs | Out-Null
        # Print DB summary after append
        & py -3.14 $BuildBoxscoreRef "--summary" 2>&1 | ForEach-Object { Write-Host "      | $_" -ForegroundColor DarkGray }
    }

    # ── Seed player IDs from CSV maps if they haven't been seeded yet ──
    $NBAIdMap = "$ScriptsDir\nba_to_espn_id_map.csv"
    if (Test-Path $NBAIdMap) {
        & py -3.14 $BuildBoxscoreRef "--seed-ids" "nba=$NBAIdMap" 2>&1 | ForEach-Object { Write-Host "      | $_" -ForegroundColor DarkGray }
    }

    # ── Upsert defense reports if they were generated recently ──
    $NBADef    = "$NBADir\scripts\nba_defense_summary.csv"
    $NHLDef    = "$NHLDir\nhl_defense_summary.csv"
    $SoccerDef = "$SoccerDir\outputs\soccer_defense_summary.csv"
    if (Test-Path $NBADef)    { & py -3.14 $BuildBoxscoreRef "--upsert-defense" "nba=$NBADef"       2>&1 | Out-Null }
    if (Test-Path $NHLDef)    { & py -3.14 $BuildBoxscoreRef "--upsert-defense" "nhl=$NHLDef"       2>&1 | Out-Null }
    if (Test-Path $SoccerDef) { & py -3.14 $BuildBoxscoreRef "--upsert-defense" "soccer=$SoccerDef" 2>&1 | Out-Null }
} else {
    Write-Host "  WARNING: build_boxscore_ref.py not found at $BuildBoxscoreRef" -ForegroundColor Yellow
    Write-Host "  Drop build_boxscore_ref.py into scripts\ to enable automatic DB updates." -ForegroundColor Yellow
}
Write-Host ""

# =============================================================================
#  NBA GRADING (ADVANCED)
# =============================================================================
if (-not $CBBOnly -and -not $NHLOnly -and -not $SoccerOnly) {
    Write-Host "[ NBA GRADING ]" -ForegroundColor Magenta
    Write-Host ""

    # Fetch actuals
    $ok = Run-Py "NBA Fetch Actuals" $Root $FetchActuals @("--sport", "NBA", "--date", $Date, "--output", $NBAActuals)

    if ($ok -or (Test-Path $NBAActuals)) {
        if ($AdvancedMode -and -not $LegacyMode) {
            # Use advanced unified grader
            $NBASlate = "$NBADir\step8_all_direction_clean.xlsx"
            if (Test-Path $NBASlate) {
                Run-Py "NBA Advanced Grade" $Root $UnifiedGrader @("--sport", "nba", "--date", $Date, "--actuals", $NBAActuals, "--slate", $NBASlate, "--output-dir", $DateDir)
                if (Test-Path $NBAGraded) { Write-Host "  NBA advanced graded -> $NBAGraded" -ForegroundColor Green }
            } else {
                Write-Host "  Skipping NBA - slate not found at $NBASlate" -ForegroundColor Yellow
            }
        } else {
            # Legacy grader
            $NBASlateExtracted = "$DateDir\nba_slate_extracted_$Date.xlsx"
            if ($TicketsFile -and (Test-Path $TicketsFile)) {
                Run-Py "NBA Slate Extract" $Root ".\scripts\extract_nba_slate.py" @("--tickets", $TicketsFile, "--out", $NBASlateExtracted)
                if (Test-Path $NBASlateExtracted) {
                    Run-Py "NBA Grade (Legacy)" $Root $LegacySlateGrader @("--sport", "NBA", "--slate", $NBASlateExtracted, "--actuals", $NBAActuals, "--output", $NBAGraded, "--date", $Date)
                }
            }
        }
    } else {
        Write-Host "  Skipping NBA - actuals fetch failed" -ForegroundColor Yellow
    }
    Write-Host ""
}

# =============================================================================
#  CBB GRADING (ADVANCED)
# =============================================================================
if (-not $NBAOnly -and -not $NHLOnly -and -not $SoccerOnly) {
    Write-Host "[ CBB GRADING ]" -ForegroundColor Magenta
    Write-Host ""

    # Fetch actuals
    $ok = Run-Py "CBB Fetch Actuals" $Root $FetchActuals @("--sport", "CBB", "--date", $Date, "--output", $CBBActuals, "--window", "0")

    if ($ok -or (Test-Path $CBBActuals)) {
        if ($AdvancedMode -and -not $LegacyMode) {
            # Use advanced unified grader
            $CBBSlate = "$CBBDir\step6_ranked_cbb.xlsx"
            if (Test-Path $CBBSlate) {
                Run-Py "CBB Advanced Grade" $Root $UnifiedGrader @("--sport", "cbb", "--date", $Date, "--actuals", $CBBActuals, "--slate", $CBBSlate, "--output-dir", $DateDir)
                if (Test-Path $CBBGraded) { Write-Host "  CBB advanced graded -> $CBBGraded" -ForegroundColor Green }
            } else {
                Write-Host "  Skipping CBB - slate not found at $CBBSlate" -ForegroundColor Yellow
            }
        } else {
            # Legacy grader
            $CBBSlateExtracted = "$DateDir\cbb_slate_extracted_$Date.csv"
            if ($TicketsFile -and (Test-Path $TicketsFile)) {
                Run-Py "CBB Slate Extract" $Root ".\scripts\extract_cbb_slate.py" @("--tickets", $TicketsFile, "--out", $CBBSlateExtracted)
                if (Test-Path $CBBSlateExtracted) {
                    Run-Py "CBB Grade (Legacy)" $Root ".\scripts\grading\grade_cbb_full_slate.py" @("--slate", $CBBSlateExtracted, "--actuals", $CBBActuals, "--out", $CBBGraded)
                }
            }
        }
    } else {
        Write-Host "  Skipping CBB - actuals fetch failed" -ForegroundColor Yellow
    }
    Write-Host ""
}

# =============================================================================
#  NHL GRADING (ADVANCED)
# =============================================================================
if ($NHLOnly -or (-not $NBAOnly -and -not $CBBOnly -and -not $SoccerOnly)) {
    Write-Host "[ NHL GRADING ]" -ForegroundColor Magenta
    Write-Host ""

    $NHLSlate = "$NHLDir\outputs\step8_nhl_direction_clean.xlsx"
    
    if (Test-Path $NHLSlate) {
        $ok = Run-Py "NHL Fetch Actuals" $Root $FetchActuals @("--sport", "NHL", "--date", $Date, "--output", $NHLActuals)

        if ($ok -or (Test-Path $NHLActuals)) {
            if ($AdvancedMode -and -not $LegacyMode -and (Test-Path $NHLAdvGrader)) {
                # Use advanced NHL grader with opponent analysis
                $OppCache = "$NHLDir\cache\s6a_nhl_opp_stats_cache.csv"
                if (Test-Path $OppCache) {
                    Run-Py "NHL Advanced Grade (with opponent analysis)" $Root $NHLAdvGrader @("--date", $Date, "--actuals", $NHLActuals, "--slate", $NHLSlate, "--opp-cache", $OppCache, "--output-dir", $DateDir)
                } else {
                    Run-Py "NHL Advanced Grade (no opponent cache)" $Root $NHLAdvGrader @("--date", $Date, "--actuals", $NHLActuals, "--slate", $NHLSlate, "--output-dir", $DateDir)
                }
                if (Test-Path $NHLGraded) { Write-Host "  NHL advanced graded -> $NHLGraded" -ForegroundColor Green }
                if (Test-Path $NHLRecommendations) { Write-Host "  Recommendations -> $NHLRecommendations" -ForegroundColor Green }
            } else {
                # Sport-specific grader (handles NHL/Soccer step8 format)
                $NHLSoccerGrader = "$ScriptsDir\nhl_soccer_grader.py"
                if (Test-Path $NHLSoccerGrader) {
                    Run-Py "NHL Grade" $Root $NHLSoccerGrader @("--sport", "NHL", "--date", $Date, "--slate", $NHLSlate, "--actuals", $NHLActuals, "--output-dir", $DateDir)
                    if (Test-Path $NHLGraded) { Write-Host "  NHL graded -> $NHLGraded" -ForegroundColor Green }
                } else {
                    Write-Host "  WARNING: nhl_soccer_grader.py not found at $NHLSoccerGrader" -ForegroundColor Red
                    Write-Host "  nhl_soccer_grader.py should be in $ScriptsDir" -ForegroundColor Yellow
                }
            }
        } else {
            Write-Host "  Skipping NHL - actuals fetch failed" -ForegroundColor Yellow
        }
    } else {
        if ($NHLOnly) { Write-Host "  WARNING: NHL slate not found at $NHLSlate" -ForegroundColor Yellow }
    }
    Write-Host ""
    
    if ($NHLOnly) {
        Write-Host "======================================================" -ForegroundColor Cyan
        Write-Host "  GRADING COMPLETE  |  $Date" -ForegroundColor Cyan
        Write-Host "======================================================" -ForegroundColor Cyan
        exit
    }
}

# =============================================================================
#  SOCCER GRADING (ADVANCED)
# =============================================================================
if ($SoccerOnly -or (-not $NBAOnly -and -not $CBBOnly -and -not $NHLOnly)) {
    Write-Host "[ SOCCER GRADING ]" -ForegroundColor Magenta
    Write-Host ""

    $SoccerSlate = "$SoccerDir\outputs\step8_soccer_direction_clean.xlsx"
    
    if (Test-Path $SoccerSlate) {
        # ── Derive game dates from the slate so we fetch the right actuals ──
        # Soccer slates often cover multiple days (e.g. today + tomorrow).
        # We collect all unique YYYY-MM-DD dates from the slate's Game Time column
        # and fetch actuals for each one, then merge into a single actuals CSV.
        $SoccerDates = @($Date)  # default: fall back to $Date if detection fails
        if (Test-Path $SoccerSlate) {
            $slateDatesRaw = & py -3.14 -c @"
import pandas as pd, sys
try:
    xf = pd.ExcelFile(r'$SoccerSlate')
    sheet = next((s for s in xf.sheet_names if 'all' in s.lower()), xf.sheet_names[0])
    df = pd.read_excel(r'$SoccerSlate', sheet_name=sheet)
    gt_col = next((c for c in df.columns if c.lower() in ('game time','game_time','gametime','kickoff','start_time','starttime','start time')), None)
    if gt_col:
        sample = str(df[gt_col].dropna().iloc[0]) if len(df[gt_col].dropna()) > 0 else ''
        # Check if the column has date info or just time
        has_date = any(c.isdigit() and len(sample) > 8 for c in [sample]) and ('/' in sample or '-' in sample)
        if has_date:
            # Full datetime — convert to ET using UTC-4 offset
            ts = pd.to_datetime(df[gt_col], utc=True, errors='coerce').dropna()
            et_offset = pd.Timedelta(hours=-4)
            dates = sorted(set((ts + et_offset).dt.date.astype(str).tolist()))
            from datetime import date
            today = str(date.today())
            dates = [d for d in dates if d <= today]
            print('\n'.join(dates) if dates else '$Date')
        else:
            # Time-only column — all games belong to the grading date
            print('$Date')
    else:
        print('$Date')
except Exception as e:
    print('$Date')
"@ 2>$null
            if ($slateDatesRaw) {
                $SoccerDates = @($slateDatesRaw -split "`n" | Where-Object { $_ -match '^\d{4}-\d{2}-\d{2}$' })
                if ($SoccerDates.Count -eq 0) { $SoccerDates = @($Date) }
            }
            Write-Host "  Soccer game dates to fetch actuals for: $($SoccerDates -join ', ')" -ForegroundColor DarkGray
        }

        # Fetch actuals for each game date and merge
        $allSoccerActuals = @()
        foreach ($sDate in $SoccerDates) {
            $sDailyActuals = "$DateDir\actuals_soccer_$sDate.csv"
            if (-not (Test-Path $sDailyActuals)) {
                Run-Py "Soccer Fetch Actuals ($sDate)" $Root $FetchActuals @("--sport", "Soccer", "--date", $sDate, "--output", $sDailyActuals) | Out-Null
            }
            if (Test-Path $sDailyActuals) { $allSoccerActuals += $sDailyActuals }
        }

        # Merge daily actuals into the main SoccerActuals path
        if ($allSoccerActuals.Count -gt 1) {
            & py -3.14 -c @"
import pandas as pd
files = r'$($allSoccerActuals -join '|')'.split('|')
pd.concat([pd.read_csv(f) for f in files if __import__('os').path.exists(f)], ignore_index=True).to_csv(r'$SoccerActuals', index=False)
print(f'Merged {len(files)} actuals files -> $SoccerActuals')
"@ 2>&1 | ForEach-Object { Write-Host "      | $_" -ForegroundColor DarkGray }
        } elseif ($allSoccerActuals.Count -eq 1 -and $allSoccerActuals[0] -ne $SoccerActuals) {
            Copy-Item $allSoccerActuals[0] $SoccerActuals -Force
        }

        # Run Soccer-specific DB grader first (uses dated archive slate)
        $SoccerGradedOut = "$SoccerDir\outputs\graded\soccer_graded_$Date.xlsx"
        $SoccerSlateForGrading = "$DateDir\step8_soccer_direction_clean_$Date.xlsx"
        if (-not (Test-Path $SoccerSlateForGrading)) { $SoccerSlateForGrading = $SoccerSlate }
        if (Test-Path $SoccerGraderScript) {
            Run-Py "Soccer Grade (DB)" $Root $SoccerGraderScript @("--date", $Date, "--slate", $SoccerSlateForGrading, "--out", "$SoccerDir\outputs\graded")
            if (Test-Path $SoccerGradedOut) {
                Copy-Item $SoccerGradedOut "$DateDir\soccer_graded_$Date.xlsx" -Force
                Write-Host "  Soccer graded -> $SoccerGradedOut" -ForegroundColor Green
            }
        } else {
            Write-Host "  WARNING: soccer_grader.py not found at $SoccerGraderScript" -ForegroundColor Yellow
        }

        if ($ok -or (Test-Path $SoccerActuals)) {
            if ($AdvancedMode -and -not $LegacyMode -and (Test-Path $SoccerAdvGrader)) {
                $OppCache = "$SoccerDir\cache\s6a_soccer_opp_stats_cache.csv"
                if (Test-Path $OppCache) {
                    Run-Py "Soccer Advanced Grade (with opponent analysis)" $Root $SoccerAdvGrader @("--date", $Date, "--actuals", $SoccerActuals, "--slate", $SoccerSlate, "--opp-cache", $OppCache, "--output-dir", $DateDir)
                } else {
                    Run-Py "Soccer Advanced Grade (no opponent cache)" $Root $SoccerAdvGrader @("--date", $Date, "--actuals", $SoccerActuals, "--slate", $SoccerSlate, "--output-dir", $DateDir)
                }
                if (Test-Path $SoccerGraded) { Write-Host "  Soccer advanced graded -> $SoccerGraded" -ForegroundColor Green }
            } else {
                $NHLSoccerGrader = "$ScriptsDir\nhl_soccer_grader.py"
                if (Test-Path $NHLSoccerGrader) {
                    Run-Py "Soccer Grade (legacy)" $Root $NHLSoccerGrader @("--sport", "Soccer", "--date", $Date, "--slate", $SoccerSlate, "--actuals", $SoccerActuals, "--output-dir", $DateDir)
                    if (Test-Path $SoccerGraded) { Write-Host "  Soccer graded -> $SoccerGraded" -ForegroundColor Green }
                }
            }
        } else {
            Write-Host "  Skipping Soccer legacy grade - actuals fetch failed" -ForegroundColor Yellow
        }
    } else {
        if ($SoccerOnly) { Write-Host "  WARNING: Soccer slate not found at $SoccerSlate" -ForegroundColor Yellow }
    }
    Write-Host ""
    
    if ($SoccerOnly) {
        Write-Host "======================================================" -ForegroundColor Cyan
        Write-Host "  GRADING COMPLETE  |  $Date" -ForegroundColor Cyan
        Write-Host "======================================================" -ForegroundColor Cyan
        exit
    }
}

# =============================================================================
#  TICKET GRADING (if applicable)
# =============================================================================
Write-Host "[ TICKET GRADING ]" -ForegroundColor Magenta
Write-Host ""

if (-not $TicketsFile) {
    Write-Host "  Skipping - no tickets file found for $Date" -ForegroundColor Yellow
} elseif (-not (Test-Path $NBAActuals)) {
    Write-Host "  Skipping - NBA actuals missing" -ForegroundColor Yellow
} else {
    if (-not (Test-Path $CBBActuals)) {
        Run-Py "CBB Fetch Actuals (ticket grader)" $Root $FetchActuals @("--sport", "CBB", "--date", $Date, "--output", $CBBActuals, "--window", "0")
    } else {
        Write-Host "  CBB actuals already present" -ForegroundColor DarkGray
    }

    if (Test-Path $CBBActuals) {
        Run-Py "Ticket Grade" $Root ".\scripts\combined_ticket_grader.py" @("--tickets", $TicketsFile, "--nba_actuals", $NBAActuals, "--cbb_actuals", $CBBActuals, "--out", "$DateDir\combined_tickets_graded_$Date.xlsx")
    }
}
Write-Host ""

# =============================================================================
#  SUMMARY
# =============================================================================
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  GRADING COMPLETE  |  $Date" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

$found = Get-ChildItem $DateDir -Filter "*graded*" -ErrorAction SilentlyContinue
if ($found) {
    Write-Host "  Output: $DateDir" -ForegroundColor Green
    $found | ForEach-Object { Write-Host "    $($_.Name)" -ForegroundColor Green }
    Write-Host ""
    Write-Host "  Recommendations & Analysis:" -ForegroundColor Green
    $recs = Get-ChildItem $DateDir -Filter "*recommendation*" -ErrorAction SilentlyContinue
    if ($recs) {
        $recs | ForEach-Object { Write-Host "    $($_.Name)" -ForegroundColor Green }
    }
} else {
    Write-Host "  No graded files found in $DateDir" -ForegroundColor Yellow
}
Write-Host ""

# =============================================================================
#  BUILD GRADES HTML
# =============================================================================
Write-Host "[ BUILDING GRADES HTML ]" -ForegroundColor Magenta
Write-Host ""

$BuildGradeReport = "$ScriptsDir\build_grade_report.py"

if (Test-Path $BuildGradeReport) {
    $htmlArgs = @("--date", $Date)
    if (Test-Path $NBAGraded)    { $htmlArgs += "--nba";    $htmlArgs += $NBAGraded }
    if (Test-Path $CBBGraded)    { $htmlArgs += "--cbb";    $htmlArgs += $CBBGraded }
    if (Test-Path $NHLGraded)    { $htmlArgs += "--nhl";    $htmlArgs += $NHLGraded }
    if (Test-Path $SoccerGraded) { $htmlArgs += "--soccer"; $htmlArgs += $SoccerGraded }

    if ($htmlArgs.Count -gt 2) {
        $htmlOut = "$Root\ui_runner\templates\slate_eval_$Date.html"
        $htmlArgs += "--out"; $htmlArgs += $htmlOut
        Run-Py "Grades HTML" $Root $BuildGradeReport $htmlArgs
        if (Test-Path $htmlOut) { Write-Host "  HTML -> $htmlOut" -ForegroundColor Green }
    } else {
        Write-Host "  Skipping - no graded files found for $Date" -ForegroundColor Yellow
    }
} else {
    Write-Host "  WARNING: build_grade_report.py not found at $BuildGradeReport" -ForegroundColor Yellow
    Write-Host "  Drop build_grade_report.py into $Root to enable grade reports." -ForegroundColor Yellow
}
Write-Host ""

# =============================================================================
#  BUILD TICKET EVAL HTML
# =============================================================================
Write-Host "[ BUILDING TICKET EVAL HTML ]" -ForegroundColor Magenta
Write-Host ""

$BuildTicketEval = "$ScriptsDir\build_ticket_eval_html.py"
$TicketGraded    = "$DateDir\combined_tickets_graded_$Date.xlsx"

if (-not (Test-Path $BuildTicketEval)) {
    Write-Host "  WARNING: build_ticket_eval_html.py not found at $BuildTicketEval" -ForegroundColor Yellow
    Write-Host "  Drop build_ticket_eval_html.py into $Root to enable ticket eval reports." -ForegroundColor Yellow
} elseif (-not (Test-Path $TicketGraded)) {
    Write-Host "  Skipping - no graded tickets file found for $Date" -ForegroundColor Yellow
} else {
    $ticketHtmlOut = "$Root\ui_runner\templates\ticket_eval_$Date.html"
    Run-Py "Ticket Eval HTML" $Root $BuildTicketEval @("--date", $Date, "--graded", $TicketGraded, "--out", $ticketHtmlOut)
    if (Test-Path $ticketHtmlOut) { Write-Host "  HTML -> $ticketHtmlOut" -ForegroundColor Green }
}
Write-Host ""

Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  SlateIQ Grader Run Complete" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

# =============================================================================
#  GIT PUSH — push grade reports to GitHub so Railway deploys them
# =============================================================================
Write-Host "[ PUSHING GRADE REPORTS TO GITHUB ]" -ForegroundColor Magenta
Write-Host ""

Push-Location $Root
try {
    $slateEval  = "ui_runner/templates/slate_eval_$Date.html"
    $ticketEval = "ui_runner/templates/ticket_eval_$Date.html"

    $filesToAdd = @()
    if (Test-Path "$Root\$slateEval")  { $filesToAdd += $slateEval }
    if (Test-Path "$Root\$ticketEval") { $filesToAdd += $ticketEval }

    if ($filesToAdd.Count -eq 0) {
        Write-Host "  No HTML reports found to push — skipping git." -ForegroundColor Yellow
    } else {
        git add @($filesToAdd) 2>&1 | Out-Null
        git commit -m "grades: $Date slate + ticket eval" 2>&1 | Out-Null
        git push origin main 2>&1 | ForEach-Object { Write-Host "  | $_" -ForegroundColor DarkGray }
        Write-Host "  Pushed: $($filesToAdd -join ', ')" -ForegroundColor Green
    }
} catch {
    Write-Host "  Git push failed: $_" -ForegroundColor Red
} finally {
    Pop-Location
}
Write-Host ""

