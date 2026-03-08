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
$Root    = Split-Path -Parent $MyInvocation.MyCommand.Definition
$NBADir     = "$Root\NBA"
$CBBDir     = "$Root\CBB"
$NHLDir     = "$Root\NHL"
$SoccerDir  = "$Root\Soccer"
$OutRoot = "$Root\outputs"
$GraderDir = "$Root\grader"  # NEW: advanced graders location

# ── Unified actuals fetcher ──
$FetchActuals = "$Root\scripts\fetch_actuals.py"

# ── Advanced graders (NEW) ──
$UnifiedGrader   = "$GraderDir\unified_grader_with_analytics.py"
$NHLAdvGrader    = "$GraderDir\nhl_grader_advanced.py"
$SoccerAdvGrader = "$GraderDir\soccer_grader_advanced.py"

# ── Legacy graders (backward compat) ──
$LegacySlateGrader = "$Root\scripts\grading\slate_grader.py"

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

    $NHLSlate = "$NHLDir\step8_nhl_direction_clean.xlsx"
    
    if (Test-Path $NHLSlate) {
        $ok = Run-Py "NHL Fetch Actuals" $Root $FetchActuals @("--sport", "NHL", "--date", $Date, "--output", $NHLActuals)

        if ($ok -or (Test-Path $NHLActuals)) {
            if ($AdvancedMode -and -not $LegacyMode -and (Test-Path $NHLAdvGrader)) {
                # Use advanced NHL grader with opponent analysis
                $OppCache = "$NHLDir\s6a_nhl_opp_stats_cache.csv"
                if (Test-Path $OppCache) {
                    Run-Py "NHL Advanced Grade (with opponent analysis)" $Root $NHLAdvGrader @("--date", $Date, "--actuals", $NHLActuals, "--slate", $NHLSlate, "--opp-cache", $OppCache, "--output-dir", $DateDir)
                } else {
                    Run-Py "NHL Advanced Grade (no opponent cache)" $Root $NHLAdvGrader @("--date", $Date, "--actuals", $NHLActuals, "--slate", $NHLSlate, "--output-dir", $DateDir)
                }
                if (Test-Path $NHLGraded) { Write-Host "  NHL advanced graded -> $NHLGraded" -ForegroundColor Green }
                if (Test-Path $NHLRecommendations) { Write-Host "  Recommendations -> $NHLRecommendations" -ForegroundColor Green }
            } else {
                # Sport-specific grader (handles NHL/Soccer step8 format)
                $NHLSoccerGrader = "$Root\nhl_soccer_grader.py"
                if (Test-Path $NHLSoccerGrader) {
                    Run-Py "NHL Grade" $Root $NHLSoccerGrader @("--sport", "NHL", "--date", $Date, "--slate", $NHLSlate, "--actuals", $NHLActuals, "--output-dir", $DateDir)
                    if (Test-Path $NHLGraded) { Write-Host "  NHL graded -> $NHLGraded" -ForegroundColor Green }
                } else {
                    Write-Host "  WARNING: nhl_soccer_grader.py not found at $NHLSoccerGrader" -ForegroundColor Red
                    Write-Host "  Drop nhl_soccer_grader.py into $Root to enable NHL grading." -ForegroundColor Yellow
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

    $SoccerSlate = "$SoccerDir\step8_soccer_direction_clean.xlsx"
    
    if (Test-Path $SoccerSlate) {
        $ok = Run-Py "Soccer Fetch Actuals" $Root $FetchActuals @("--sport", "Soccer", "--date", $Date, "--output", $SoccerActuals)

        if ($ok -or (Test-Path $SoccerActuals)) {
            if ($AdvancedMode -and -not $LegacyMode -and (Test-Path $SoccerAdvGrader)) {
                # Use advanced soccer grader with multi-league & position analysis
                $OppCache = "$SoccerDir\s6a_soccer_opp_stats_cache.csv"
                if (Test-Path $OppCache) {
                    Run-Py "Soccer Advanced Grade (with opponent analysis)" $Root $SoccerAdvGrader @("--date", $Date, "--actuals", $SoccerActuals, "--slate", $SoccerSlate, "--opp-cache", $OppCache, "--output-dir", $DateDir)
                } else {
                    Run-Py "Soccer Advanced Grade (no opponent cache)" $Root $SoccerAdvGrader @("--date", $Date, "--actuals", $SoccerActuals, "--slate", $SoccerSlate, "--output-dir", $DateDir)
                }
                if (Test-Path $SoccerGraded) { Write-Host "  Soccer advanced graded -> $SoccerGraded" -ForegroundColor Green }
                if (Test-Path $SoccerRecommendations) { Write-Host "  Recommendations -> $SoccerRecommendations" -ForegroundColor Green }
            } else {
                # Sport-specific grader (handles NHL/Soccer step8 format)
                $NHLSoccerGrader = "$Root\nhl_soccer_grader.py"
                if (Test-Path $NHLSoccerGrader) {
                    Run-Py "Soccer Grade" $Root $NHLSoccerGrader @("--sport", "Soccer", "--date", $Date, "--slate", $SoccerSlate, "--actuals", $SoccerActuals, "--output-dir", $DateDir)
                    if (Test-Path $SoccerGraded) { Write-Host "  Soccer graded -> $SoccerGraded" -ForegroundColor Green }
                } else {
                    Write-Host "  WARNING: nhl_soccer_grader.py not found at $NHLSoccerGrader" -ForegroundColor Red
                    Write-Host "  Drop nhl_soccer_grader.py into $Root to enable Soccer grading." -ForegroundColor Yellow
                }
            }
        } else {
            Write-Host "  Skipping Soccer - actuals fetch failed" -ForegroundColor Yellow
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

$BuildGradeReport = "$Root\build_grade_report.py"

if (Test-Path $BuildGradeReport) {
    $htmlArgs = @("--date", $Date)
    if (Test-Path $NBAGraded)    { $htmlArgs += "--nba";    $htmlArgs += $NBAGraded }
    if (Test-Path $CBBGraded)    { $htmlArgs += "--cbb";    $htmlArgs += $CBBGraded }
    if (Test-Path $NHLGraded)    { $htmlArgs += "--nhl";    $htmlArgs += $NHLGraded }
    if (Test-Path $SoccerGraded) { $htmlArgs += "--soccer"; $htmlArgs += $SoccerGraded }

    if ($htmlArgs.Count -gt 2) {
        $htmlOut = "$Root\ui_runner\ui_runner\templates\slate_eval_$Date.html"
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

Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  SlateIQ Grader Run Complete" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""
