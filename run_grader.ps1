# ============================================================
#  PROP PIPELINE  -  Grader Script  [OPTIMIZED]
#
#  Usage:
#    .\run_grader.ps1                    # Grade yesterday (default)
#    .\run_grader.ps1 -Date 2026-02-26   # Grade specific date
#    .\run_grader.ps1 -NBAOnly           # NBA grading only
#    .\run_grader.ps1 -CBBOnly           # CBB grading only
# ============================================================
param(
    [string]$Date    = "",
    [switch]$NBAOnly,
    [switch]$CBBOnly
)

$ErrorActionPreference = "Continue"
$Root    = Split-Path -Parent $MyInvocation.MyCommand.Definition
$NBADir  = "$Root\NbaPropPipelineA"
$CBBDir  = "$Root\cbb2"
$OutRoot = "$Root\outputs"

if (-not $Date) { $Date = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd") }

$env:PYTHONUTF8 = "1"; $env:PYTHONIOENCODING = "utf-8"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
try { chcp 65001 | Out-Null } catch { }

if ((Test-Path "$Root\.venv\Scripts\Activate.ps1") -and (-not $env:VIRTUAL_ENV)) {
    & "$Root\.venv\Scripts\Activate.ps1"
}

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  SlateIQ GRADER  |  $Date  |  $(Get-Date -Format 'HH:mm:ss')" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

# -- Dated output folder ------------------------------------------------------
$DateDir = "$OutRoot\$Date"
if (-not (Test-Path $DateDir)) { New-Item -ItemType Directory -Force -Path $DateDir | Out-Null }

# -- Helper -------------------------------------------------------------------
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
    } catch {
        Write-Host "      EXCEPTION: $_" -ForegroundColor Red; return $false
    } finally { Pop-Location }
}

# -- Locate combined tickets file ---------------------------------------------
# Check outputs\{date}\ first, then root as fallback
$TicketsFile = "$DateDir\combined_slate_tickets_$Date.xlsx"
if (-not (Test-Path $TicketsFile)) { $TicketsFile = "$Root\combined_slate_tickets_$Date.xlsx" }

if (Test-Path $TicketsFile) {
    Write-Host "  Tickets file: $TicketsFile" -ForegroundColor DarkGray
} else {
    Write-Host "  WARNING: combined_slate_tickets_$Date.xlsx not found - ticket grading will be skipped" -ForegroundColor Yellow
    $TicketsFile = ""
}
Write-Host ""

# -- Pre-declare actuals paths at script scope (fixes ticket grader skip bug) --
$NBAActuals      = "$DateDir\actuals_nba_$Date.csv"
$CBBActuals      = "$DateDir\cbb_actuals_$Date.csv"
$CBBActualsLong  = "$DateDir\actuals_cbb_$Date.csv"
$NBAGraded       = "$DateDir\nba_graded_$Date.xlsx"
$CBBGraded       = "$DateDir\cbb_graded_$Date.xlsx"
$NBASlateExtracted = "$DateDir\nba_slate_extracted_$Date.xlsx"
$CBBSlateExtracted = "$DateDir\cbb_slate_extracted_$Date.csv"
$TicketGraded    = "$DateDir\combined_tickets_graded_$Date.xlsx"

# =============================================================================
#  NBA GRADING
# =============================================================================
if (-not $CBBOnly) {
    Write-Host "[ NBA GRADING ]" -ForegroundColor Magenta
    Write-Host ""

    if (-not $TicketsFile) {
        Write-Host "  Skipping NBA -- no tickets file found for $Date" -ForegroundColor Yellow
    } else {
        Run-Py "NBA Slate Extract" $Root ".\scripts\extract_nba_slate.py" @("--tickets", $TicketsFile, "--out", $NBASlateExtracted)

        if (Test-Path $NBASlateExtracted) {
            Write-Host "  Slate: $NBASlateExtracted" -ForegroundColor DarkGray
            $ok = Run-Py "NBA Fetch Actuals" $NBADir "$Root\fetch_actuals.py" @("--sport", "NBA", "--date", $Date, "--output", $NBAActuals)

            if ($ok -or (Test-Path $NBAActuals)) {
                Run-Py "NBA Grade" $Root ".\scripts\grading\slate_grader.py" @("--sport", "NBA", "--slate", $NBASlateExtracted, "--actuals", $NBAActuals, "--output", $NBAGraded, "--date", $Date)
                if (Test-Path $NBAGraded) { Write-Host "  NBA graded -> $NBAGraded" -ForegroundColor Green }
            } else {
                Write-Host "  Skipping NBA grade -- fetch failed" -ForegroundColor Yellow
            }
        } else {
            Write-Host "  Skipping NBA grade -- slate extraction failed" -ForegroundColor Yellow
        }
    }
    Write-Host ""
}

# =============================================================================
#  CBB GRADING
# =============================================================================
if (-not $NBAOnly) {
    Write-Host "[ CBB GRADING ]" -ForegroundColor Magenta
    Write-Host ""

    if (-not $TicketsFile) {
        Write-Host "  Skipping CBB -- no tickets file found for $Date" -ForegroundColor Yellow
    } else {
        Run-Py "CBB Slate Extract" $Root ".\scripts\extract_cbb_slate.py" @("--tickets", $TicketsFile, "--out", $CBBSlateExtracted)

        if (Test-Path $CBBSlateExtracted) {
            Write-Host "  Slate: $CBBSlateExtracted" -ForegroundColor DarkGray
            $ok = Run-Py "CBB Fetch Actuals" $CBBDir "fetch_cbb_actuals_by_date.py" @("--date", $Date, "--out", $CBBActuals)

            if ($ok -or (Test-Path $CBBActuals)) {
                Run-Py "CBB Grade" $Root "grade_cbb_full_slate.py" @("--slate", $CBBSlateExtracted, "--actuals", $CBBActuals, "--out", $CBBGraded, "--date", $Date)
                if (Test-Path $CBBGraded) { Write-Host "  CBB graded -> $CBBGraded" -ForegroundColor Green }
            } else {
                Write-Host "  Skipping CBB grade -- fetch failed" -ForegroundColor Yellow
            }
        } else {
            Write-Host "  Skipping CBB grade -- slate extraction failed" -ForegroundColor Yellow
        }
    }
    Write-Host ""
}

# =============================================================================
#  TICKET GRADING
# =============================================================================
# NOTE: Uses long-format actuals for both sports.
# NBA: actuals_nba_$Date.csv (long-format from fetch_actuals.py)
# CBB: actuals_cbb_$Date.csv (long-format fetched separately)
Write-Host "[ TICKET GRADING ]" -ForegroundColor Magenta
Write-Host ""

if (-not $TicketsFile) {
    Write-Host "  Skipping - no tickets file found for $Date" -ForegroundColor Yellow
} elseif (-not (Test-Path $NBAActuals)) {
    # This was the original bug: $NBAActuals was out of scope when -CBBOnly was passed.
    # Now pre-declared above, so this check works correctly in all switch combinations.
    Write-Host "  Skipping - NBA actuals missing at: $NBAActuals" -ForegroundColor Yellow
} else {
    # Fetch CBB long-format actuals if not already present
    if (-not (Test-Path $CBBActualsLong)) {
        Run-Py "CBB Fetch Actuals (long-format for ticket grader)" $NBADir "$Root\fetch_actuals.py" @("--sport", "CBB", "--date", $Date, "--output", $CBBActualsLong)
    } else {
        Write-Host "  CBB long-format actuals already exist, skipping fetch." -ForegroundColor DarkGray
    }

    if (Test-Path $CBBActualsLong) {
        Run-Py "Ticket Grade" $Root ".\scripts\combined_ticket_grader.py" @("--tickets", $TicketsFile, "--nba_actuals", $NBAActuals, "--cbb_actuals", $CBBActualsLong, "--out", $TicketGraded)
        if (Test-Path $TicketGraded) { Write-Host "  Tickets graded -> $TicketGraded" -ForegroundColor Green }
    } else {
        Write-Host "  Skipping ticket grader - CBB long-format actuals fetch failed" -ForegroundColor Yellow
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
} else {
    Write-Host "  No graded files found in $DateDir" -ForegroundColor Yellow
}
Write-Host ""

# =============================================================================
#  BUILD GRADES HTML
# =============================================================================
Write-Host "[ BUILDING GRADES HTML ]" -ForegroundColor Magenta
Write-Host ""

$htmlArgs = @("--date", $Date)
if (Test-Path $NBAGraded) { $htmlArgs += "--nba"; $htmlArgs += $NBAGraded }
if (Test-Path $CBBGraded) { $htmlArgs += "--cbb"; $htmlArgs += $CBBGraded }

if ($htmlArgs.Count -gt 2) {
    $htmlOut = "$Root\ui_runner\templates\slate_eval_$Date.html"
    $htmlArgs += "--out"; $htmlArgs += $htmlOut
    Run-Py "Grades HTML" $Root ".\build_grades_html.py" $htmlArgs
    if (Test-Path $htmlOut) { Write-Host "  HTML -> $htmlOut" -ForegroundColor Green }
} else {
    Write-Host "  No graded files found - skipping HTML build." -ForegroundColor Yellow
}
Write-Host ""

