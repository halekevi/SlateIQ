param(
    [switch]$SkipNBA,
    [switch]$SkipCBB,
    [switch]$NBAOnly,
    [switch]$CBBOnly,
    [string]$Date = ""   # override date (YYYY-MM-DD). Default = yesterday.
)

$ErrorActionPreference = "Stop"
$StartTime  = Get-Date
$Timestamp  = Get-Date -Format "yyyy-MM-dd HH:mm:ss"

# ── Date resolution ───────────────────────────────────────────────────────────
if ($Date -eq "") {
    $GradeDate = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd")
} else {
    $GradeDate = $Date
}

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  PROP PIPELINE GRADER  |  $Timestamp" -ForegroundColor Cyan
Write-Host "  Grading slate for: $GradeDate" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

# ── Paths ─────────────────────────────────────────────────────────────────────
$Root        = Split-Path -Parent $MyInvocation.MyCommand.Path
$NBADir      = Join-Path $Root "NbaPropPipelineA"
$CBBDir      = Join-Path $Root "CBB2"
$OutDir      = Join-Path $Root "outputs\$GradeDate"
$GradeDir    = Join-Path $Root "grades"

# NBA slate = step8 output saved from that day's run
$NBASlate    = Join-Path $OutDir "nba_ranked_$GradeDate.xlsx"
$CBBSlate    = Join-Path $OutDir "cbb_ranked_$GradeDate.xlsx"

# Actuals CSVs (written into grade folder)
$NBAActs     = Join-Path $GradeDir "actuals_nba_$GradeDate.csv"
$CBBActs     = Join-Path $GradeDir "actuals_cbb_$GradeDate.csv"

# Graded outputs
$NBAGraded   = Join-Path $OutDir "nba_graded_$GradeDate.xlsx"
$CBBGraded   = Join-Path $OutDir "cbb_graded_$GradeDate.xlsx"

# Create grade dir if needed
if (-not (Test-Path $GradeDir))  { New-Item -ItemType Directory -Path $GradeDir  | Out-Null }
if (-not (Test-Path $OutDir))    { New-Item -ItemType Directory -Path $OutDir    | Out-Null }

Write-Host "  Slate folder : $OutDir" -ForegroundColor DarkGray
Write-Host "  Actuals      : $GradeDir" -ForegroundColor DarkGray
Write-Host ""

# ── Helper ────────────────────────────────────────────────────────────────────
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

# ── NBA Grading ───────────────────────────────────────────────────────────────
$RunNBA = (-not $SkipNBA) -and (-not $CBBOnly)
if ($RunNBA) {
    Write-Host "[ NBA - Grading $GradeDate ]" -ForegroundColor Magenta
    Write-Host ""

    # Check slate exists
    if (-not (Test-Path $NBASlate)) {
        Write-Host "  WARNING: NBA slate not found at $NBASlate" -ForegroundColor Red
        Write-Host "  Was the pipeline run on $GradeDate? Skipping NBA grade." -ForegroundColor Red
    } else {
        try {
            # Step 1: Fetch actuals from ESPN
            Run-Step "Fetch NBA Actuals" $Root ".\fetch_actuals.py --sport NBA --date $GradeDate --output `"$NBAActs`""

            # Step 2: Grade the slate
            Run-Step "Grade NBA Slate" $Root ".\slate_grader.py --sport NBA --slate `"$NBASlate`" --actuals `"$NBAActs`" --output `"$NBAGraded`" --date $GradeDate"

            Write-Host ""
            Write-Host "  NBA graded -> $NBAGraded" -ForegroundColor Green
        } catch {
            Write-Host "  NBA GRADE FAILED: $_" -ForegroundColor Red
            Write-Host "  Continuing to CBB..." -ForegroundColor Yellow
        }
    }
    Write-Host ""
}

# ── CBB Grading ───────────────────────────────────────────────────────────────
$RunCBB = (-not $SkipCBB) -and (-not $NBAOnly)
if ($RunCBB) {
    Write-Host "[ CBB - Grading $GradeDate ]" -ForegroundColor Magenta
    Write-Host ""

    # Check slate exists
    if (-not (Test-Path $CBBSlate)) {
        Write-Host "  WARNING: CBB slate not found at $CBBSlate" -ForegroundColor Red
        Write-Host "  Was the pipeline run on $GradeDate? Skipping CBB grade." -ForegroundColor Red
    } else {
        try {
            # Step 1: Fetch actuals from ESPN
            Run-Step "Fetch CBB Actuals" $Root ".\fetch_actuals.py --sport CBB --date $GradeDate --output `"$CBBActs`""

            # Step 2: Grade the slate
            Run-Step "Grade CBB Slate" $Root ".\slate_grader.py --sport CBB --slate `"$CBBSlate`" --actuals `"$CBBActs`" --output `"$CBBGraded`" --date $GradeDate"

            Write-Host ""
            Write-Host "  CBB graded -> $CBBGraded" -ForegroundColor Green
        } catch {
            Write-Host "  CBB GRADE FAILED: $_" -ForegroundColor Red
        }
    }
    Write-Host ""
}

# ── Summary ───────────────────────────────────────────────────────────────────
$Elapsed = (Get-Date) - $StartTime
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  DONE  |  $($Elapsed.ToString('mm\:ss'))  |  $OutDir" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""
