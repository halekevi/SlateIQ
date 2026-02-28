# ============================================================
#  NBA-Pipelines  -  Folder Organizer
#  Run once to clean up root and archive old dated files
#  Safe: never deletes anything — only moves
# ============================================================

$Root = Split-Path -Parent $MyInvocation.MyCommand.Definition

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  FOLDER ORGANIZER  |  $(Get-Date -Format 'yyyy-MM-dd HH:mm')" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""

# ── Create clean folder structure ────────────────────────────────────────────

$Folders = @(
    "$Root\scripts",                  # All Python + PS pipeline scripts
    "$Root\scripts\grading",          # Grading-specific scripts
    "$Root\scripts\ui",               # UI/render scripts
    "$Root\archive",                  # Old dated outputs no longer needed in root
    "$Root\archive\outputs",          # Dated combined slate xlsx files
    "$Root\archive\graded",           # Old graded xlsx files
    "$Root\archive\dev",              # Progress notes, cheatsheets, old docs
    "$Root\ui_runner\components",     # JSX components (if not already there)
    "$Root\outputs"                   # Already exists — dated subfolders go here
)

foreach ($f in $Folders) {
    if (-not (Test-Path $f)) {
        New-Item -ItemType Directory -Force -Path $f | Out-Null
        Write-Host "  [+] Created: $f" -ForegroundColor Green
    }
}

Write-Host ""
Write-Host "[ MOVING PIPELINE SCRIPTS -> scripts\ ]" -ForegroundColor Magenta
Write-Host ""

# Core pipeline scripts -> scripts\
$PipelineScripts = @(
    "combined_slate_tickets.py",
    "combined_ticket_grader.py",
    "fetch_actuals.py",
    "extract_nba_slate.py",
    "extract_cbb_slate.py",
    "fetch_cbb_actuals_by_date.py"
)
foreach ($f in $PipelineScripts) {
    $src = "$Root\$f"
    $dst = "$Root\scripts\$f"
    if (Test-Path $src) {
        Move-Item $src $dst -Force
        Write-Host "  Moved: $f -> scripts\" -ForegroundColor DarkGray
    }
}

# Grading scripts -> scripts\grading\
$GradingScripts = @(
    "slate_grader.py",
    "grade_cbb_full_slate.py",
    "build_grades_html.py"
)
foreach ($f in $GradingScripts) {
    $src = "$Root\$f"
    $dst = "$Root\scripts\grading\$f"
    if (Test-Path $src) {
        Move-Item $src $dst -Force
        Write-Host "  Moved: $f -> scripts\grading\" -ForegroundColor DarkGray
    }
}

# UI/render scripts -> scripts\ui\
$UIScripts = @(
    "build_tickets_html.py",
    "render_combined_slate_latest.py"
)
foreach ($f in $UIScripts) {
    $src = "$Root\$f"
    $dst = "$Root\scripts\ui\$f"
    if (Test-Path $src) {
        Move-Item $src $dst -Force
        Write-Host "  Moved: $f -> scripts\ui\" -ForegroundColor DarkGray
    }
}

Write-Host ""
Write-Host "[ ARCHIVING OLD DATED OUTPUT FILES ]" -ForegroundColor Magenta
Write-Host ""

# Old dated combined slate xlsx (keep only today's in root)
$Today = Get-Date -Format "yyyy-MM-dd"
$OldSlates = Get-ChildItem "$Root\combined_slate_tickets_*.xlsx" -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -notmatch $Today }
foreach ($f in $OldSlates) {
    Move-Item $f.FullName "$Root\archive\outputs\$($f.Name)" -Force
    Write-Host "  Archived: $($f.Name) -> archive\outputs\" -ForegroundColor DarkGray
}

# Old graded xlsx files in root
$OldGraded = Get-ChildItem "$Root\*graded*.xlsx" "$Root\SlateIQ_*.xlsx" -ErrorAction SilentlyContinue
foreach ($f in $OldGraded) {
    Move-Item $f.FullName "$Root\archive\graded\$($f.Name)" -Force
    Write-Host "  Archived: $($f.Name) -> archive\graded\" -ForegroundColor DarkGray
}

# Today's combined slate -> outputs\today\ (canonical location going forward)
$TodaySlate = "$Root\combined_slate_tickets_$Today.xlsx"
$TodayOutDir = "$Root\outputs\$Today"
if ((Test-Path $TodaySlate) -and -not (Test-Path "$TodayOutDir\combined_slate_tickets_$Today.xlsx")) {
    if (-not (Test-Path $TodayOutDir)) { New-Item -ItemType Directory -Force -Path $TodayOutDir | Out-Null }
    Copy-Item $TodaySlate "$TodayOutDir\combined_slate_tickets_$Today.xlsx" -Force
    Write-Host "  Copied today's slate -> outputs\$Today\" -ForegroundColor Green
}

Write-Host ""
Write-Host "[ ARCHIVING DEV / MISC FILES ]" -ForegroundColor Magenta
Write-Host ""

# Dev notes, progress files, cheatsheets
$DevFiles = @(
    "powershell_cheatsheet.txt",
    "prizepicks_payout_engine_progress.xlsx",
    "prizepicks_payout_engine_progress.md",
    "GITHUB_DEPLOY.md"
)
foreach ($f in $DevFiles) {
    $src = "$Root\$f"
    if (Test-Path $src) {
        Move-Item $src "$Root\archive\dev\$f" -Force
        Write-Host "  Archived: $f -> archive\dev\" -ForegroundColor DarkGray
    }
}

Write-Host ""
Write-Host "[ MOVING JSX COMPONENTS -> ui_runner\components\ ]" -ForegroundColor Magenta
Write-Host ""

# JSX components -> ui_runner\components\
$JSXFiles = @(
    "payout_calculator.jsx",
    "payout_calculator_render.jsx",
    "pipeline_dashboard.jsx"
)
foreach ($f in $JSXFiles) {
    $src = "$Root\$f"
    $dst = "$Root\ui_runner\components\$f"
    if (Test-Path $src) {
        Move-Item $src $dst -Force
        Write-Host "  Moved: $f -> ui_runner\components\" -ForegroundColor DarkGray
    }
}

# tickets_latest files -> ui_runner\docs\ (where they belong)
foreach ($f in @("tickets_latest.html", "tickets_latest.json")) {
    $src = "$Root\$f"
    $dst = "$Root\ui_runner\docs\$f"
    if ((Test-Path $src) -and -not (Test-Path $dst)) {
        Move-Item $src $dst -Force
        Write-Host "  Moved: $f -> ui_runner\docs\" -ForegroundColor DarkGray
    } elseif (Test-Path $src) {
        Remove-Item $src -Force  # duplicate — ui_runner\docs already has it
        Write-Host "  Removed duplicate: $f (already in ui_runner\docs\)" -ForegroundColor DarkGray
    }
}

Write-Host ""
Write-Host "[ UPDATING run_pipeline.ps1 SCRIPT PATHS ]" -ForegroundColor Magenta
Write-Host ""

# The pipeline scripts moved to scripts\ — update run_pipeline.ps1 references
$pipelinePs1 = "$Root\run_pipeline.ps1"
if (Test-Path $pipelinePs1) {
    $content = Get-Content $pipelinePs1 -Raw

    # Update combined_slate_tickets.py reference
    $content = $content -replace '"\.\\combined_slate_tickets\.py"', '".\scripts\combined_slate_tickets.py"'
    $content = $content -replace "'\.\\combined_slate_tickets\.py'", "'.\scripts\combined_slate_tickets.py'"
    $content = $content -replace '\.\s*\\combined_slate_tickets\.py', '.\scripts\combined_slate_tickets.py'

    Set-Content $pipelinePs1 -Value $content -Encoding UTF8
    Write-Host "  Updated run_pipeline.ps1 script paths" -ForegroundColor Green
}

# Update run_grader.ps1 script paths
$graderPs1 = "$Root\run_grader.ps1"
if (Test-Path $graderPs1) {
    $content = Get-Content $graderPs1 -Raw

    $pathMap = @{
        '".\extract_nba_slate.py"'      = '".\scripts\extract_nba_slate.py"'
        '".\extract_cbb_slate.py"'      = '".\scripts\extract_cbb_slate.py"'
        '".\fetch_actuals.py"'          = '".\scripts\fetch_actuals.py"'
        '".\slate_grader.py"'           = '".\scripts\grading\slate_grader.py"'
        '".\combined_ticket_grader.py"' = '".\scripts\combined_ticket_grader.py"'
        '".\grade_cbb_full_slate.py"'   = '".\scripts\grading\grade_cbb_full_slate.py"'
        '".\build_grades_html.py"'      = '".\scripts\grading\build_grades_html.py"'
        '"fetch_cbb_actuals_by_date.py"' = '"$Root\scripts\fetch_cbb_actuals_by_date.py"'
    }
    foreach ($old in $pathMap.Keys) {
        $content = $content -replace [regex]::Escape($old), $pathMap[$old]
    }

    Set-Content $graderPs1 -Value $content -Encoding UTF8
    Write-Host "  Updated run_grader.ps1 script paths" -ForegroundColor Green
}

Write-Host ""
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host "  DONE. Final structure:" -ForegroundColor Cyan
Write-Host "======================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  NBA-Pipelines\" -ForegroundColor White
Write-Host "  ├── run_pipeline.ps1          (master runner)" -ForegroundColor Green
Write-Host "  ├── run_grader.ps1            (grader runner)" -ForegroundColor Green
Write-Host "  ├── combined_slate_tickets_TODAY.xlsx" -ForegroundColor Green
Write-Host "  ├── scripts\                  (all Python pipeline scripts)" -ForegroundColor Yellow
Write-Host "  │   ├── grading\              (slate_grader, grade_cbb, build_grades_html)" -ForegroundColor Yellow
Write-Host "  │   └── ui\                   (build_tickets_html, render_combined_slate)" -ForegroundColor Yellow
Write-Host "  ├── NbaPropPipelineA\         (NBA step scripts)" -ForegroundColor Cyan
Write-Host "  ├── CBB2\                     (CBB step scripts)" -ForegroundColor Cyan
Write-Host "  ├── outputs\{date}\           (dated combined slates + graded files)" -ForegroundColor Cyan
Write-Host "  ├── ui_runner\                (web UI)" -ForegroundColor Cyan
Write-Host "  │   ├── components\           (JSX files)" -ForegroundColor Cyan
Write-Host "  │   └── docs\                 (tickets_latest HTML/JSON)" -ForegroundColor Cyan
Write-Host "  └── archive\                  (old dated files, dev notes)" -ForegroundColor DarkGray
Write-Host "      ├── outputs\              (old combined slates)" -ForegroundColor DarkGray
Write-Host "      ├── graded\               (old graded xlsx)" -ForegroundColor DarkGray
Write-Host "      └── dev\                  (cheatsheets, progress notes)" -ForegroundColor DarkGray
Write-Host ""
