# organize_slateiq_root.ps1
# Reorganizes SlateIQ root folder into clean structure

param(
    [string]$SlateIQRoot = "C:\Users\halek\OneDrive\Desktop\Vision Board\SlateIQ\SlateIQ"
)

Write-Host ""
Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host "  SlateIQ Root Folder Organizer" -ForegroundColor Cyan
Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Target: $SlateIQRoot" -ForegroundColor Yellow
Write-Host ""

if (-not (Test-Path $SlateIQRoot)) {
    Write-Host "ERROR: SlateIQ root folder not found!" -ForegroundColor Red
    exit 1
}

# Create folder structure
Write-Host "Creating folder structure..." -ForegroundColor Green

$folders = @(
    "scripts",
    "data\cache",
    "data\inputs",
    "data\outputs",
    "docs",
    "ui_runner\templates",
    "config",
    "archive\old_scripts",
    "archive\old_outputs",
    "archive\old_docs"
)

foreach ($folder in $folders) {
    $path = Join-Path $SlateIQRoot $folder
    if (-not (Test-Path $path)) {
        New-Item -ItemType Directory -Force -Path $path | Out-Null
        Write-Host "  ✓ Created: $folder" -ForegroundColor Green
    }
}

Write-Host ""
Write-Host "Moving files..." -ForegroundColor Green
Write-Host ""

# Move root-level PowerShell scripts to scripts/
Write-Host "  → Pipeline scripts..." -ForegroundColor Yellow
$scripts = @(
    "run_pipeline.ps1",
    "run_grader.ps1",
    "run_wnba_pipeline.ps1",
    "run_cbb_pipeline.ps1",
    "run_mlb_pipeline.ps1",
    "cleanup_pipeline.ps1",
    "organize_folder.ps1",
    "organize_root.ps1",
    "rename_to_slateiq.ps1",
    "Register_Daily_Task.ps1",
    "daily_grades.ps1",
    "TEST_DATE_PARSING.ps1"
)

foreach ($script in $scripts) {
    $path = Join-Path $SlateIQRoot $script
    if (Test-Path $path) {
        Move-Item -Path $path -Destination (Join-Path $SlateIQRoot "scripts") -Force
        Write-Host "    ✓ $script" -ForegroundColor Gray
    }
}

# Move Python build/utility scripts to scripts/
Write-Host "  → Python utility scripts..." -ForegroundColor Yellow
$pyScripts = @(
    "combined_slate_tickets.py",
    "combined_ticket_grader.py",
    "fetch_actuals.py",
    "extract_nba_slate.py",
    "extract_cbb_slate.py",
    "fetch_cbb_actuals_by_date.py",
    "build_ticket_eval_html.py",
    "build_grade_report.py",
    "check_graded.py",
    "debug_soccer_actuals.py",
    "nhl_soccer_grader.py",
    "patch_step1_*.py"
)

foreach ($pattern in $pyScripts) {
    Get-ChildItem -Path $SlateIQRoot -Filter $pattern -File -ErrorAction SilentlyContinue | 
    ForEach-Object {
        Move-Item -Path $_.FullName -Destination (Join-Path $SlateIQRoot "scripts") -Force
        Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
    }
}

# Move documentation to docs/
Write-Host "  → Documentation..." -ForegroundColor Yellow
$docs = @(
    "*.md",
    "*.txt"
)

foreach ($pattern in $docs) {
    Get-ChildItem -Path $SlateIQRoot -Filter $pattern -File -MaxDepth 1 -ErrorAction SilentlyContinue | 
    Where-Object { $_.Name -notlike ".git*" } |
    ForEach-Object {
        Move-Item -Path $_.FullName -Destination (Join-Path $SlateIQRoot "docs") -Force
        Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
    }
}

# Move template files to ui_runner/templates/
Write-Host "  → UI templates..." -ForegroundColor Yellow
if (Test-Path "$SlateIQRoot\ui_runner\templates") {
    Get-ChildItem -Path "$SlateIQRoot\ui_runner" -Filter "*.html" -File -ErrorAction SilentlyContinue | 
    ForEach-Object {
        Move-Item -Path $_.FullName -Destination "$SlateIQRoot\ui_runner\templates" -Force
        Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
    }
}

# Move CSV cache/reference files to data/cache/
Write-Host "  → Cache files..." -ForegroundColor Yellow
$cacheFiles = @(
    "*_cache.csv",
    "*_map.csv",
    "nba_espn_id_map.csv",
    "defense_team_summary.csv"
)

foreach ($pattern in $cacheFiles) {
    Get-ChildItem -Path $SlateIQRoot -Filter $pattern -File -MaxDepth 1 -ErrorAction SilentlyContinue | 
    ForEach-Object {
        Move-Item -Path $_.FullName -Destination (Join-Path $SlateIQRoot "data\cache") -Force
        Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
    }
}

# Move output files to archive (if not in subdirectories)
Write-Host "  → Archive old outputs..." -ForegroundColor Yellow
Get-ChildItem -Path $SlateIQRoot -Filter "combined_*.xlsx" -File -MaxDepth 1 -ErrorAction SilentlyContinue | 
ForEach-Object {
    Move-Item -Path $_.FullName -Destination (Join-Path $SlateIQRoot "archive\old_outputs") -Force
    Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
}

Get-ChildItem -Path $SlateIQRoot -Filter "best_*.xlsx" -File -MaxDepth 1 -ErrorAction SilentlyContinue | 
ForEach-Object {
    Move-Item -Path $_.FullName -Destination (Join-Path $SlateIQRoot "archive\old_outputs") -Force
    Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
}

# Archive old patches and fixes
Write-Host "  → Archive old patches..." -ForegroundColor Yellow
Get-ChildItem -Path $SlateIQRoot -Filter "patch_*.py" -File -MaxDepth 1 -ErrorAction SilentlyContinue | 
ForEach-Object {
    Move-Item -Path $_.FullName -Destination (Join-Path $SlateIQRoot "archive\old_scripts") -Force
    Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
}

# Move .git files appropriately
Write-Host "  → Git files..." -ForegroundColor Yellow
if (Test-Path "$SlateIQRoot\.gitignore") {
    Move-Item -Path "$SlateIQRoot\.gitignore" -Destination (Join-Path $SlateIQRoot "docs") -Force
    Write-Host "    ✓ .gitignore" -ForegroundColor Gray
}

if (Test-Path "$SlateIQRoot\.gitattributes") {
    Move-Item -Path "$SlateIQRoot\.gitattributes" -Destination (Join-Path $SlateIQRoot "docs") -Force
    Write-Host "    ✓ .gitattributes" -ForegroundColor Gray
}

Write-Host ""
Write-Host "Creating README..." -ForegroundColor Green

$readme = @"
# SlateIQ - Organized Structure

## 📁 Folder Layout

```
SlateIQ/
├── scripts/              # All pipeline & utility scripts
│   ├── run_pipeline.ps1
│   ├── run_grader.ps1
│   ├── run_wnba_pipeline.ps1
│   ├── run_cbb_pipeline.ps1
│   ├── run_mlb_pipeline.ps1
│   ├── combined_slate_tickets.py
│   ├── combined_ticket_grader.py
│   ├── build_ticket_eval_html.py
│   └── ...
│
├── data/
│   ├── cache/            # ESPN, Vegas, player mappings
│   │   ├── *_cache.csv
│   │   ├── *_map.csv
│   │   └── defense_team_summary.csv
│   │
│   ├── inputs/           # Source data (actuals, raw props)
│   │   ├── actuals_*.csv
│   │   └── *_props_today.csv
│   │
│   └── outputs/          # Daily pipeline outputs
│       ├── combined_slate_tickets_2026-03-08.xlsx
│       ├── combined_tickets_graded_2026-03-08.xlsx
│       └── ...
│
├── ui_runner/            # Web UI for slate viewer
│   ├── templates/        # HTML templates
│   └── components/       # JSX/React components
│
├── docs/                 # Documentation
│   ├── README.md
│   ├── GUIDES.md
│   ├── .gitignore
│   └── *.md
│
├── config/               # Configuration files
│   └── settings.json (future)
│
├── NBA/                  # NBA pipeline (organized)
│   ├── scripts/
│   ├── data/cache/
│   ├── data/inputs/
│   ├── data/outputs/
│   └── ...
│
├── CBB/                  # College Basketball pipeline
├── NHL/                  # Hockey pipeline
├── Soccer/               # Soccer pipeline
├── MLB/                  # Baseball pipeline (if available)
├── WNBA/                 # WNBA pipeline (if available)
│
├── grader/               # Grading utility folder
│
├── outputs/              # Consolidated daily outputs (symlink possible)
│
└── archive/              # Old runs, backups
    ├── old_scripts/
    ├── old_outputs/
    └── old_docs/
```

## 🚀 Quick Start

```powershell
cd "C:\Users\halek\OneDrive\Desktop\Vision Board\SlateIQ\SlateIQ"

# Run full pipeline
.\scripts\run_pipeline.ps1 -Date 2026-03-09

# Run grader
.\scripts\run_grader.ps1 -Date 2026-03-08

# View combined slate
.\scripts\run_pipeline.ps1 -Date 2026-03-09 | Open data\outputs\combined_slate_tickets_2026-03-09.xlsx
```

## 📊 Sports Pipelines

Each sport has its own organized structure:
- **NBA/** - Basketball (primary)
- **CBB/** - College Basketball
- **NHL/** - Hockey
- **Soccer/** - Soccer/Football
- **MLB/** - Baseball (if enabled)
- **WNBA/** - Women's Basketball (if enabled)

Each follows the same pattern:
```
Sport/
├── scripts/         # step1, step2, ... scripts
├── data/cache/      # Sport-specific cache
├── data/inputs/     # Raw props
└── data/outputs/    # Pipeline outputs
```

## 🔑 Critical Files (DO NOT DELETE)

```
data/cache/nba_espn_boxscore_cache.csv
data/cache/nba_to_espn_id_map.csv
data/cache/defense_team_summary.csv
NBA/data/cache/nba_espn_boxscore_cache.csv
```

## ✨ New Features

- **NBA H2H Matchups (Step 6d)** - Shows last game vs opponent stats
- **Multi-sport support** - NBA, CBB, NHL, Soccer, MLB, WNBA
- **Organized by function** - Scripts, data, UI, docs all in their places
- **Archive structure** - Old runs preserved but out of the way

## 📌 Notes

- All intermediate CSV files can be regenerated
- Cache files should be backed up periodically
- Use `run_pipeline.ps1 -RefreshCache` to rebuild ESPN cache
- Daily tasks can auto-run via `Register_Daily_Task.ps1`

---

**Last Updated:** $(Get-Date -Format 'yyyy-MM-dd')
**Version:** 1.0 Organized
"@

$readme | Out-File -FilePath "$SlateIQRoot\SLATEIQ_README.md" -Encoding UTF8
Write-Host "  ✓ Created: SLATEIQ_README.md" -ForegroundColor Green

Write-Host ""
Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host "✅ Organization Complete!" -ForegroundColor Green
Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Root-level folders:" -ForegroundColor Yellow
Get-ChildItem -Path $SlateIQRoot -Directory -ErrorAction SilentlyContinue | 
Select-Object Name | Format-Table -AutoSize

Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Run: .\scripts\run_pipeline.ps1 -Date 2026-03-09" -ForegroundColor Gray
Write-Host "  2. Run: .\scripts\run_grader.ps1 -Date 2026-03-08" -ForegroundColor Gray
Write-Host "  3. See SLATEIQ_README.md for details" -ForegroundColor Gray
Write-Host ""
