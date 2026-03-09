# organize_nba_folder.ps1
# Reorganizes NBA folder into clean structure with scripts, data, docs, and archive

param(
    [string]$NBARoot = "C:\Users\halek\OneDrive\Desktop\Vision Board\SlateIQ\SlateIQ\NBA"
)

Write-Host ""
Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host "  NBA Folder Organizer" -ForegroundColor Cyan
Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Target: $NBARoot" -ForegroundColor Yellow
Write-Host ""

if (-not (Test-Path $NBARoot)) {
    Write-Host "ERROR: NBA folder not found!" -ForegroundColor Red
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
    "archive"
)

foreach ($folder in $folders) {
    $path = Join-Path $NBARoot $folder
    if (-not (Test-Path $path)) {
        New-Item -ItemType Directory -Force -Path $path | Out-Null
        Write-Host "  ✓ Created: $folder" -ForegroundColor Green
    }
}

Write-Host ""
Write-Host "Moving files..." -ForegroundColor Green
Write-Host ""

# Move Python scripts to scripts/
Write-Host "  → Scripts..." -ForegroundColor Yellow
$scripts = @(
    "step*.py",
    "*_grader.py",
    "nba_grader.py",
    "defense_report.py",
    "fix_*.py"
)

foreach ($pattern in $scripts) {
    Get-ChildItem -Path $NBARoot -Filter $pattern -File -ErrorAction SilentlyContinue | 
    ForEach-Object {
        Move-Item -Path $_.FullName -Destination (Join-Path $NBARoot "scripts") -Force
        Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
    }
}

# Move documentation to docs/
Write-Host "  → Documentation..." -ForegroundColor Yellow
Get-ChildItem -Path $NBARoot -Filter "*.md" -File -ErrorAction SilentlyContinue | 
ForEach-Object {
    Move-Item -Path $_.FullName -Destination (Join-Path $NBARoot "docs") -Force
    Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
}

# Move cache files to data/cache/
Write-Host "  → Cache files..." -ForegroundColor Yellow
Get-ChildItem -Path $NBARoot -Filter "*_cache.csv" -File -ErrorAction SilentlyContinue | 
ForEach-Object {
    Move-Item -Path $_.FullName -Destination (Join-Path $NBARoot "data\cache") -Force
    Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
}

Get-ChildItem -Path $NBARoot -Filter "*_map.csv" -File -ErrorAction SilentlyContinue | 
ForEach-Object {
    Move-Item -Path $_.FullName -Destination (Join-Path $NBARoot "data\cache") -Force
    Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
}

Get-ChildItem -Path $NBARoot -Filter "defense_team_summary.csv" -File -ErrorAction SilentlyContinue | 
ForEach-Object {
    Move-Item -Path $_.FullName -Destination (Join-Path $NBARoot "data\cache") -Force
    Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
}

# Move step*.csv files to data/outputs/
Write-Host "  → Step CSV files..." -ForegroundColor Yellow
Get-ChildItem -Path $NBARoot -Filter "step*.csv" -File -ErrorAction SilentlyContinue | 
ForEach-Object {
    Move-Item -Path $_.FullName -Destination (Join-Path $NBARoot "data\outputs") -Force
    Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
}

# Move XLSX files to data/outputs/
Write-Host "  → Excel files..." -ForegroundColor Yellow
Get-ChildItem -Path $NBARoot -Filter "*.xlsx" -File -ErrorAction SilentlyContinue | 
ForEach-Object {
    Move-Item -Path $_.FullName -Destination (Join-Path $NBARoot "data\outputs") -Force
    Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
}

# Move actuals and debug files to data/inputs/
Write-Host "  → Input/debug files..." -ForegroundColor Yellow
Get-ChildItem -Path $NBARoot -Filter "actuals*.csv" -File -ErrorAction SilentlyContinue | 
ForEach-Object {
    Move-Item -Path $_.FullName -Destination (Join-Path $NBARoot "data\inputs") -Force
    Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
}

Get-ChildItem -Path $NBARoot -Filter "*debug*.csv" -File -ErrorAction SilentlyContinue | 
ForEach-Object {
    Move-Item -Path $_.FullName -Destination (Join-Path $NBARoot "data\inputs") -Force
    Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
}

Get-ChildItem -Path $NBARoot -Filter "step1_pp_props_today.csv" -File -ErrorAction SilentlyContinue | 
ForEach-Object {
    Move-Item -Path $_.FullName -Destination (Join-Path $NBARoot "data\inputs") -Force
    Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
}

# Move old stuff to archive/
Write-Host "  → Archive old files..." -ForegroundColor Yellow
Get-ChildItem -Path $NBARoot -Filter "*.bak" -File -ErrorAction SilentlyContinue | 
ForEach-Object {
    Move-Item -Path $_.FullName -Destination (Join-Path $NBARoot "archive") -Force
    Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
}

Get-ChildItem -Path $NBARoot -Filter "RUN_COMPLETE.flag" -File -ErrorAction SilentlyContinue | 
ForEach-Object {
    Move-Item -Path $_.FullName -Destination (Join-Path $NBARoot "archive") -Force
    Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
}

Get-ChildItem -Path $NBARoot -Filter "organize.sh" -File -ErrorAction SilentlyContinue | 
ForEach-Object {
    Move-Item -Path $_.FullName -Destination (Join-Path $NBARoot "archive") -Force
    Write-Host "    ✓ $($_.Name)" -ForegroundColor Gray
}

# Move id_map folder to archive
if (Test-Path "$NBARoot\id_map") {
    Move-Item -Path "$NBARoot\id_map" -Destination (Join-Path $NBARoot "archive") -Force
    Write-Host "    ✓ id_map/ folder" -ForegroundColor Gray
}

# Move old archive folders if they exist
if (Test-Path "$NBARoot\archive\old_runs") {
    Write-Host "    ✓ archive/old_runs/ (already organized)" -ForegroundColor Gray
}

Write-Host ""
Write-Host "Creating README..." -ForegroundColor Green

$readme = @"
# SlateIQ NBA Pipeline - Organized Structure

## 📁 Folder Layout

``````
NBA/
├── scripts/              # All Python pipeline scripts
│   ├── step1_fetch_prizepicks_api.py
│   ├── step2_attach_picktypes.py
│   ├── step3_attach_defense.py
│   ├── step4_attach_player_stats_espn_cache.py
│   ├── step5_add_line_hit_rates.py
│   ├── step6_team_role_context.py
│   ├── step6a_attach_opponent_stats_NBA.py
│   ├── step6b_attach_game_context.py
│   ├── step6c_schedule_flags.py
│   ├── step6d_attach_h2h_matchups.py  ← NEW: Head-to-head last game stats
│   ├── step7_rank_props.py
│   ├── step8_add_direction_context.py
│   ├── step9_build_tickets.py
│   ├── nba_grader.py
│   ├── defense_report.py
│   └── fix_step4_stats.py
│
├── data/
│   ├── cache/            # ESPN cache & reference data
│   │   ├── nba_espn_boxscore_cache.csv (CRITICAL)
│   │   ├── nba_to_espn_id_map.csv      (CRITICAL)
│   │   └── defense_team_summary.csv
│   │
│   ├── inputs/           # Source data
│   │   ├── actuals_nba_*.csv
│   │   ├── step1_pp_props_today.csv
│   │   └── *debug*.csv
│   │
│   └── outputs/          # Pipeline outputs
│       ├── step2_with_picktypes.csv
│       ├── step3_with_defense.csv
│       ├── ...
│       ├── step8_all_direction.csv ← FINAL SLATE
│       ├── step8_all_direction_clean.xlsx
│       └── best_tickets.xlsx
│
├── docs/                 # Documentation
│   ├── README.md
│   ├── SOLUTION_SUMMARY.md
│   └── *.md
│
└── archive/              # Old runs, backups
    ├── old_runs/
    ├── old_csv/
    └── ...
``````

## 🚀 Quick Start

From SlateIQ root:

```powershell
cd NBA

# Full pipeline
..\run_pipeline.ps1 -Date 2026-03-09

# Or run individual steps
py -3.14 scripts\step2_attach_picktypes.py --input data\inputs\step1_pp_props_today.csv --output data\outputs\step2_with_picktypes.csv
```

## 📊 Pipeline Steps

| Step | Script | Purpose |
|------|--------|---------|
| 1 | fetch_prizepicks_api.py | Fetch daily props |
| 2 | attach_picktypes.py | Add pick types |
| 3 | attach_defense.py | Add opponent defense |
| 4 | attach_player_stats_espn_cache.py | Add player stats |
| 5 | add_line_hit_rates.py | Add hit rates |
| 6 | team_role_context.py | Add player roles |
| 6a | attach_opponent_stats_NBA.py | Add opponent context |
| 6b | attach_game_context.py | Add Vegas lines |
| 6c | schedule_flags.py | Add B2B flags |
| **6d** | **attach_h2h_matchups.py** | **NEW: Last game vs opponent** |
| 7 | rank_props.py | Rank & tier |
| 8 | add_direction_context.py | Final direction |
| 9 | build_tickets.py | Generate tickets |

## 🔑 Critical Files (DO NOT DELETE)

```
data/cache/nba_espn_boxscore_cache.csv     ← ESPN player stats
data/cache/nba_to_espn_id_map.csv          ← Player name mapping
```

## ✨ New: H2H Matchups (Step 6d)

Finds each player's **last actual game vs the opponent team** and pulls that stat value.

- Fill rate: ~10% (only players with prior history)
- Columns: `h2h_last_stat`, `h2h_last_date`, `h2h_games_vs_opp`

**Last Updated:** $(Get-Date -Format 'yyyy-MM-dd')
"@

$readme | Out-File -FilePath "$NBARoot\NBA_PIPELINE_README.md" -Encoding UTF8
Write-Host "  ✓ Created: NBA_PIPELINE_README.md" -ForegroundColor Green

Write-Host ""
Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host "✅ Organization Complete!" -ForegroundColor Green
Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "Folder structure:" -ForegroundColor Yellow
Get-ChildItem -Path $NBARoot -Directory | Select-Object Name | Format-Table -AutoSize

Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Update run_pipeline.ps1 to use new paths if needed" -ForegroundColor Gray
Write-Host "  2. Test pipeline: cd NBA && ..\run_pipeline.ps1" -ForegroundColor Gray
Write-Host "  3. See NBA_PIPELINE_README.md for details" -ForegroundColor Gray
Write-Host ""
