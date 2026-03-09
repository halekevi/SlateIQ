# ============================================================
#  SlateIQ — NHL Folder Reorganization
#  Run from: SlateIQ root (where the NHL\ folder lives)
# ============================================================

$nhl = "NHL"

Write-Host ""
Write-Host "SlateIQ — NHL Reorganization" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor DarkGray

# ── 1. Create subdirectories ─────────────────────────────────
Write-Host "`n[1/5] Creating subdirectories..." -ForegroundColor Yellow
New-Item -ItemType Directory -Force -Path "$nhl\scripts" | Out-Null
New-Item -ItemType Directory -Force -Path "$nhl\cache"   | Out-Null
New-Item -ItemType Directory -Force -Path "$nhl\outputs" | Out-Null
Write-Host "      scripts\  cache\  outputs\  created." -ForegroundColor DarkGray

# ── 2. Move pipeline scripts ─────────────────────────────────
Write-Host "`n[2/5] Moving scripts..." -ForegroundColor Yellow
Move-Item -Force "$nhl\step*.py"                "$nhl\scripts\" 
Move-Item -Force "$nhl\nhl_defense_report.py"   "$nhl\scripts\"
Write-Host "      step1–step9 + nhl_defense_report.py  →  scripts\" -ForegroundColor DarkGray

# ── 3. Move cache / reference files ──────────────────────────
Write-Host "`n[3/5] Moving cache files..." -ForegroundColor Yellow
Move-Item -Force "$nhl\nhl_id_cache.csv"          "$nhl\cache\"
Move-Item -Force "$nhl\nhl_stats_cache.csv"        "$nhl\cache\"
Move-Item -Force "$nhl\nhl_gamelog_cache.json"     "$nhl\cache\"
Move-Item -Force "$nhl\nhl_defense_summary.csv"    "$nhl\cache\"
Write-Host "      nhl_id_cache, nhl_stats_cache, nhl_gamelog_cache, nhl_defense_summary  →  cache\" -ForegroundColor DarkGray

# ── 4. Move intermediate outputs and final XLSXs ─────────────
Write-Host "`n[4/5] Moving outputs..." -ForegroundColor Yellow
Move-Item -Force "$nhl\step*.csv"   "$nhl\outputs\"
Move-Item -Force "$nhl\step*.xlsx"  "$nhl\outputs\"
Write-Host "      step1–step9 CSVs + XLSXs  →  outputs\" -ForegroundColor DarkGray

# ── 5. Remove __pycache__ ────────────────────────────────────
Write-Host "`n[5/5] Cleaning up __pycache__..." -ForegroundColor Yellow
Remove-Item -Recurse -Force "$nhl\__pycache__" -ErrorAction SilentlyContinue
Write-Host "      __pycache__ removed." -ForegroundColor DarkGray

# ── Done ─────────────────────────────────────────────────────
Write-Host ""
Write-Host "============================================================" -ForegroundColor DarkGray
Write-Host "NHL reorganization complete." -ForegroundColor Green
Write-Host ""
Write-Host "Next steps:" -ForegroundColor White
Write-Host "  1. Update run_pipeline.ps1 NHL block:" -ForegroundColor DarkGray
Write-Host "       py -3.14 `"NHL\scripts\step1_fetch_prizepicks_nhl.py`"" -ForegroundColor Cyan
Write-Host "  2. Add BASE_DIR to each step script:" -ForegroundColor DarkGray
Write-Host "       BASE_DIR = Path(__file__).resolve().parent.parent" -ForegroundColor Cyan
Write-Host ""
