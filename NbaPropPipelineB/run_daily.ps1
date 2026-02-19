Write-Host "==============================="
Write-Host " NBA PROP PIPELINE RUNNER"
Write-Host "==============================="

Set-Location $PSScriptRoot

$PY    = "py"
$PYVER = "-3.14"

# Outputs
$STEP1 = "slate_input.csv"
$STEP2 = "slate_input_with_lastn_2025_26.csv"
$STEP3 = "ranked_props_with_edges_2025_26.xlsx"

# Defense file
$DEF   = "defense_team_summary.csv"

# -----------------------------
Write-Host "`n--- STEP 1: Parse PrizePicks Props ---"
& $PY $PYVER parse_props.py `
  --url "https://api.prizepicks.com/projections?league_id=7&per_page=250&single_stat=true&in_game=true&state_code=GA&game_mode=prizepools" `
  --output $STEP1

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Step 1 failed"
    exit
}

Write-Host "✅ Step 1 complete → $STEP1"

# -----------------------------
Write-Host "`n--- STEP 2: Pull Last-N Stats ---"
& $PY $PYVER slate_lastn_nbaapi.py `
  --input $STEP1 `
  --output $STEP2 `
  --season 2025-26 `
  --lastn 5

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Step 2 failed"
    exit
}

Write-Host "✅ Step 2 complete → $STEP2"

# -----------------------------
Write-Host "`n--- STEP 3: Rank Props + Edges (XLSX OUTPUT) ---"
& $PY $PYVER rank_props_with_lastn.py `
  --input $STEP2 `
  --defense-csv $DEF `
  --output $STEP3

if ($LASTEXITCODE -ne 0) {
    Write-Host "❌ Step 3 failed"
    exit
}

Write-Host "✅ Step 3 complete → $STEP3"

# -----------------------------
Write-Host "`n==============================="
Write-Host " PIPELINE COMPLETE ✅"
Write-Host "==============================="
Write-Host "Ranked Output: $STEP3"


#.\run_daily.ps1
#python build_defense_team_summary.py
#py -3.14 pull_and_grade_last_night.py --date 2026-02-06 --ranked ranked_props_with_edges_2025_26.xlsx_ranked_graded.csv
