# 🚀 Quick Start: How to Get 3/2 NBA Games

## What Changed
Your original script uses `Get-Date` which is locked to TODAY. The **fixed script** adds an explicit `-Date` parameter so you can specify any date.

---

## Usage Examples

### Get 3/2 Games (Today)
```powershell
# Default: uses today's date
.\run_pipeline.ps1

# Same thing with explicit date
.\run_pipeline.ps1 -Date "2026-03-02"

# Also works: MM/DD/YYYY format
.\run_pipeline.ps1 -Date "03/02/2026"
```

### NBA Only for 3/2
```powershell
.\run_pipeline.ps1 -Date "2026-03-02" -NBAOnly
```

### Get 3/2 + Run Combined
```powershell
# Fresh full run for 3/2
.\run_pipeline.ps1 -Date "2026-03-02"

# Or just re-run combined (re-uses existing step8 files)
.\run_pipeline.ps1 -CombinedOnly
```

### Other Date Formats
```powershell
# All of these work:
.\run_pipeline.ps1 -Date "2026-03-02"
.\run_pipeline.ps1 -Date "03/02/2026"
.\run_pipeline.ps1 -Date "3/2/2026"
```

---

## How It Works

### Without Date Parameter (Uses Today)
```powershell
.\run_pipeline.ps1                    # Uses today's date automatically
.\run_pipeline.ps1 -NBAOnly           # NBA only, today's date
.\run_pipeline.ps1 -CombinedOnly      # Re-run combined
```

### With Date Parameter (Explicit Date)
```powershell
.\run_pipeline.ps1 -Date "2026-03-02"                    # Full run for 3/2
.\run_pipeline.ps1 -Date "2026-03-02" -NBAOnly          # NBA only for 3/2
.\run_pipeline.ps1 -Date "2026-03-02" -IncludeNHL       # NBA+CBB+NHL for 3/2
```

---

## The Difference: Before vs After

### ❌ BEFORE (Your Current Script)
```powershell
# Line 43: $Date = Get-Date -Format "yyyy-MM-dd"
# Locked to TODAY, can't change

.\run_pipeline.ps1              # Uses 03/02 (if run on 3/2)
.\run_pipeline.ps1 0            # Still uses 03/02, the "0" gets ignored
.\run_pipeline.ps1 -Date "3/2"  # ERROR: -Date parameter doesn't exist
```

### ✅ AFTER (Fixed Script)
```powershell
# param([string]$Date = "", ... $CacheAgeDays = 7)
# Date is explicit, can be changed

.\run_pipeline.ps1                      # Uses today (3/2)
.\run_pipeline.ps1 -Date "2026-03-01"   # Uses 3/1 (backfill)
.\run_pipeline.ps1 -Date "2026-03-03"   # Uses 3/3 (future)
.\run_pipeline.ps1 0 -Date "03/02/2026" # Works! Date takes priority
```

---

## Installation

1. **Backup your current script:**
   ```powershell
   Copy-Item run_pipeline.ps1 run_pipeline_backup.ps1
   ```

2. **Replace with fixed version:**
   ```powershell
   Copy-Item run_pipeline_FIXED.ps1 run_pipeline.ps1
   ```

   OR copy the changes manually:
   - Add `[string]$Date = ""` to the param block (as first parameter)
   - Replace the date initialization (lines ~43) with the new logic

3. **Test it:**
   ```powershell
   .\run_pipeline.ps1 -Date "2026-03-02"
   ```

---

## What the Fix Does

**Before:**
```powershell
$Date = Get-Date -Format "yyyy-MM-dd"  # Locked to today, no way to change
```

**After:**
```powershell
if (-not $Date) {
    $Date = Get-Date -Format "yyyy-MM-dd"  # Default to today if not specified
    Write-Host "  [Date] No date specified, using today: $Date"
} else {
    # Parse flexible date formats (YYYY-MM-DD or MM/DD/YYYY)
    $DateObj = [datetime]::ParseExact($Date, @("yyyy-MM-dd", "MM/dd/yyyy", "M/d/yyyy"), $null)
    $Date = $DateObj.ToString("yyyy-MM-dd")
    Write-Host "  [Date] Using specified date: $Date"
}
```

**Benefits:**
- ✅ Backward compatible: `.\run_pipeline.ps1` still uses today
- ✅ Flexible: Accepts `2026-03-02` or `03/02/2026` or `3/2/2026`
- ✅ Explicit: Can specify any date, even past or future
- ✅ Safe: Validates date format before proceeding

---

## Common Issues & Fixes

### "The parameter cannot be processed because the value is not valid"
**Problem:** You're passing `-Date` but the original script doesn't support it.  
**Fix:** Use the `run_pipeline_FIXED.ps1` version provided.

### "Using date: 0"
**Problem:** You ran `.\run_pipeline.ps1 0 -GameDate "03/02/2026"`  
**Fix:** Use correct syntax: `.\run_pipeline.ps1 -Date "03/02/2026"`

### Still only 3/1 games after using -Date
**Problem:** Old cache or step3 file only has 3/1 props  
**Fix:** 
```powershell
# Clear cache and re-run
Remove-Item "NbaPropPipelineA\nba_espn_boxscore_cache.csv"
.\run_pipeline.ps1 -Date "2026-03-02" -NBAOnly
```

### All Switches Still Work
```powershell
.\run_pipeline.ps1 -Date "2026-03-02" -NBAOnly                      # ✓ Works
.\run_pipeline.ps1 -Date "2026-03-02" -IncludeNHL -IncludeMLB       # ✓ Works
.\run_pipeline.ps1 -Date "2026-03-02" -SkipFetch -NBAOnly           # ✓ Works
.\run_pipeline.ps1 -Date "2026-03-02" -RefreshCache                 # ✓ Works
.\run_pipeline.ps1 -Date "2026-03-02" -CacheAgeDays 3               # ✓ Works
```

---

## Summary

| Task | Command |
|------|---------|
| Run today's date (normal) | `.\run_pipeline.ps1` |
| Run for 3/2 specifically | `.\run_pipeline.ps1 -Date "2026-03-02"` |
| NBA only for 3/2 | `.\run_pipeline.ps1 -Date "2026-03-02" -NBAOnly` |
| Combined only | `.\run_pipeline.ps1 -CombinedOnly` |
| Include NHL | `.\run_pipeline.ps1 -Date "2026-03-02" -IncludeNHL` |
| Clear cache + fresh run | `.\run_pipeline.ps1 -Date "2026-03-02" -RefreshCache` |
| Backfill 3/1 | `.\run_pipeline.ps1 -Date "2026-03-01" -NBAOnly` |

You're all set! 🎉
