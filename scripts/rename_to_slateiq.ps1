# ============================================================
#  rename_to_slateiq.ps1
#  Renames root folder and all sport subfolders to match
#  SlateIQ architecture, then updates all path references
#  inside .ps1 and .py files automatically.
#
#  Renames:
#    SlateIQ\          -> SlateIQ\          (parent folder)
#    SlateIQ\NBA\ -> SlateIQ\NBA\
#    SlateIQ\CBB\           -> SlateIQ\CBB\
#    SlateIQ\NHL\            (created if not exists)
#    SlateIQ\Soccer\         (created if not exists)
#    SlateIQ\MLB\            (created if not exists)
#    SlateIQ\WNBA\           (created if not exists)
#
#  Usage:
#    .\rename_to_slateiq.ps1           # Preview mode
#    .\rename_to_slateiq.ps1 -Execute  # Apply all changes
# ============================================================
param([switch]$Execute)

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Definition
$ParentDir  = Split-Path -Parent $ScriptDir
$OldRoot    = $ScriptDir
$NewRoot    = Join-Path $ParentDir "SlateIQ"

# ── Subfolder rename map ──────────────────────────────────────────────────────
$SubRenames = [ordered]@{
    "NBA" = "NBA"
    "CBB"             = "CBB"
}

# ── Sport folders to ensure exist (new or already named correctly) ────────────
$EnsureFolders = @("NBA", "CBB", "NHL", "Soccer", "MLB", "WNBA")

# ── Path string replacements for .ps1 and .py files ──────────────────────────
# Order matters — more specific replacements first
$PathReplacements = [ordered]@{
    # Root folder name
    "SlateIQ"      = "SlateIQ"
    # Sport subfolder names
    "NBA"   = "NBA"
    "CBB"               = "CBB"
}

# ── Display helpers ───────────────────────────────────────────────────────────
function Write-Preview { param([string]$msg) Write-Host "  $msg" -ForegroundColor Yellow }
function Write-Ok      { param([string]$msg) Write-Host "  $msg" -ForegroundColor Green  }
function Write-Info    { param([string]$msg) Write-Host "  $msg" -ForegroundColor DarkGray }
function Write-Warn    { param([string]$msg) Write-Host "  $msg" -ForegroundColor Red    }

# ─────────────────────────────────────────────────────────────────────────────
if (-not $Execute) {
    Write-Host ""
    Write-Host "=====================================================" -ForegroundColor Cyan
    Write-Host "  PREVIEW MODE — no changes will be made" -ForegroundColor Cyan
    Write-Host "  Run with -Execute to apply" -ForegroundColor Cyan
    Write-Host "=====================================================" -ForegroundColor Cyan
} else {
    Write-Host ""
    Write-Host "=====================================================" -ForegroundColor Yellow
    Write-Host "  EXECUTE MODE — applying all renames now" -ForegroundColor Yellow
    Write-Host "=====================================================" -ForegroundColor Yellow
}
Write-Host ""

# =============================================================================
#  STEP 1 — Rename sport subfolders first (while still inside OldRoot)
# =============================================================================
Write-Host "[ 1 ] Rename sport subfolders" -ForegroundColor Magenta
Write-Host ""

foreach ($old in $SubRenames.Keys) {
    $new     = $SubRenames[$old]
    $oldPath = Join-Path $OldRoot $old
    $newPath = Join-Path $OldRoot $new

    if (Test-Path $oldPath) {
        if (Test-Path $newPath) {
            Write-Info "SKIP: $old\ already exists as $new\ (or rename already done)"
        } else {
            Write-Preview "RENAME  $old\  ->  $new\"
            if ($Execute) {
                try {
                    Rename-Item $oldPath $new -ErrorAction Stop
                    Write-Ok    "        OK"
                } catch {
                    Write-Warn  "        FAILED: $_"
                }
            }
        }
    } else {
        Write-Info "SKIP: $old\ not found (already renamed or doesn't exist)"
    }
}

# =============================================================================
#  STEP 2 — Ensure all sport folders exist
# =============================================================================
Write-Host ""
Write-Host "[ 2 ] Ensure sport folders exist" -ForegroundColor Magenta
Write-Host ""

foreach ($sport in $EnsureFolders) {
    # After root rename these will be under $NewRoot, but during execute
    # we create them under $OldRoot first (root rename happens in step 3)
    $targetRoot = if ($Execute) { $OldRoot } else { $NewRoot }
    $sportPath  = Join-Path $targetRoot $sport
    if (Test-Path $sportPath) {
        Write-Info "EXISTS: $sport\"
    } else {
        Write-Preview "CREATE  $sport\"
        if ($Execute) {
            try {
                New-Item -ItemType Directory -Force -Path $sportPath | Out-Null
                Write-Ok "        OK — created $sport\"
            } catch {
                Write-Warn "        FAILED: $_"
            }
        }
    }
}

# =============================================================================
#  STEP 3 — Update path references in all .ps1 and .py files
# =============================================================================
Write-Host ""
Write-Host "[ 3 ] Update path references in scripts" -ForegroundColor Magenta
Write-Host ""

$scriptFiles = @(
    Get-ChildItem $OldRoot -Filter "*.ps1" -File -ErrorAction SilentlyContinue
    Get-ChildItem (Join-Path $OldRoot "scripts") -Filter "*.ps1" -File -Recurse -ErrorAction SilentlyContinue
    Get-ChildItem (Join-Path $OldRoot "scripts") -Filter "*.py"  -File -Recurse -ErrorAction SilentlyContinue
    Get-ChildItem (Join-Path $OldRoot "NBA")     -Filter "*.py"  -File -Recurse -ErrorAction SilentlyContinue
    Get-ChildItem (Join-Path $OldRoot "CBB")     -Filter "*.py"  -File -Recurse -ErrorAction SilentlyContinue
    # Also check old names in case subfolders weren't renamed yet
    Get-ChildItem (Join-Path $OldRoot "NBA") -Filter "*.py" -File -Recurse -ErrorAction SilentlyContinue
    Get-ChildItem (Join-Path $OldRoot "CBB")    -Filter "*.py"  -File -Recurse -ErrorAction SilentlyContinue
)

foreach ($file in $scriptFiles) {
    if (-not $file -or -not (Test-Path $file.FullName)) { continue }

    $content     = Get-Content $file.FullName -Raw -Encoding UTF8
    $newContent  = $content
    $changed     = $false
    $changeLog   = @()

    foreach ($old in $PathReplacements.Keys) {
        $new = $PathReplacements[$old]
        if ($newContent -match [regex]::Escape($old)) {
            $newContent = $newContent -replace [regex]::Escape($old), $new
            $changed    = $true
            $changeLog += "$old -> $new"
        }
    }

    if ($changed) {
        $relPath = $file.FullName.Replace($OldRoot, "").TrimStart("\")
        Write-Preview "UPDATE  $relPath"
        foreach ($log in $changeLog) { Write-Info "        $log" }
        if ($Execute) {
            try {
                Set-Content $file.FullName -Value $newContent -Encoding UTF8
                Write-Ok "        OK"
            } catch {
                Write-Warn "        FAILED: $_"
            }
        }
    }
}

# =============================================================================
#  STEP 4 — Rename root folder  (must be last — we're running from inside it)
# =============================================================================
Write-Host ""
Write-Host "[ 4 ] Rename root folder" -ForegroundColor Magenta
Write-Host ""

$currentRootName = Split-Path $OldRoot -Leaf

if ($currentRootName -eq "SlateIQ") {
    Write-Info "Root is already named SlateIQ — skipping"
} elseif (Test-Path $NewRoot) {
    Write-Warn "WARNING: SlateIQ\ already exists at $NewRoot"
    Write-Warn "         Resolve conflict manually before renaming root."
} else {
    Write-Preview "RENAME  $currentRootName\  ->  SlateIQ\"
    Write-Preview "        FROM: $OldRoot"
    Write-Preview "        TO:   $NewRoot"
    if ($Execute) {
        Write-Host ""
        Write-Host "  NOTE: Cannot rename the folder this script is running from." -ForegroundColor Cyan
        Write-Host "  Run this command from the PARENT directory instead:" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "    cd `"$ParentDir`"" -ForegroundColor White
        Write-Host "    Rename-Item `"$OldRoot`" `"SlateIQ`"" -ForegroundColor White
        Write-Host ""
        Write-Host "  All internal renames and reference updates are complete." -ForegroundColor Green
        Write-Host "  Only the root folder rename remains — run the two lines above." -ForegroundColor Green
    }
}

# =============================================================================
#  SUMMARY
# =============================================================================
Write-Host ""
Write-Host "=====================================================" -ForegroundColor Cyan
if (-not $Execute) {
    Write-Host "  PREVIEW COMPLETE — run with -Execute to apply" -ForegroundColor Cyan
} else {
    Write-Host "  DONE. Final structure:" -ForegroundColor Green
    Write-Host ""
    Write-Host "  SlateIQ\"                      -ForegroundColor White
    Write-Host "  ├── run_pipeline.ps1"          -ForegroundColor Green
    Write-Host "  ├── run_grader.ps1"            -ForegroundColor Green
    Write-Host "  ├── scripts\"                  -ForegroundColor Yellow
    Write-Host "  │   └── grading\"              -ForegroundColor Yellow
    Write-Host "  ├── NBA\          (was NBA)" -ForegroundColor Cyan
    Write-Host "  ├── CBB\          (was CBB)"  -ForegroundColor Cyan
    Write-Host "  ├── NHL\          (ready)"     -ForegroundColor Cyan
    Write-Host "  ├── Soccer\       (ready)"     -ForegroundColor Cyan
    Write-Host "  ├── MLB\          (ready)"     -ForegroundColor Cyan
    Write-Host "  ├── WNBA\         (ready)"     -ForegroundColor Cyan
    Write-Host "  ├── outputs\"                  -ForegroundColor Cyan
    Write-Host "  └── ui_runner\"                -ForegroundColor Cyan
}
Write-Host "=====================================================" -ForegroundColor Cyan
Write-Host ""

