# Quick Test Script
# Copy this to your PowerShell and run to test date parsing

$Date = "2026-03-02"

# Test the new logic
if (-not $Date) {
    $Date = Get-Date -Format "yyyy-MM-dd"
    Write-Host "  [Date] No date specified, using today: $Date" -ForegroundColor DarkGray
} else {
    # Accept the date as-is (user is responsible for format)
    # Just validate it's a reasonable date string
    if ($Date -match "^\d{4}-\d{2}-\d{2}$|^\d{1,2}/\d{1,2}/\d{4}$|^\d{1,2}-\d{1,2}-\d{4}$") {
        Write-Host "  [Date] Using specified date: $Date" -ForegroundColor Cyan
    } else {
        Write-Host "  [Date] ERROR: Invalid date format '$Date'." -ForegroundColor Red
        Write-Host "         Use formats like: 2026-03-02 or 03/02/2026" -ForegroundColor Red
        exit 1
    }
}

Write-Host "Final date: $Date"
Write-Host ""
Write-Host "Test passed! Date parsing works." -ForegroundColor Green
