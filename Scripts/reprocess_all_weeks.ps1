# Reprocess all weeks 1-58 with the current mapping (e.g. Week 42 Cell Mapping - reviewed corrected),
# then run the slip-vs-pallet audit and write SlipTotals_vs_PalletLines_2025.csv.
# Run from repo root: .\Scripts\reprocess_all_weeks.ps1

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not (Test-Path (Join-Path $root "Scripts\process_weekly_loading_slip.py"))) {
    $root = $PSScriptRoot
}
Set-Location $root

Write-Host "Reprocessing weeks 1-58 with current mapping, then auditing."
Write-Host ""

1..58 | ForEach-Object {
    Write-Host "`n=== Week $_ ===" -ForegroundColor Cyan
    python Scripts/process_weekly_loading_slip.py --week $_
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

Write-Host "`n=== Audit: slip vs PalletLines for all weeks ===" -ForegroundColor Cyan
python Scripts/process_weekly_loading_slip.py --audit-all-slips
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`nDone. SlipTotals_vs_PalletLines_2025.csv updated in Traceability_Exports." -ForegroundColor Green
