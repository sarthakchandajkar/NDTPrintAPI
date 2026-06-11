# June 2026 NDT rebuild — preflight + dry run only (no files or SQL modified).
# Prerequisites on the VM:
#   1. NdtBundleService running (http://localhost:5000 or set -ApiBase)
#   2. appsettings.Production.json: EnableSlitMonitoringWorker=false, EnableNdtTagZplAndPrint=false
#   3. Service account can read Input Slit, Input Slit Accepted, PO Accepted (UNC paths)

param(
    [string]$ApiBase = "http://localhost:5000",
    [string]$FromUtc = "2026-06-01T00:00:00Z",
    [int]$PlannedMonth = 6,
    [int]$ProductionYear = 2026
)

$ErrorActionPreference = "Stop"

function Invoke-NdtApi {
    param([string]$Method, [string]$Path, $Body = $null)
    $uri = "$ApiBase$Path"
    if ($Body) {
        return Invoke-RestMethod -Method $Method -Uri $uri -ContentType "application/json" -Body ($Body | ConvertTo-Json -Depth 8)
    }
    return Invoke-RestMethod -Method $Method -Uri $uri
}

Write-Host "Checking API..." -ForegroundColor Cyan
try {
    $health = Invoke-NdtApi -Method GET -Path "/api/Status/sql-traceability"
    Write-Host "SQL traceability: Connected=$($health.connected) Database=$($health.database)" -ForegroundColor Gray
}
catch {
    Write-Host "ERROR: Cannot reach $ApiBase — start NdtBundleService first." -ForegroundColor Red
    throw
}

Write-Host "`n=== Preflight (planned month $PlannedMonth / $ProductionYear) ===" -ForegroundColor Cyan
$preflightPath = "/api/Test/rebuild-preflight?fromUtc={0}&plannedMonth={1}&productionYear={2}" -f `
    [uri]::EscapeDataString($FromUtc), $PlannedMonth, $ProductionYear
$preflight = Invoke-NdtApi -Method GET -Path $preflightPath
$preflight | ConvertTo-Json -Depth 8

Write-Host "`nReview targetPoByMill (expected June POs per mill) and startingSequenceByMill before continuing." -ForegroundColor Yellow

Write-Host "`n=== Dry run (read-only) ===" -ForegroundColor Cyan
$dry = Invoke-NdtApi -Method POST -Path "/api/Test/rebuild-ndt-from-date" -Body @{
    fromUtc = $FromUtc
    plannedMonth = $PlannedMonth
    productionYear = $ProductionYear
    dryRun = $true
    purgeExistingFromDate = $false
    source = "InputSlitCsv"
}
$dry | ConvertTo-Json -Depth 8

Write-Host "`nDry run complete. Check slitRowsReplayed, slitRowsExcluded, excludedSamples, targetPoByMill." -ForegroundColor Green
Write-Host "When ready for purge+rebuild: backup SQL/CSV/state, then run .\scripts\rebuild-ndt-june2026.ps1 -ExecuteRebuild"
