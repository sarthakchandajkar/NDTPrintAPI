# Rebuild NDT bundle numbers, CSVs, and SQL from June 1 2026 while mills may keep running.
# NdtBundleService must be RUNNING (API on port 5000) with EnableSlitMonitoringWorker=false.
# Run scripts/rebuild-dry-run.ps1 first. Physical labels are NOT reprinted when ZPL is disabled.

param(
    [string]$ApiBase = "http://localhost:5000",
    [string]$FromUtc = "2026-06-01T00:00:00Z",
    [switch]$SkipDryRun,
    [switch]$ExecuteRebuild
)

$ErrorActionPreference = "Stop"

function Invoke-NdtApi {
    param([string]$Method, [string]$Path, $Body = $null)
    $uri = "$ApiBase$Path"
    if ($Body) {
        return Invoke-RestMethod -Method $Method -Uri $uri -ContentType "application/json" -Body ($Body | ConvertTo-Json -Depth 6)
    }
    return Invoke-RestMethod -Method $Method -Uri $uri
}

Write-Host "=== NDT rebuild preflight ===" -ForegroundColor Cyan
$preflight = Invoke-NdtApi -Method GET -Path "/api/Test/rebuild-preflight?fromUtc=$([uri]::EscapeDataString($FromUtc))&plannedMonth=6&productionYear=2026"
$preflight | ConvertTo-Json -Depth 8

if ($preflight.zplPrintEnabled) {
    Write-Host "Disabling ZPL/print..." -ForegroundColor Yellow
    Invoke-NdtApi -Method POST -Path "/api/Status/zpl-generation" -Body @{ enabled = $false } | Out-Null
}

if (-not $SkipDryRun) {
    Write-Host "`n=== Dry run ===" -ForegroundColor Cyan
    $dry = Invoke-NdtApi -Method POST -Path "/api/Test/rebuild-ndt-from-date" -Body @{
        fromUtc = $FromUtc
        plannedMonth = 6
        productionYear = 2026
        dryRun = $true
        purgeExistingFromDate = $false
        source = "InputSlitCsv"
    }
    $dry | ConvertTo-Json -Depth 8
}

if (-not $ExecuteRebuild) {
    Write-Host "`nStopped before destructive rebuild. After backup, run: .\scripts\rebuild-ndt-june2026.ps1 -ExecuteRebuild" -ForegroundColor Yellow
    exit 0
}

Write-Host "`n=== Purge + rebuild (EnableSlitMonitoringWorker must be false in appsettings) ===" -ForegroundColor Red
$body = @{
    fromUtc = $FromUtc
    plannedMonth = 6
    productionYear = 2026
    dryRun = $false
    purgeExistingFromDate = $true
    source = "InputSlitCsv"
}
if ($preflight.startingSequenceByMill) {
    $body.startingSequenceByMill = $preflight.startingSequenceByMill
}

$result = Invoke-NdtApi -Method POST -Path "/api/Test/rebuild-ndt-from-date" -Body $body
$result | ConvertTo-Json -Depth 8

Write-Host "`n=== Mill sequences after rebuild ===" -ForegroundColor Cyan
Invoke-NdtApi -Method GET -Path "/api/Status/mill-sequences" | ConvertTo-Json -Depth 8

Write-Host "`nRe-enable ZPL and start NdtBundleService when validated." -ForegroundColor Green
Write-Host "POST $ApiBase/api/Status/zpl-generation  body: { `"enabled`": true }"
