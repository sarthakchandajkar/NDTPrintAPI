# Rebuild NDT bundle numbers, CSVs, and SQL for June 2026 planned production.
# NdtBundleService must be RUNNING with EnableSlitMonitoringWorker=false and ZPL disabled.
# Run scripts/rebuild-dry-run.ps1 and scripts/backup-ndt-june2026.ps1 first.

param(
    [string]$ApiBase = "http://localhost:5000",
    [string]$FromUtc = "2026-06-01T00:00:00Z",
    [switch]$SkipDryRun,
    [switch]$ExecuteRebuild,
    [int]$TimeoutSec = 3600
)

$ErrorActionPreference = "Stop"

function Invoke-NdtApi {
    param([string]$Method, [string]$Path, $Body = $null)
    $uri = "$ApiBase$Path"
    if ($Body) {
        return Invoke-RestMethod -Method $Method -Uri $uri -ContentType "application/json" `
            -Body ($Body | ConvertTo-Json -Depth 8) -TimeoutSec $TimeoutSec
    }
    return Invoke-RestMethod -Method $Method -Uri $uri -TimeoutSec $TimeoutSec
}

Write-Host "=== NDT rebuild preflight ===" -ForegroundColor Cyan
$preflightPath = '/api/Test/rebuild-preflight?fromUtc={0}&plannedMonth={1}&productionYear={2}' -f `
    [uri]::EscapeDataString($FromUtc), 6, 2026
$preflight = Invoke-NdtApi -Method GET -Path $preflightPath
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
    if ($dry.slitRowsReplayed -le 0) {
        Write-Host "ERROR: slitRowsReplayed is 0. Do not execute rebuild." -ForegroundColor Red
        exit 1
    }
}

if (-not $ExecuteRebuild) {
    Write-Host "`nStopped before destructive rebuild." -ForegroundColor Yellow
    Write-Host '  1. .\scripts\backup-ndt-june2026.ps1'
    Write-Host '  2. .\scripts\rebuild-ndt-june2026.ps1 -ExecuteRebuild -SkipDryRun'
    exit 0
}

$confirm = Read-Host "`nPURGE + REBUILD will delete June NDT SQL/CSV and regenerate. Type YES to continue"
if ($confirm -ne "YES") {
    Write-Host "Aborted." -ForegroundColor Yellow
    exit 0
}

Write-Host "`n=== Purge + rebuild ===" -ForegroundColor Red
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

Write-Host "`nValidate output, then run: .\scripts\post-rebuild-enable-production.ps1" -ForegroundColor Green
