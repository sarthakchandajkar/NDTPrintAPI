# Re-enable live NDT processing after June rebuild validation.
# Updates appsettings.Production.json flags and restarts NdtBundleService (if installed).

param(
    [string]$ApiBase = "http://localhost:5000",
    [string]$AppSettingsPath = "",
    [string]$ServiceName = "NdtBundleService",
    [switch]$SkipServiceRestart
)

$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($AppSettingsPath)) {
    $repoRoot = Split-Path $PSScriptRoot -Parent
    $candidates = @(
        (Join-Path $repoRoot "src\NdtBundleService\appsettings.Production.json"),
        "C:\Apps\NdtBundleService\appsettings.Production.json"
    )
    foreach ($c in $candidates) {
        if (Test-Path -LiteralPath $c) { $AppSettingsPath = $c; break }
    }
}

if (-not (Test-Path -LiteralPath $AppSettingsPath)) {
    throw "appsettings.Production.json not found. Pass -AppSettingsPath."
}

Write-Host "Enabling production flags in $AppSettingsPath ..." -ForegroundColor Cyan
$content = Get-Content -LiteralPath $AppSettingsPath -Raw
$content = $content -replace '"EnableSlitMonitoringWorker":\s*false', '"EnableSlitMonitoringWorker": true'
$content = $content -replace '"EnableNdtTagZplAndPrint":\s*false', '"EnableNdtTagZplAndPrint": true'
$content = $content -replace '"EnableUploadNdtBundleScheduler":\s*false', '"EnableUploadNdtBundleScheduler": true'
$content = $content -replace '("MillSlitLive"\s*:\s*\{[\s\S]*?"Enabled"\s*:\s*)false', '${1}true'
$content = $content -replace '("PlcHandshake"\s*:\s*\{[\s\S]*?"Enabled"\s*:\s*)false', '${1}true'
Set-Content -LiteralPath $AppSettingsPath -Value $content -Encoding UTF8 -NoNewline
Add-Content -LiteralPath $AppSettingsPath -Value ""

Write-Host "POST ZPL enable..." -ForegroundColor Cyan
try {
    Invoke-RestMethod -Method POST -Uri "$ApiBase/api/Status/zpl-generation" `
        -ContentType "application/json" -Body '{"enabled":true}' | Out-Null
}
catch {
    Write-Host "WARN: Could not reach API to enable ZPL (service may be stopped): $_" -ForegroundColor Yellow
}

if (-not $SkipServiceRestart) {
    $svc = Get-Service -Name $ServiceName -ErrorAction SilentlyContinue
    if ($svc) {
        Write-Host "Restarting $ServiceName ..." -ForegroundColor Cyan
        Restart-Service -Name $ServiceName -Force
        Write-Host "Service restarted." -ForegroundColor Green
    }
    else {
        Write-Host "Service '$ServiceName' not found. Restart the app manually." -ForegroundColor Yellow
    }
}

Write-Host "`nProduction flags enabled. Verify mill sequences and a test slit before leaving unattended." -ForegroundColor Green
