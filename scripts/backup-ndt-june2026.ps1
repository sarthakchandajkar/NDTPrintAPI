# Backup JazeeraMES_Prod and NDT output folders before June 2026 rebuild.
# Run on the VM as a user with SQL backup rights and UNC read access to pas-sap shares.

param(
    [string]$BackupRoot = "C:\Backups\NDT-June2026-Rebuild",
    [string]$SqlServer = "AJS-SOH-VM-PAS-\SQLEXPRESS",
    [string]$SqlDatabase = "JazeeraMES_Prod",
    [string]$StateFile = "\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Input Slit\NdtBundleRuntimeState.json"
)

$ErrorActionPreference = "Stop"
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$dest = Join-Path $BackupRoot $stamp
New-Item -ItemType Directory -Path $dest -Force | Out-Null

Write-Host "Backup folder: $dest" -ForegroundColor Cyan

# SQL database
$sqlBak = Join-Path $dest "$SqlDatabase.bak"
Write-Host "Backing up SQL $SqlDatabase..." -ForegroundColor Cyan
$query = "BACKUP DATABASE [$SqlDatabase] TO DISK = N'$($sqlBak -replace "'", "''")' WITH INIT, COMPRESSION, STATS = 10;"
sqlcmd -S $SqlServer -E -Q $query
if ($LASTEXITCODE -ne 0) { throw "SQL backup failed." }
Write-Host "SQL backup: $sqlBak" -ForegroundColor Green

# Runtime state
if (Test-Path -LiteralPath $StateFile) {
    $stateDest = Join-Path $dest "NdtBundleRuntimeState.json"
    Copy-Item -LiteralPath $StateFile -Destination $stateDest -Force
    Write-Host "State file copied." -ForegroundColor Green
}
else {
    Write-Host "WARN: State file not found at $StateFile" -ForegroundColor Yellow
}

# NDT derived folders (not source Input Slit / Input Slit Accepted)
$folders = @(
    "\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Input Slit\Input Slit",
    "\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Bundles",
    "\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Final Output\Bundle",
    "\\10.2.20.210\pas-sap\To SAP\TM\NDT\MES PAS NDT\Bundle"
)
foreach ($src in $folders) {
    $name = ($src -split '\\')[-1]
    if ($src -match 'NDT Input Slit\\Input Slit$') { $name = "NDT-Input-Slit-Output" }
    $target = Join-Path $dest $name
    if (Test-Path -LiteralPath $src) {
        Write-Host "Robocopy $src ..." -ForegroundColor Cyan
        robocopy $src $target /E /R:2 /W:5 /NFL /NDL /NJH /NJS /nc /ns /np | Out-Null
        if ($LASTEXITCODE -ge 8) { throw "Robocopy failed for $src (exit $LASTEXITCODE)" }
        Write-Host "  -> $target" -ForegroundColor Green
    }
    else {
        Write-Host "WARN: Folder not found: $src" -ForegroundColor Yellow
    }
}

Write-Host "`nBackup complete: $dest" -ForegroundColor Green
Write-Host "Next: .\scripts\rebuild-ndt-june2026.ps1 -ExecuteRebuild"
