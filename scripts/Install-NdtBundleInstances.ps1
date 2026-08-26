#requires -Version 5.1
<#
.SYNOPSIS
  Installs or updates five NdtBundleService Windows Services: Shared + Mill-1..4.

.PARAMETER BasePath
  Root deploy folder containing bin\ and instances\ subfolders.

.PARAMETER ServiceAccount
  Optional credential for the service logon account. Omit for Local System.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $BasePath,

    [System.Management.Automation.PSCredential] $ServiceAccount
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$exePath = Join-Path $BasePath 'bin\NdtBundleService.exe'
if (-not (Test-Path -LiteralPath $exePath)) {
    throw "Publish output not found: $exePath — run dotnet publish first."
}

$definitions = @(
    @{
        Name        = 'NdtBundleService-Shared'
        DisplayName = 'NDT Bundle Service — Shared (Dashboard API)'
        ContentRoot = Join-Path $BasePath 'instances\shared'
    },
    @{
        Name        = 'NdtBundleService-M1'
        DisplayName = 'NDT Bundle Service — Mill 1'
        ContentRoot = Join-Path $BasePath 'instances\mill-1'
    },
    @{
        Name        = 'NdtBundleService-M2'
        DisplayName = 'NDT Bundle Service — Mill 2'
        ContentRoot = Join-Path $BasePath 'instances\mill-2'
    },
    @{
        Name        = 'NdtBundleService-M3'
        DisplayName = 'NDT Bundle Service — Mill 3'
        ContentRoot = Join-Path $BasePath 'instances\mill-3'
    },
    @{
        Name        = 'NdtBundleService-M4'
        DisplayName = 'NDT Bundle Service — Mill 4'
        ContentRoot = Join-Path $BasePath 'instances\mill-4'
    }
)

foreach ($def in $definitions) {
    if (-not (Test-Path -LiteralPath $def.ContentRoot)) {
        throw "Instance content root missing: $($def.ContentRoot)"
    }

    $binaryPath = "`"$exePath`" --contentRoot `"$($def.ContentRoot)`""
    $existing = Get-Service -Name $def.Name -ErrorAction SilentlyContinue

    if ($null -eq $existing) {
        Write-Host "Creating service $($def.Name) ..."
        $params = @{
            Name           = $def.Name
            BinaryPathName = $binaryPath
            DisplayName    = $def.DisplayName
            Description    = $def.DisplayName
            StartupType    = 'Automatic'
        }
        if ($null -ne $ServiceAccount) {
            $params['Credential'] = $ServiceAccount
        }
        New-Service @params | Out-Null
    }
    else {
        Write-Host "Updating binary path for $($def.Name) ..."
        sc.exe config $def.Name binPath= $binaryPath | Out-Null
    }

    # Restart on failure: 60s / 60s / 60s, reset failure count after 24h
    sc.exe failure $def.Name reset= 86400 actions= restart/60000/60000/60000 | Out-Null
    Write-Host "Configured recovery for $($def.Name)."
}

Write-Host @"

Services installed. Update / start order:
  1) Stop-Service NdtBundleService-M1,M2,M3,M4
  2) Stop-Service NdtBundleService-Shared
  3) Replace $BasePath\bin\
  4) Start-Service NdtBundleService-Shared
  5) Start-Service NdtBundleService-M1,M2,M3,M4

Dashboard/API: http://*:5000 (Shared)
Mill workers:  http://127.0.0.1:5001–5004 (localhost only — block from firewall)

Before production cutover: docs/VALIDATION-MILL1-NONPROD.md and docs/DEPLOYMENT-FIVE-INSTANCE.md
"@
