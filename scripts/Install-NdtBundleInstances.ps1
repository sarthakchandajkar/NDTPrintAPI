#requires -Version 5.1
<#
.SYNOPSIS
  Installs or updates NdtBundleService Windows Services (Shared + Mill-1..4).

.DESCRIPTION
  Each service points at a release folder of NdtBundleService.exe plus its own
  instances\<role> content root. Prefer versioned releases under BasePath\releases\<id>
  so one service can be retargeted without locking a shared bin\ used by others.

.PARAMETER BasePath
  Root deploy folder containing instances\ and either bin\ or releases\.

.PARAMETER ReleasePath
  Folder containing NdtBundleService.exe for the services being (re)configured.
  Defaults to BasePath\bin (legacy shared layout). Prefer BasePath\releases\<git-sha>.

.PARAMETER Instance
  Optional. When set, only that service is created/updated:
  Shared | M1 | M2 | M3 | M4 (aliases: mill-1..mill-4, 1..4).

.PARAMETER ServiceAccount
  Optional credential for the service logon account. Omit for Local System.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string] $BasePath,

    [string] $ReleasePath,

    [ValidateSet('Shared', 'M1', 'M2', 'M3', 'M4', 'mill-1', 'mill-2', 'mill-3', 'mill-4', '1', '2', '3', '4')]
    [string] $Instance,

    [System.Management.Automation.PSCredential] $ServiceAccount
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Resolve-InstanceKey {
    param([string] $Name)
    if ([string]::IsNullOrWhiteSpace($Name)) { return $null }
    switch -Regex ($Name.Trim()) {
        '^(?i)shared$' { return 'Shared' }
        '^(?i)(m1|mill-1|1)$' { return 'M1' }
        '^(?i)(m2|mill-2|2)$' { return 'M2' }
        '^(?i)(m3|mill-3|3)$' { return 'M3' }
        '^(?i)(m4|mill-4|4)$' { return 'M4' }
        default { throw "Unknown Instance '$Name'. Use Shared, M1, M2, M3, or M4." }
    }
}

$BasePath = [System.IO.Path]::GetFullPath($BasePath.TrimEnd('\', '/'))
if ([string]::IsNullOrWhiteSpace($ReleasePath)) {
    $ReleasePath = Join-Path $BasePath 'bin'
}
else {
    $ReleasePath = [System.IO.Path]::GetFullPath($ReleasePath.TrimEnd('\', '/'))
}

$exePath = Join-Path $ReleasePath 'NdtBundleService.exe'
if (-not (Test-Path -LiteralPath $exePath)) {
    throw "Publish output not found: $exePath - run dotnet publish -o `"$ReleasePath`" first (or Deploy-NdtBundleInstance.ps1)."
}

# Never point services at BasePath\releases itself (publish must be releases\<id>\).
$releasesRoot = [System.IO.Path]::GetFullPath((Join-Path $BasePath 'releases'))
if ([string]::Equals(
        $ReleasePath.TrimEnd('\', '/'),
        $releasesRoot.TrimEnd('\', '/'),
        [StringComparison]::OrdinalIgnoreCase)) {
    throw @"
ReleasePath must be a versioned subfolder, not the releases root.
  Bad:  $releasesRoot
  Good: $releasesRoot\<git-sha>
Your publish was dumped into releases\ (you will see de\, es\, LatoFont\, runtimes\ there).
Re-publish to releases\<sha> and re-run Install/Deploy.
"@
}

$allDefinitions = @(
    @{
        Key         = 'Shared'
        Name        = 'NdtBundleService-Shared'
        DisplayName = 'NDT Bundle Service - Shared (Dashboard API)'
        ContentRoot = Join-Path $BasePath 'instances\shared'
    },
    @{
        Key         = 'M1'
        Name        = 'NdtBundleService-M1'
        DisplayName = 'NDT Bundle Service - Mill 1'
        ContentRoot = Join-Path $BasePath 'instances\mill-1'
    },
    @{
        Key         = 'M2'
        Name        = 'NdtBundleService-M2'
        DisplayName = 'NDT Bundle Service - Mill 2'
        ContentRoot = Join-Path $BasePath 'instances\mill-2'
    },
    @{
        Key         = 'M3'
        Name        = 'NdtBundleService-M3'
        DisplayName = 'NDT Bundle Service - Mill 3'
        ContentRoot = Join-Path $BasePath 'instances\mill-3'
    },
    @{
        Key         = 'M4'
        Name        = 'NdtBundleService-M4'
        DisplayName = 'NDT Bundle Service - Mill 4'
        ContentRoot = Join-Path $BasePath 'instances\mill-4'
    }
)

$filterKey = Resolve-InstanceKey -Name $Instance
$definitions = if ($null -eq $filterKey) {
    $allDefinitions
}
else {
    @($allDefinitions | Where-Object { $_.Key -eq $filterKey })
}

if ($definitions.Count -eq 0) {
    throw "No service definitions matched Instance='$Instance'."
}

foreach ($def in $definitions) {
    if (-not (Test-Path -LiteralPath $def.ContentRoot)) {
        throw "Instance content root missing: $($def.ContentRoot)"
    }

    # Quoted exe + contentRoot (required when paths have spaces; keeps sc/CIM from stripping args).
    $binaryPath = '"{0}" --contentRoot "{1}"' -f $exePath, $def.ContentRoot
    $existing = Get-Service -Name $def.Name -ErrorAction SilentlyContinue

    if ($null -eq $existing) {
        Write-Host "Creating service $($def.Name) -> $ReleasePath"
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
        Write-Host "Updating binary path for $($def.Name) -> $ReleasePath"
        # sc.exe + PowerShell often strips quotes from binPath=; CIM Change preserves them.
        $cim = Get-CimInstance -ClassName Win32_Service -Filter "Name='$($def.Name)'" -ErrorAction Stop
        $result = Invoke-CimMethod -InputObject $cim -MethodName Change -Arguments @{ PathName = $binaryPath }
        if ($null -eq $result -or [int]$result.ReturnValue -ne 0) {
            $code = if ($null -eq $result) { 'null' } else { $result.ReturnValue }
            throw "Win32_Service.Change failed for $($def.Name) (ReturnValue=$code)."
        }
    }

    sc.exe failure $def.Name reset= 86400 actions= restart/60000/60000/60000 | Out-Null
    Write-Host "Configured recovery for $($def.Name)."
}

Write-Host @"

Configured $($definitions.Count) service(s) against:
  Release: $ReleasePath
  Base:    $BasePath

Single-service deploy (others keep running):
  .\scripts\Deploy-NdtBundleInstance.ps1 -BasePath $BasePath -Instance M1

First-time all five (after publish to a release folder):
  .\scripts\Install-NdtBundleInstances.ps1 -BasePath $BasePath -ReleasePath $ReleasePath

Dashboard/API: http://*:5000 (Shared)
Mill workers:  http://127.0.0.1:5001-5004 (localhost only)

See docs/DEPLOYMENT-FIVE-INSTANCE.md
"@
