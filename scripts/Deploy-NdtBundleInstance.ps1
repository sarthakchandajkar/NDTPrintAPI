#requires -Version 5.1
<#
.SYNOPSIS
  Publishes a versioned release and retargets / restarts ONE NdtBundleService instance.

.DESCRIPTION
  Enables singular deploys without stopping Shared or other mills:
    1) dotnet publish -> BasePath\releases\<ReleaseId>\
    2) sc config only the chosen service to that folder
    3) Stop + Start only that service

  Other services keep their existing BinaryPathName and stay running.

.PARAMETER BasePath
  Deploy root (default C:\Apps\NdtBundleService).

.PARAMETER Instance
  Shared | M1 | M2 | M3 | M4 (aliases mill-1..4, 1..4).

.PARAMETER RepoRoot
  Git repo root for publish (default: parent of scripts\).

.PARAMETER ReleaseId
  Folder name under releases\. Default: short git SHA, else yyyyMMdd-HHmmss.

.PARAMETER SkipPublish
  Only retarget/restart using an existing -ReleasePath (or releases\<ReleaseId>).

.PARAMETER ReleasePath
  Explicit publish folder. When set with -SkipPublish, must already contain the exe.

.PARAMETER ServiceAccount
  Forwarded to Install-NdtBundleInstances.ps1 on first create only.
#>
[CmdletBinding()]
param(
    [string] $BasePath = 'C:\Apps\NdtBundleService',

    [Parameter(Mandatory = $true)]
    [ValidateSet('Shared', 'M1', 'M2', 'M3', 'M4', 'mill-1', 'mill-2', 'mill-3', 'mill-4', '1', '2', '3', '4')]
    [string] $Instance,

    [string] $RepoRoot,

    [string] $ReleaseId,

    [switch] $SkipPublish,

    [string] $ReleasePath,

    [System.Management.Automation.PSCredential] $ServiceAccount
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Resolve-InstanceKey {
    param([string] $Name)
    switch -Regex ($Name.Trim()) {
        '^(?i)shared$' { return 'Shared' }
        '^(?i)(m1|mill-1|1)$' { return 'M1' }
        '^(?i)(m2|mill-2|2)$' { return 'M2' }
        '^(?i)(m3|mill-3|3)$' { return 'M3' }
        '^(?i)(m4|mill-4|4)$' { return 'M4' }
        default { throw "Unknown Instance '$Name'." }
    }
}

$instanceKey = Resolve-InstanceKey -Name $Instance
$serviceName = switch ($instanceKey) {
    'Shared' { 'NdtBundleService-Shared' }
    'M1' { 'NdtBundleService-M1' }
    'M2' { 'NdtBundleService-M2' }
    'M3' { 'NdtBundleService-M3' }
    'M4' { 'NdtBundleService-M4' }
}

$BasePath = [System.IO.Path]::GetFullPath($BasePath.TrimEnd('\', '/'))
if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
}
else {
    $RepoRoot = [System.IO.Path]::GetFullPath($RepoRoot.TrimEnd('\', '/'))
}

$project = Join-Path $RepoRoot 'src\NdtBundleService\NdtBundleService.csproj'
if (-not (Test-Path -LiteralPath $project)) {
    throw "Project not found: $project (set -RepoRoot to the NDTPrintAPI clone)."
}

$releasesRoot = Join-Path $BasePath 'releases'
if (-not (Test-Path -LiteralPath $releasesRoot)) {
    New-Item -ItemType Directory -Path $releasesRoot | Out-Null
}

function Assert-VersionedReleasePath {
    param([string] $Path, [string] $ReleasesRoot)
    $full = [System.IO.Path]::GetFullPath($Path.TrimEnd('\', '/'))
    $root = [System.IO.Path]::GetFullPath($ReleasesRoot.TrimEnd('\', '/'))
    if ([string]::Equals($full, $root, [StringComparison]::OrdinalIgnoreCase)) {
        throw @"
Refuse to use releases\ root as a publish target (locks / mixes localization folders with the exe).
Use a versioned folder, e.g. $root\<git-sha>
"@
    }
    $parent = Split-Path -Parent $full
    if (-not [string]::Equals($parent, $root, [StringComparison]::OrdinalIgnoreCase)) {
        Write-Host "Note: ReleasePath is not under $root (allowed, but unusual): $full"
    }
}

if ([string]::IsNullOrWhiteSpace($ReleasePath)) {
    if ([string]::IsNullOrWhiteSpace($ReleaseId)) {
        Push-Location $RepoRoot
        try {
            $sha = (& git rev-parse --short HEAD 2>$null)
            if ($sha -is [array]) { $sha = $sha | Select-Object -First 1 }
            $sha = [string]$sha
            if ($null -ne $sha) { $sha = $sha.Trim() }
        }
        finally {
            Pop-Location
        }
        if ([string]::IsNullOrWhiteSpace($sha) -or $sha -notmatch '^[a-fA-F0-9]{4,40}$') {
            $ReleaseId = Get-Date -Format 'yyyyMMdd-HHmmss'
        }
        else {
            $ReleaseId = $sha
        }
    }
    $ReleasePath = Join-Path $releasesRoot $ReleaseId
}
else {
    $ReleasePath = [System.IO.Path]::GetFullPath($ReleasePath.TrimEnd('\', '/'))
    if ([string]::IsNullOrWhiteSpace($ReleaseId)) {
        $ReleaseId = Split-Path -Leaf $ReleasePath
    }
}

Assert-VersionedReleasePath -Path $ReleasePath -ReleasesRoot $releasesRoot

$exePath = Join-Path $ReleasePath 'NdtBundleService.exe'

if (-not $SkipPublish) {
    Write-Host "Publishing to $ReleasePath ..."
    if (-not (Test-Path -LiteralPath $ReleasePath)) {
        New-Item -ItemType Directory -Path $ReleasePath | Out-Null
    }
    & dotnet publish $project -c Release -o $ReleasePath
    if ($LASTEXITCODE -ne 0) {
        throw "dotnet publish failed (exit $LASTEXITCODE)."
    }
}

if (-not (Test-Path -LiteralPath $exePath)) {
    throw "NdtBundleService.exe not found at $exePath. Publish first or drop -SkipPublish."
}

$installScript = Join-Path $PSScriptRoot 'Install-NdtBundleInstances.ps1'
$installArgs = @{
    BasePath    = $BasePath
    ReleasePath = $ReleasePath
    Instance    = $instanceKey
}
if ($null -ne $ServiceAccount) {
    $installArgs['ServiceAccount'] = $ServiceAccount
}

Write-Host "Retargeting $serviceName to $ReleasePath ..."
& $installScript @installArgs

Write-Host "Restarting $serviceName only (other instances stay up) ..."
$svc = Get-Service -Name $serviceName -ErrorAction Stop
if ($svc.Status -ne 'Stopped') {
    Stop-Service -Name $serviceName -Force
    $svc.WaitForStatus('Stopped', [TimeSpan]::FromSeconds(60))
}

Start-Service -Name $serviceName
(Get-Service -Name $serviceName).WaitForStatus('Running', [TimeSpan]::FromSeconds(60))

Write-Host @"

Done.
  Instance:  $instanceKey ($serviceName)
  Release:   $ReleasePath
  Status:    $((Get-Service -Name $serviceName).Status)

Verify: sc.exe qc $serviceName
Other services were not stopped.
"@
