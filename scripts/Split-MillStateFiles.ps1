#requires -Version 5.1
<#
.SYNOPSIS
  One-time split of monolith NdtBundleRuntimeState.json / MillPrinterSettings.json / PoLifecycleState.json
  into per-mill files (M1–M4).

.PARAMETER SourceRuntimeStateFile
  Full path to the monolith NdtBundleRuntimeState.json.

.PARAMETER MillNo
  Optional single mill (1–4). When omitted, splits all mills 1–4.

.PARAMETER WhatIf
  Report actions without writing.
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter(Mandatory = $true)]
    [string] $SourceRuntimeStateFile,

    [string] $SourcePrinterSettingsFile = "",

    [string] $SourceLifecycleStateFile = "",

    [ValidateRange(1, 4)]
    [int[]] $MillNo = @(1, 2, 3, 4)
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-JsonProperty {
    param(
        [Parameter(Mandatory = $true)] $Object,
        [Parameter(Mandatory = $true)] [string] $Name,
        [Parameter(Mandatory = $true)] [string] $Context,
        [switch] $Required
    )

    if ($null -eq $Object) {
        if ($Required) { throw "$Context is null (expected property '$Name')." }
        return $null
    }

    $prop = $Object.PSObject.Properties |
        Where-Object { $_.Name -ieq $Name } |
        Select-Object -First 1

    if ($null -eq $prop) {
        if ($Required) {
            $keys = @($Object.PSObject.Properties | ForEach-Object { $_.Name }) -join ', '
            throw "$Context missing required property '$Name' (case-insensitive). Found: $keys"
        }
        return $null
    }

    return $prop
}

function Get-PropertyCount {
    param($Object)
    if ($null -eq $Object) { return 0 }
    return @($Object.PSObject.Properties).Count
}

if (-not (Test-Path -LiteralPath $SourceRuntimeStateFile)) {
    throw "Source runtime state file not found: $SourceRuntimeStateFile"
}

$dir = Split-Path -Parent $SourceRuntimeStateFile
if ([string]::IsNullOrWhiteSpace($SourcePrinterSettingsFile)) {
    $SourcePrinterSettingsFile = Join-Path $dir 'MillPrinterSettings.json'
}
if ([string]::IsNullOrWhiteSpace($SourceLifecycleStateFile)) {
    $SourceLifecycleStateFile = Join-Path $dir 'PoLifecycleState.json'
}

$raw = Get-Content -LiteralPath $SourceRuntimeStateFile -Raw -Encoding UTF8
try {
    $root = $raw | ConvertFrom-Json
}
catch {
    throw "Source runtime state is not valid JSON ($SourceRuntimeStateFile): $($_.Exception.Message)"
}

$millsProp = Get-JsonProperty -Object $root -Name 'mills' -Context 'NdtBundleRuntimeState' -Required
$millMaxProp = Get-JsonProperty -Object $root -Name 'millMaxSequence' -Context 'NdtBundleRuntimeState'
if ($null -eq $millsProp.Value -or $millsProp.Value -is [System.Array]) {
    throw "NdtBundleRuntimeState.mills must be a JSON object keyed by PO|mill (not null/array)."
}

$sourceRuntimeSlotCount = Get-PropertyCount $millsProp.Value
$sourceRuntimeSlotsForSelected = 0
foreach ($p in $millsProp.Value.PSObject.Properties) {
    foreach ($m in $MillNo) {
        if ($p.Name -match ('\|' + [string]$m + '$')) {
            $sourceRuntimeSlotsForSelected++
            break
        }
    }
}

$printerRoot = $null
if (Test-Path -LiteralPath $SourcePrinterSettingsFile) {
    try {
        $printerRoot = (Get-Content -LiteralPath $SourcePrinterSettingsFile -Raw -Encoding UTF8) | ConvertFrom-Json
    }
    catch {
        throw "Printer settings source is not valid JSON ($SourcePrinterSettingsFile): $($_.Exception.Message)"
    }
    $null = Get-JsonProperty -Object $printerRoot -Name 'mills' -Context 'MillPrinterSettings' -Required
}

$lifecycleRoot = $null
$sourceLifecycleForSelected = 0
if (Test-Path -LiteralPath $SourceLifecycleStateFile) {
    try {
        $lifecycleRoot = (Get-Content -LiteralPath $SourceLifecycleStateFile -Raw -Encoding UTF8) | ConvertFrom-Json
    }
    catch {
        throw "Lifecycle state source is not valid JSON ($SourceLifecycleStateFile): $($_.Exception.Message)"
    }

    # ConvertFrom-Json: [] -> Object[]; single object -> PSCustomObject (NOT an array) — refuse silent [].
    if ($null -eq $lifecycleRoot) {
        throw "PoLifecycleState source deserialized to null ($SourceLifecycleStateFile)."
    }

    if ($lifecycleRoot -isnot [System.Array]) {
        $typeName = $lifecycleRoot.GetType().FullName
        throw "PoLifecycleState source must be a JSON array of entries with millNo. Got $typeName at $SourceLifecycleStateFile. Refusing to write empty [] (would drop Closed phases)."
    }

    foreach ($entry in $lifecycleRoot) {
        if ($null -eq $entry) {
            throw "PoLifecycleState contains a null entry."
        }
        $millEntryProp = Get-JsonProperty -Object $entry -Name 'millNo' -Context 'PoLifecycleState entry' -Required
        $millVal = 0
        if (-not [int]::TryParse([string]$millEntryProp.Value, [ref]$millVal)) {
            throw "PoLifecycleState entry millNo is not an integer: '$($millEntryProp.Value)'."
        }
        if ($MillNo -contains $millVal) {
            $sourceLifecycleForSelected++
        }
    }
}

$writtenRuntimeSlots = 0
$writtenLifecycleEntries = 0

foreach ($m in $MillNo) {
    $millKey = [string]$m
    $destRuntime = Join-Path $dir ("NdtBundleRuntimeState-M{0}.json" -f $m)
    $destPrinters = Join-Path $dir ("MillPrinterSettings-M{0}.json" -f $m)
    $destLifecycle = Join-Path $dir ("PoLifecycleState-M{0}.json" -f $m)

    $newRoot = [ordered]@{
        version         = if ($null -ne (Get-JsonProperty -Object $root -Name 'version' -Context 'runtime').Value) {
            (Get-JsonProperty -Object $root -Name 'version' -Context 'runtime').Value
        } else { 1 }
        updatedUtc      = (Get-Date).ToUniversalTime().ToString('o')
        millMaxSequence = @{}
        mills           = @{}
    }

    if ($null -ne $millMaxProp -and $null -ne $millMaxProp.Value) {
        $maxRow = Get-JsonProperty -Object $millMaxProp.Value -Name $millKey -Context "millMaxSequence"
        if ($null -ne $maxRow) {
            $newRoot.millMaxSequence[$millKey] = [int]$maxRow.Value
        }
    }

    $suffix = '\|' + $millKey + '$'
    foreach ($p in $millsProp.Value.PSObject.Properties) {
        if ($p.Name -match $suffix) {
            $newRoot.mills[$p.Name] = $p.Value
        }
    }

    $writtenRuntimeSlots += $newRoot.mills.Count

    $runtimeJson = $newRoot | ConvertTo-Json -Depth 20
    if ($WhatIfPreference) {
        Write-Host ("[WhatIf] Would write {0} ({1} slots)." -f $destRuntime, $newRoot.mills.Count)
    }
    elseif ($PSCmdlet.ShouldProcess($destRuntime, 'Write mill runtime state')) {
        $runtimeJson | Set-Content -LiteralPath $destRuntime -Encoding UTF8
        Write-Host "Wrote $destRuntime ($($newRoot.mills.Count) slot(s))"
    }

    if ($null -ne $printerRoot) {
        $newPrinters = [ordered]@{ mills = @{} }
        $printerMills = (Get-JsonProperty -Object $printerRoot -Name 'mills' -Context 'MillPrinterSettings' -Required).Value
        $row = Get-JsonProperty -Object $printerMills -Name $millKey -Context "MillPrinterSettings.mills"
        if ($null -eq $row) {
            $row = Get-JsonProperty -Object $printerMills -Name ([string]$m) -Context "MillPrinterSettings.mills"
        }
        if ($null -ne $row) {
            $newPrinters.mills[$millKey] = $row.Value
        }
        $printerJson = $newPrinters | ConvertTo-Json -Depth 10
        if ($WhatIfPreference) {
            Write-Host "[WhatIf] Would write $destPrinters"
        }
        elseif ($PSCmdlet.ShouldProcess($destPrinters, 'Write mill printer settings')) {
            $printerJson | Set-Content -LiteralPath $destPrinters -Encoding UTF8
            Write-Host "Wrote $destPrinters"
        }
    }
    else {
        Write-Warning "Printer settings source not found; skipping MillPrinterSettings-M$m.json."
    }

    if ($null -ne $lifecycleRoot) {
        $filtered = @()
        foreach ($entry in $lifecycleRoot) {
            $millEntryProp = Get-JsonProperty -Object $entry -Name 'millNo' -Context 'PoLifecycleState entry' -Required
            if ([int]$millEntryProp.Value -eq $m) {
                $filtered += $entry
            }
        }
        $writtenLifecycleEntries += $filtered.Count

        if ($filtered.Count -eq 0) {
            $lifeJson = '[]'
        }
        else {
            $lifeJson = ($filtered | ConvertTo-Json -Depth 10)
            if ($filtered.Count -eq 1 -and $lifeJson.TrimStart().StartsWith('{')) {
                # ConvertTo-Json collapses single-element arrays to a bare object — wrap back.
                $lifeJson = "[$lifeJson]"
            }
        }

        if ($WhatIfPreference) {
            Write-Host ("[WhatIf] Would write {0} ({1} entr(y/ies))." -f $destLifecycle, $filtered.Count)
        }
        elseif ($PSCmdlet.ShouldProcess($destLifecycle, 'Write mill lifecycle state')) {
            $lifeJson | Set-Content -LiteralPath $destLifecycle -Encoding UTF8
            Write-Host "Wrote $destLifecycle ($($filtered.Count) entr(y/ies))"
        }
    }
}

if ($writtenRuntimeSlots -ne $sourceRuntimeSlotsForSelected) {
    throw ("Runtime split count mismatch: source had {0} slot(s) for selected mill(s), wrote {1}. Aborting." -f `
        $sourceRuntimeSlotsForSelected, $writtenRuntimeSlots)
}

if ($null -ne $lifecycleRoot -and $writtenLifecycleEntries -ne $sourceLifecycleForSelected) {
    throw ("Lifecycle split count mismatch: source had {0} entr(y/ies) for selected mill(s), wrote {1}. Aborting." -f `
        $sourceLifecycleForSelected, $writtenLifecycleEntries)
}

Write-Host ("Done. Validated runtime slots in/out={0}, lifecycle entries in/out={1}. Source monolith files were not deleted." -f `
    $writtenRuntimeSlots, $writtenLifecycleEntries)
