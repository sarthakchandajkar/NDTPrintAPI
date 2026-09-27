# Five-instance NdtBundleService deployment

Five Windows Services and five content roots. Process isolation keeps a Mill-N crash from taking down other mills. **Binary isolation** (versioned `releases\<id>\`) lets you deploy **one** service without stopping the others.

## Folder layout

```
C:\Apps\NdtBundleService\
  releases\
    1a0b3ed\                    # immutable publish (git short SHA or timestamp)
    f815e5b\                    # previous build (rollback target)
  bin\                          # optional legacy shared folder — avoid for day-to-day updates
  instances\
    shared\appsettings.Production.json + Logs\
    mill-1\appsettings.Production.json + Logs\
    mill-2\...
    mill-3\...
    mill-4\...
```

Each service `binPath` points at **its own** release folder + `--contentRoot` for that role. Shared can stay on `releases\f815e5b` while Mill-1 runs `releases\1a0b3ed`.

Templates live in repo under `deploy/instances/{shared,mill-1..4}/`. Copy them into the server `instances\` folders and replace `REPLACE_WITH_UNC` paths.

## Pre-requisites (SQL)

1. `docs/App_Setting_AddTable.sql` — shared ZPL print toggle
2. `docs/Mill_Instance_Lease.sql` — exclusive mill lease
3. Fill-to-target schema already applied (`docs/NDT_Bundle_Alter_CsvFill.sql`)
4. `docs/Bundle_Accumulation_AddTable.sql`, `docs/Po_Lifecycle_AddTable.sql`, `docs/Printer_AddTable.sql`
5. `docs/Mill_Instance_Status_AddTable.sql` — mill live PLC snapshot for Shared dashboard tiles (PO-end/print stay on each mill)
6. When shipping Running_Po columns: `docs/Mill_Instance_Status_Alter_RunningPo.sql` **before** restarting mills on that build

Shared `appsettings` must include `NdtBundle:MillInstanceProxy` (Enabled + BaseUrls to `:5001`–`:5004`) so Settings PLC connect / disconnect / test PO-change / open-accumulation forward to the mill that owns S7.

## Mill state is SQL (no JSON split)

`Split-MillStateFiles.ps1` is deleted. Open remainder is `Bundle_Accumulation`; PO drain/closed is `Po_Lifecycle`; printers are `Printer` (seeded `192.168.0.125:9100`). Delete leftover `NdtBundleRuntimeState*.json`, `PoLifecycleState*.json`, and `MillPrinterSettings*.json` or mill/shared startup throws.

## First-time install (all five)

```powershell
cd C:\Users\sarthak\source\repos\NDTPrintAPI
$sha = git rev-parse --short HEAD
$rel = "C:\Apps\NdtBundleService\releases\$sha"
dotnet publish src\NdtBundleService\NdtBundleService.csproj -c Release -o $rel
# copy deploy\instances\* into C:\Apps\NdtBundleService\instances\ (once)
.\scripts\Install-NdtBundleInstances.ps1 -BasePath C:\Apps\NdtBundleService -ReleasePath $rel
Start-Service NdtBundleService-Shared
Start-Service NdtBundleService-M1,NdtBundleService-M2,NdtBundleService-M3,NdtBundleService-M4
```

Recovery: `sc failure … reset=86400 actions=restart/60000/60000/60000` (set by the install script).

## Single-service deploy (preferred)

Does **not** stop other mills or Shared. Publishes to a new folder (nothing locks it), retargets one service, restarts only that service.

```powershell
cd C:\Users\sarthak\source\repos\NDTPrintAPI
# Mill-1 only:
.\scripts\Deploy-NdtBundleInstance.ps1 -BasePath C:\Apps\NdtBundleService -Instance M1

# Shared only:
.\scripts\Deploy-NdtBundleInstance.ps1 -BasePath C:\Apps\NdtBundleService -Instance Shared

# Retarget to an existing publish without rebuilding:
.\scripts\Deploy-NdtBundleInstance.ps1 -BasePath C:\Apps\NdtBundleService -Instance M1 `
  -SkipPublish -ReleasePath C:\Apps\NdtBundleService\releases\1a0b3ed
```

`-Instance` accepts `Shared`, `M1`…`M4`, `mill-1`…`mill-4`, or `1`…`4`.

Verify:

```powershell
sc.exe qc NdtBundleService-M1
Get-Service NdtBundleService-Shared,NdtBundleService-M1,NdtBundleService-M2,NdtBundleService-M3,NdtBundleService-M4
```

**Do not** `dotnet publish -o C:\Apps\NdtBundleService\bin` while any service still points at that folder — Windows will lock the DLL.

## Config checklist per role

| Instance | Port | Workers | Import / Upload / SAP status | Lease |
|---|---|---|---|---|
| Shared | `:5000` | none | yes | no |
| Mill-n | `127.0.0.1:500n` | owned mill only | no | claims mill n |

## Shared ZPL physical-print toggle

Shared + mill instances read/write `App_Setting` key for physical ZPL print via `SqlZplGenerationToggle`.
Each process caches the flag for **~2 seconds**. After flipping print off/on from the Shared dashboard,
expect up to ~2s before a mill instance observes the change (bounded, deliberate). Monolith mode still uses
the in-memory toggle (no cross-process cache).

## First-startup log checks

- Shared: no `PlcHandshakeWorker` / `SlitMonitoringWorker`; Serilog tag `[Shared/-]`
- Mill-n: `Claimed Mill_Instance_Lease for mill n`; `Fill-to-target cutover check passed (mill n)` (if guard on); Serilog `[Mill/n]`
- Second mill process for same mill: fatal `Mill_Instance_Lease claim failed … already owned`

## Rollback (one service)

```powershell
.\scripts\Deploy-NdtBundleInstance.ps1 -BasePath C:\Apps\NdtBundleService -Instance M1 `
  -SkipPublish -ReleasePath C:\Apps\NdtBundleService\releases\<previous-sha>
```

Full five-instance / monolith rollback: stop all five, restore a known-good release path on each (or legacy `bin\`), then start Shared → mills. Optionally `DELETE FROM dbo.Mill_Instance_Lease` (or wait for TTL).

Never run the old monolith and a mill instance against **production** for the same mill. Lease claim fails startup if another live holder exists.

## Non-prod Mill-1 gate (required before production)

See [VALIDATION-MILL1-NONPROD.md](./VALIDATION-MILL1-NONPROD.md).

## Dashboard gaps

See [DASHBOARD-MULTI-INSTANCE-GAPS.md](./DASHBOARD-MULTI-INSTANCE-GAPS.md).

## Follow-up (this release): station printers

All ZPL printers live in `dbo.Printer`. Mill tags use `MILL_1`…`MILL_4`. Station tags use Shared-only writes of three station keys — three rows:

| Key | Physical point | Workflows |
|---|---|---|
| `VISUAL_REVISUAL` | A (Visual and Revisual, same printer) | Visual, Revisual |
| `BIG_HYDRO` | B | BigHydrotesting (legacy `Hydrotesting` also maps here with a warning) |
| `FOUR_HEAD_HYDRO` | C | FourHeadHydrotesting |

**Behaviour change:** `ManualNdtTagService` used to print via `ResolveForMill(state.MillNo)` (bundle mill). It now resolves by station. A Mill-2 bundle at Visual prints at point A, not Mill-2.

Seed is `192.168.0.125:9100` on all `Printer` rows until real IPs are saved. Missing/empty station row fails with `Printer not configured for Visual/Revisual` (never a mill or another station). ManualTags stays Shared-only.
