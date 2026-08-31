# Five-instance NdtBundleService deployment

One binary, five Windows Services, five content roots. Prevents a Mill-N crash from taking down the other mills' SAP/NDT path.

## Folder layout

```
C:\Apps\NdtBundleService\
  bin\                          # single publish output (NdtBundleService.exe + deps)
  instances\
    shared\appsettings.Production.json + Logs\
    mill-1\appsettings.Production.json + Logs\
    mill-2\...
    mill-3\...
    mill-4\...
```

Templates live in repo under `deploy/instances/{shared,mill-1..4}/`. Copy them into the server `instances\` folders and replace `REPLACE_WITH_UNC` paths.

## Pre-requisites (SQL)

1. `docs/App_Setting_AddTable.sql` — shared ZPL print toggle
2. `docs/Mill_Instance_Lease.sql` — exclusive mill lease
3. Fill-to-target schema already applied (`docs/NDT_Bundle_Alter_CsvFill.sql`)
4. `docs/Bundle_Accumulation_AddTable.sql`, `docs/Po_Lifecycle_AddTable.sql`, `docs/Mill_Printer_AddTable.sql`, `docs/Station_Printer_AddTable.sql`

## Mill state is SQL (no JSON split)

`Split-MillStateFiles.ps1` is deleted. Open remainder is `Bundle_Accumulation`; PO drain/closed is `Po_Lifecycle`; printers are `Mill_Printer` (seeded `192.168.0.125:9100`). Delete leftover `NdtBundleRuntimeState*.json`, `PoLifecycleState*.json`, and `MillPrinterSettings*.json` or mill/shared startup throws.

## Install / update services

```powershell
dotnet publish src\NdtBundleService\NdtBundleService.csproj -c Release -o C:\Apps\NdtBundleService\bin
# copy deploy\instances\* into C:\Apps\NdtBundleService\instances\
.\scripts\Install-NdtBundleInstances.ps1 -BasePath C:\Apps\NdtBundleService
```

Recovery: `sc failure … reset=86400 actions=restart/60000/60000/60000` (set by the script).

## Update order (mandatory)

1. Stop mills: `NdtBundleService-M1` … `M4`
2. Stop Shared: `NdtBundleService-Shared`
3. Replace `bin\`
4. Start Shared
5. Start mills

Never run the old monolith and a mill instance against **production** for the same mill. Lease claim fails startup if another live holder exists.

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

## Rollback

1. Stop all five new services
2. Restore previous `bin\` (monolith publish) and original monolith contentRoot / single service
3. Start monolith
4. Optionally `DELETE FROM dbo.Mill_Instance_Lease` (or wait for TTL expiry)

## Non-prod Mill-1 gate (required before production)

See [VALIDATION-MILL1-NONPROD.md](./VALIDATION-MILL1-NONPROD.md).

## Dashboard gaps

See [DASHBOARD-MULTI-INSTANCE-GAPS.md](./DASHBOARD-MULTI-INSTANCE-GAPS.md).

## Follow-up (this release): station printers

`Mill_Printer` stays mills 1–4. Station tags use Shared-only `dbo.Station_Printer`, keyed by station code — three rows:

| Code | Physical point | Workflows |
|---|---|---|
| `VISUAL_REVISUAL` | A (Visual and Revisual, same printer) | Visual, Revisual |
| `BIG_HYDRO` | B | BigHydrotesting (legacy `Hydrotesting` also maps here with a warning) |
| `FOUR_HEAD_HYDRO` | C | FourHeadHydrotesting |

**Behaviour change:** `ManualNdtTagService` used to print via `ResolveForMill(state.MillNo)` (bundle mill). It now resolves by station. A Mill-2 bundle at Visual prints at point A, not Mill-2.

Seed is `192.168.0.125:9100` on all three until real IPs are saved. Missing/empty station row fails with `Printer not configured for Visual/Revisual` (never a mill or another station). ManualTags stays Shared-only.
