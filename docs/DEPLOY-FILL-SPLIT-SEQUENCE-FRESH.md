# Fresh deploy: fill-to-target + five instances + Mill_Sequence + mill-state SQL

Deploy **this binary** as `9377bca`. This is a **full reset**. SAP has not switched pickup folders. Do not preserve NDT operational data.

| Commit | What it is |
|---|---|
| `69d0d75` | **Pre-deployment / rollback target** (last production-safe monolith before this stack) |
| `a91efbd` | Fill-to-target CSV assignment |
| `d22b620` | Five Windows Services (Shared + Mill-1..4), mill lease |
| `9e79619` | `Mill_Sequence` as close numbering; write-then-clear; bundle merge columns |
| `ba19b00` | Stop writing unused CSV and ZPL files to the NDT Bundles folder |
| `e38fc56` | MES PAS NDT upload CSVs per batch after Revisual (not a 12-hour timer) |
| `2f5cbdb` | Fail closed on F-5 SQL coverage; hold backfill that would stamp past a terminal fill |
| `298f328` | Complete five-instance Production configs; skip leftover SAP slits without moving source folders |
| `9377bca` | Mill remainder, PO lifecycle, and mill printers in SQL (this binary) |

Share root used below:

`\\10.2.20.210\pas-sap\`

UNC folder `NDT Input Slit` means:

`\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Input Slit`

Database: `JazeeraMES_Prod` on `AJS-SOH-VM-PAS-\SQLEXPRESS`.

Service install root: `C:\Apps\NdtBundleService\` (`bin\` + `instances\`).

`Split-MillStateFiles.ps1` is **deleted**. There is no JSON split and no migration. Leftover mill-state JSON **throws on startup**.

These keys are **gone** from the five Production files (do not add them back): `EnableNdtBundleRuntimeStatePersistence`, `NdtBundleRuntimeStateFile`, `MillPrinterSettingsFile`, `RuntimeStatePruning`.

---

## Numbered checklist

### 1. Backup and rollback prep

1. Confirm current production binary is still **`69d0d75`** (or note the exact commit/folder you will restore). Keep a copy of that publish folder, e.g. `C:\Apps\NdtBundleService\bin-69d0d75\`.
2. Full backup of `JazeeraMES_Prod` (schema + data). Name it with the date. This is the rollback image. Do not skip it.
3. Snapshot (copy aside, do not delete yet) of:
   - `C:\Apps\NdtBundleService\` (current monolith `bin` + content root / `appsettings`)
   - Windows Service name/binPath of the running monolith (`sc.exe qc NdtBundleService` or whatever name is installed)
4. Stop mill work: no new slits, no PO-end, no dashboard reconcile. Then stop the **current monolith** Windows Service so it cannot write during reset.

Rollback later = restore this backup + this `bin` + the old single service (step 9).

---

### 2. Complete reset (SQL + folders + state files)

#### 2.1 Tables to **clear** (operational / generated)

Delete in this order (FK: `Output_Slit_Row.NDT_Batch_No` → `NDT_Bundle.Bundle_No`). If a table does not exist yet, skip it.

```sql
USE JazeeraMES_Prod;
GO

-- Child / generated first
DELETE FROM dbo.Output_Slit_Row;
DELETE FROM dbo.Output_Slit_Sap_Status_Event;
DELETE FROM dbo.Output_Slit_Sap_Status;
DELETE FROM dbo.Ppc_Correction_Item;
DELETE FROM dbo.NDT_Csv_Fill_Hold;
DELETE FROM dbo.NDT_Csv_Fill_Audit;
DELETE FROM dbo.Input_Slit_Row;
DELETE FROM dbo.Input_Slit_File_Seen;
DELETE FROM dbo.Handshake_Event;
DELETE FROM dbo.Manual_Station_Run;
DELETE FROM dbo.NDT_Process_Consolidated;
DELETE FROM dbo.Upload_Bundle_Row;
DELETE FROM dbo.Pipeline_Event;
DELETE FROM dbo.Bundle_Label;
DELETE FROM dbo.NDT_Bundle;

-- Sequence / lease (re-seeded by scripts / first claim)
IF OBJECT_ID(N'dbo.Mill_Sequence_Audit', N'U') IS NOT NULL
    DELETE FROM dbo.Mill_Sequence_Audit;
IF OBJECT_ID(N'dbo.Mill_Sequence', N'U') IS NOT NULL
    DELETE FROM dbo.Mill_Sequence;
IF OBJECT_ID(N'dbo.Mill_Instance_Lease', N'U') IS NOT NULL
    DELETE FROM dbo.Mill_Instance_Lease;

-- Mill-state SQL (re-seeded: Mill_Printer / Station_Printer in step 3; accumulation/lifecycle stay empty)
IF OBJECT_ID(N'dbo.Bundle_Accumulation', N'U') IS NOT NULL
    DELETE FROM dbo.Bundle_Accumulation;
IF OBJECT_ID(N'dbo.Bundle_Accumulation_Context', N'U') IS NOT NULL
    DELETE FROM dbo.Bundle_Accumulation_Context;
IF OBJECT_ID(N'dbo.Po_Lifecycle_Audit', N'U') IS NOT NULL
    DELETE FROM dbo.Po_Lifecycle_Audit;
IF OBJECT_ID(N'dbo.Po_Lifecycle', N'U') IS NOT NULL
    DELETE FROM dbo.Po_Lifecycle;
IF OBJECT_ID(N'dbo.Mill_Printer', N'U') IS NOT NULL
    DELETE FROM dbo.Mill_Printer;
IF OBJECT_ID(N'dbo.Station_Printer', N'U') IS NOT NULL
    DELETE FROM dbo.Station_Printer;

-- PO plan will be re-imported by Shared from PO Accepted
DELETE FROM dbo.PO_Plan_WIP;
GO
```

Optional identity reset after delete (keeps IDs starting at 1):

```sql
DBCC CHECKIDENT ('dbo.NDT_Bundle', RESEED, 0);
```

#### 2.2 Tables to **leave alone**

| Table | Why |
|---|---|
| `dbo.Formation_Chart` | Master pipe-size → pcs/bundle. Not NDT run history. |
| `dbo.App_Setting` | Created in step 3. After seed, leave `ZplPhysicalPrintEnabled`. If the table does not exist yet, do not create it here — the script in step 3 does. |

Do **not** drop `NDT_Bundle` / `Input_Slit_Row` / etc. You are emptying rows, not removing the base schema.

#### 2.3 Folders — SAP source vs MES output

**Do not move, delete, or empty** SAP source folders. The service only **reads** them (never writes, moves, or deletes):

| Folder | Why leave it |
|---|---|
| `\\10.2.20.210\pas-sap\To SAP\TM\Input Slit` | SAP slit inbox. Mill ingest source. Skip leftovers with mill `MinSourceFileLastWriteUtc` (step 2.5). |
| `\\10.2.20.210\pas-sap\To SAP\TM\Input Slit Accepted` | SAP-accepted slit copies. Dashboard / running-PO read only — **not** mill ingest. |
| `\\10.2.20.210\pas-sap\From SAP\TMFG_TMWIP\PO Accepted` | Shared re-imports `PO_Plan_WIP` from here |
| `\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Input Slit\FormationChart.csv` | Thresholds |
| `\\10.2.20.210\pas-sap\To SAP\TM\Bundle` and `Bundle Accepted` | WIP bundle files (PO running / mill-4 file PO-end) |

Optional: archive **MES-written** NDT outputs (move, do not delete) into `\\10.2.20.210\pas-sap\To SAP\TM\_archive\ndt-reset-YYYYMMDD\` if you want a clean pickup folder. Skip this if those folders are also SAP-owned / read-only.

| Folder (production path) | What it is |
|---|---|
| `\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Input Slit\Input Slit` | MES output CSVs (SAP pickup — not switched yet) |
| `\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Input Slit\NDT Input Slit Accepted` | SAP-accepted NDT outputs |
| `\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Input Slit\NDT Input Slit Rejected` | SAP-rejected NDT outputs |
| `\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Bundles` | Bundle summary CSVs (this binary no longer writes them) |
| `\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Final Output\Bundle` | NDT process CSVs |
| `\\10.2.20.210\pas-sap\To SAP\TM\NDT\MES PAS NDT\Bundle` | Upload-to-SAP bundle CSVs |

Do **not** recreate `Input Slit` or `Input Slit Accepted` as empty.

#### 2.4 State files to **delete** (mandatory — leftover JSON throws)

`9377bca` does **not** read mill-state JSON. If any of these files exist under `OutputBundleFolder` (`…\NDT Input Slit\Input Slit`) **or its parent** (`…\NDT Input Slit`), **every** instance (Shared and Mill-1..4) fails startup with:

`Leftover mill-state JSON found (fresh reset required, no migration): …`

Delete **all three families**. Not optional cleanup.

All under `\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Input Slit\` and `…\NDT Input Slit\Input Slit\`, plus any copy next to the old monolith content root:

| File | Must delete |
|---|---|
| `NdtBundleRuntimeState.json` | **Mandatory** (monolith leftover) |
| `NdtBundleRuntimeState-M1.json` … `M4.json` | **Mandatory** (prior split attempt) |
| `PoLifecycleState.json` | **Mandatory** |
| `PoLifecycleState-M1.json` … `M4.json` | **Mandatory** |
| `MillPrinterSettings.json` | **Mandatory** |
| `MillPrinterSettings-M1.json` … `M4.json` | **Mandatory** |
| `ManualStationState\` (entire folder) | Yes if present (`EnableManualStationStateFiles` is false; delete anyway) |

Printer IPs live in `dbo.Mill_Printer` (mills 1–4) and Shared-only `dbo.Station_Printer` (three station codes). Step 3 seeds both at `192.168.0.125:9100`. Shared Settings `PUT /api/Settings/printers` writes both; mill-n re-reads mill rows within ~2s. Mill-1 falls back to `NdtTagPrinterAddress` only when its SQL mill row is missing. Mills 2–4 have **no** fallback to another mill's printer. Station tags have **no** mill fallback.

Confirm zero leftovers before start:

```powershell
$roots = @(
  '\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Input Slit',
  '\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Input Slit\Input Slit'
)
$names = @(
  'NdtBundleRuntimeState.json',
  'NdtBundleRuntimeState-M1.json','NdtBundleRuntimeState-M2.json',
  'NdtBundleRuntimeState-M3.json','NdtBundleRuntimeState-M4.json',
  'PoLifecycleState.json',
  'PoLifecycleState-M1.json','PoLifecycleState-M2.json',
  'PoLifecycleState-M3.json','PoLifecycleState-M4.json',
  'MillPrinterSettings.json',
  'MillPrinterSettings-M1.json','MillPrinterSettings-M2.json',
  'MillPrinterSettings-M3.json','MillPrinterSettings-M4.json'
)
$hit = @()
foreach ($r in $roots) {
  foreach ($n in $names) {
    $p = Join-Path $r $n
    if (Test-Path -LiteralPath $p) { $hit += $p }
  }
}
if ($hit.Count -eq 0) { 'OK: no leftover mill-state JSON' }
else { $hit | ForEach-Object { "STILL EXISTS: $_" } }
```

Must print `OK: no leftover mill-state JSON`. If any `STILL EXISTS`, delete those files and re-run. Do not start services until this is clean.

#### 2.5 Skip leftover SAP slit files (`MinSourceFileLastWriteUtc`)

`Input Slit` and `Input Slit Accepted` stay populated (read-only SAP sources). The mill **never** moves or deletes them.

Ingest reads **only** `Input Slit`. `Input Slit Accepted` is dashboard / running-PO only.

Production `BackfillLookbackHours` is **48**. After the SQL wipe, `Input_Slit_Row` is empty, so any inbox file whose `LastWriteTimeUtc` is within 48 hours **and** on or after `MinSourceFileLastWriteUtc` is queued as a new slit. Leave the mill floor **one second after the newest leftover** in `Input Slit`.

Mill-1..4 files in this document currently use `"MinSourceFileLastWriteUtc": "2026-08-30T12:53:57Z"` (newest leftover at generate time was `12:53:56Z`). **Re-measure immediately before mill start** and bump all four mill files if a newer leftover exists:

```powershell
$newest = (Get-ChildItem '\\10.2.20.210\pas-sap\To SAP\TM\Input Slit' -File |
  Measure-Object LastWriteTimeUtc -Maximum).Maximum
([datetime]$newest).ToUniversalTime().AddSeconds(1).ToString('yyyy-MM-ddTHH:mm:ssZ')
```

Leave Shared `MinSourceFileLastWriteUtc` **empty** so PO Accepted scans are not cut off. Do **not** set `BackfillLookbackHours` to 0 — the code clamps to **minimum 1 hour**.

After mill start, first reconcile line must show `queued for backfill 0` (leftovers counted in `outside lookback/min-write`). If queued is not 0, stop that mill, raise the floor, restart.

`PoPlanImportMinLastWriteUtc` is `2026-06-01T00:00:00Z`. Shared **will** re-import PO Accepted files from that date. That is intended so the first PO has pipe size / NDT pcs. It does not create NDT bundles.

---

### 3. Schema scripts (dependency order)

Run against `JazeeraMES_Prod` in SSMS, **after** the DELETE in step 2.1 so `Mill_Sequence` seeds from an empty `NDT_Bundle` and `Mill_Printer` / `Station_Printer` re-seed after the wipe. All scripts are additive / IF-missing. Skip a file only if you have already applied it in this session.

**Ordering that matters**

- `NDT_Bundle_Alter_CsvFill.sql` **must run before** `NDT_Bundle_Alter_Voided.sql`. CsvFill creates `Csv_Fill_State` and `CK_NDT_Bundle_Csv_Fill_State` **without** `'Voided'`. Voided **drops** that CHECK (if present) and recreates it **with** `'Voided'`. If you reverse them, Voided’s CHECK add fails because `Csv_Fill_State` is missing. If you run CsvFill **after** Voided, CsvFill will **not** replace the CHECK (it only adds when missing) — Voided’s list is the one you want, so CsvFill-then-Voided is required.
- `App_Setting_AddTable.sql` has **no** dependency on the CsvFill CHECK. It can sit anywhere after the database exists. Keep it with the five-instance scripts (before mill start).
- `Mill_Sequence.sql` needs `NDT_Bundle` to exist (already does). After a wipe, seed is `0` for mills 1–4.
- `Ppc_Correction_Item_Alter_ReplacementBatch.sql` needs `Ppc_Correction_Item` (already in prod). Run after that table exists.
- `Mill_Instance_Lease.sql` is independent; needed before Mill-1..4 start.
- `Bundle_Accumulation_AddTable.sql`, `Po_Lifecycle_AddTable.sql`, `Mill_Printer_AddTable.sql`, and `Station_Printer_AddTable.sql` have **no** CsvFill CHECK dependency. Run them **after** `Mill_Sequence` / Voided / Ppc ReplacementBatch (this session’s mill-state block) and **before** first start. `Mill_Printer` seed inserts only missing mill rows — because step 2.1 deleted the table rows, seed recreates 1–4 at `192.168.0.125:9100`. `Station_Printer` seed recreates the three station rows at the same IP (Visual/Revisual share one row).

**A. Already on production at `69d0d75` — run only if a table/column is missing**

1. `docs/NDT_Traceability_Schema.sql` (or the individual `NDT_Bundle_Table.sql` + `NDT_Process_Consolidated_AddTable.sql` if you never ran the full schema)
2. `docs/NDT_Bundle_Alter_PrintStatus.sql`
3. `docs/NDT_Bundle_Alter_ManualReview.sql`
4. `docs/NDT_Bundle_Alter_CloseSource.sql`
5. `docs/NDT_Bundle_Alter_ManualRecon.sql`
6. `docs/Input_Slit_Row_Alter_SourceLastWrite.sql`
7. `docs/Input_Slit_File_Seen_AddTable.sql`
8. `docs/Handshake_Event_AddTable.sql`
9. `docs/Output_Slit_Sap_Status_AddTable.sql`
10. `docs/Ppc_Correction_Item_AddTable.sql`
11. `docs/PO_Plan_WIP_Alter_AddColumns.sql`

**B. This stack — run in this order**

12. `docs/NDT_Bundle_Alter_CsvFill.sql` (`a91efbd`) — columns `Target_Ndt_Pcs`, `Csv_Filled`, `Csv_Fill_State`, `Csv_Last_Row_AtUtc`; CHECK **without** Voided; tables `NDT_Csv_Fill_Hold`, `NDT_Csv_Fill_Audit`; backfill UPDATE is a no-op on empty `NDT_Bundle`.
13. `docs/App_Setting_AddTable.sql` (`d22b620`) — `App_Setting` + row `ZplPhysicalPrintEnabled = true`. Independent of the CHECK.
14. `docs/Mill_Instance_Lease.sql` (`d22b620`) — `Mill_Instance_Lease` (empty until mill claim).
15. `docs/Mill_Sequence.sql` (`9e79619`) — `Mill_Sequence` + `Mill_Sequence_Audit`; insert mills 1–4 with `Current_Sequence = 0` (empty live max).
16. `docs/NDT_Bundle_Alter_Voided.sql` (`9e79619`) — Voided columns; **drop + recreate** `CK_NDT_Bundle_Csv_Fill_State` including `'Voided'`.
17. `docs/Ppc_Correction_Item_Alter_ReplacementBatch.sql` (`9e79619`) — `Replacement_NDT_Batch_No`.
18. `docs/Bundle_Accumulation_AddTable.sql` (`9377bca`) — `Bundle_Accumulation` + `Bundle_Accumulation_Context` (open remainder; CHECK Pcs > 0). No JSON migration.
19. `docs/Po_Lifecycle_AddTable.sql` (`9377bca`) — `Po_Lifecycle` + `Po_Lifecycle_Audit` (Draining/Closed; Running = no row).
20. `docs/Mill_Printer_AddTable.sql` (`9377bca`) — `Mill_Printer`; seed mills 1–4 at `192.168.0.125:9100`, `Updated_By = Seed`.
21. `docs/Station_Printer_AddTable.sql` — `Station_Printer`; seed `VISUAL_REVISUAL`, `BIG_HYDRO`, `FOUR_HEAD_HYDRO` at `192.168.0.125:9100`, `Updated_By = Seed`. Also adds `Print_Status` / `Print_Error` on `Manual_Station_Run`.

Confirm CHECK after 16:

```sql
SELECT definition
FROM sys.check_constraints
WHERE name = N'CK_NDT_Bundle_Csv_Fill_State';
-- must include N'Voided'
```

Confirm mill-state tables after 18–21:

```sql
SELECT name FROM sys.tables
WHERE name IN (
  N'Bundle_Accumulation', N'Bundle_Accumulation_Context',
  N'Po_Lifecycle', N'Po_Lifecycle_Audit', N'Mill_Printer', N'Station_Printer'
)
ORDER BY name;
-- six names

SELECT Mill_No, Address, Port, Updated_By
FROM dbo.Mill_Printer
ORDER BY Mill_No;
-- four rows: 1–4, 192.168.0.125, 9100, Seed

SELECT Station_Code, Address, Port, Updated_By
FROM dbo.Station_Printer
ORDER BY Station_Code;
-- three rows: BIG_HYDRO, FOUR_HEAD_HYDRO, VISUAL_REVISUAL; 192.168.0.125, 9100, Seed
```

---

### 4. No JSON split (`Split-MillStateFiles.ps1` is deleted)

Do **not** look for or run `Split-MillStateFiles.ps1`. It is gone from the repo.

Open remainder is `Bundle_Accumulation`. PO drain/closed is `Po_Lifecycle`. Mill printers are `Mill_Printer`. Station printers are `Station_Printer` (Shared-only; Visual and Revisual share `VISUAL_REVISUAL`).

Step 2.4 must already have printed `OK: no leftover mill-state JSON`. If a service starts and you see `Leftover mill-state JSON found`, stop it, delete the named files, start again. There is no import path from those JSON files into SQL.

---

### 5. Publish, instance config, `Install-NdtBundleInstances.ps1`

1. Publish `9377bca`:

   ```powershell
   dotnet publish src\NdtBundleService\NdtBundleService.csproj -c Release -o C:\Apps\NdtBundleService\bin
   ```

2. Create `C:\Apps\NdtBundleService\instances\{shared,mill-1,mill-2,mill-3,mill-4}\` (and a `Logs\` folder in each). Paste the five complete `appsettings.Production.json` files from the end of this document (sections 10.1–10.5). Do not merge with old monolith `appsettings`. Confirm none of these keys appear: `EnableNdtBundleRuntimeStatePersistence`, `NdtBundleRuntimeStateFile`, `MillPrinterSettingsFile`, `RuntimeStatePruning`.

3. Confirm overlays (already in those files):

   | Instance | Port | Role |
   |---|---|---|
   | Shared | `http://*:5000` | Dashboard, PO import, SAP-status watcher, upload scheduler. `RequireCleanFillCutover=false`. No mill workers. `MinSourceFileLastWriteUtc` empty. |
   | Mill-n | `http://127.0.0.1:500n` | Workers for mill n only. `RequireCleanFillCutover=true`. Owned mill only. |

4. Mill-1 `MillCsvBatchMode` = FillToTarget. Mills 2–4 = Constant `10001`. Do not change for this cutover.

5. Service account must have UNC read/write and SQL Windows login to `JazeeraMES_Prod`. Mapped `Z:\` is invisible to Local System — UNC only.

6. As Administrator:

   ```powershell
   .\scripts\Install-NdtBundleInstances.ps1 -BasePath C:\Apps\NdtBundleService
   ```

   Creates/updates `NdtBundleService-Shared`, `NdtBundleService-M1`…`M4`, recovery restart 60s/60s/60s. **Does not start them.**

7. Disable or leave stopped the **old monolith** service so it cannot claim a mill. Two processes on the same mill = fatal lease fail.

8. Dashboard: point the UI at Shared `http://<host>:5000`. Mill ports 5001–5004 stay localhost-only.

---

### 6. Staged startup (mandatory order)

Start **one**, confirm logs, then the next. Do not start mills before Shared. Do not start M2 before M1 is green (lease/SQL/cutover). Logs:

Each mill instance writes to `C:\Apps\NdtBundleService\instances\mill-n\Logs\` (`ndtbundle-m{n}-.log`). Shared writes to `instances\shared\Logs\`.

Line prefix: `[Shared/-]` or `[Mill/n]`.

#### 6.1 Shared — `Start-Service NdtBundleService-Shared`

Must see:

- `SQL traceability configured for Server=… Database=JazeeraMES_Prod`
- `SQL traceability connected to …, database JazeeraMES_Prod`
- `SQL traceability table NDT_Bundle: 0 row(s).`
- `SQL traceability table Mill_Sequence: 4 row(s).`
- `SQL traceability table Bundle_Accumulation: 0 row(s).`
- `SQL traceability table Bundle_Accumulation_Context: 0 row(s).`
- `SQL traceability table Po_Lifecycle: 0 row(s).`
- `SQL traceability table Mill_Printer: 4 row(s).`  ← mill printer seed
- `SQL traceability table Station_Printer: 3 row(s).`  ← station printer seed (Visual/Revisual, Big Hydro, Four-Head Hydro)
- `NdtBundle:MinSourceFileLastWriteUtc floor is none.`
- `PO_Plan_WIP folder import starting (folder \\10.2.20.210\pas-sap\From SAP\TMFG_TMWIP\PO Accepted; …)`
- `PO_Plan_WIP folder import finished: scanned …`
- `PO plan caches warmed on startup.`
- `NDT Input Slit SAP status watcher active. Pending: …\NDT Input Slit\Input Slit; Accepted: …; Rejected: …`

Must **not** see:

- `Leftover mill-state JSON found`
- `SQL traceability tables missing … Bundle_Accumulation` / `Po_Lifecycle` / `Mill_Printer` / `Station_Printer`
- `SQL traceability columns missing … Manual_Station_Run.Print_Status` / `Manual_Station_Run.Print_Error`
- `PlcHandshakeWorker starting`
- `SlitMonitoringWorker started`
- `Claimed Mill_Instance_Lease`
- `Fill-to-target cutover blocked`
- `PoLifecycleSweepWorker started`

If `SQL traceability tables missing` names `Mill_Sequence`, `Bundle_Accumulation`, `Po_Lifecycle`, `Mill_Printer`, or `Station_Printer`, stop and finish step 3.

If `SQL traceability columns missing` names `Manual_Station_Run.Print_Status` or `Manual_Station_Run.Print_Error`, the ALTER in script 21 did not run. Re-run `docs/Station_Printer_AddTable.sql` (the table CREATE can already have succeeded) and restart Shared.

If `Mill_Printer` is `0 row(s)`, the seed INSERT did not run (table existed empty after DELETE, script skipped CREATE, but INSERT of missing mills should still run). Re-run `docs/Mill_Printer_AddTable.sql` and restart Shared.

If `Station_Printer` is `0 row(s)`, re-run `docs/Station_Printer_AddTable.sql` and restart Shared. Station tags have no mill fallback — a missing row skips the tag.

#### 6.2 Mill-1 — `Start-Service NdtBundleService-M1`

Must see (`[Mill/1]`):

- Same SQL connected lines (or at least no “not reachable”)
- `SQL traceability table Bundle_Accumulation: 0 row(s).`
- `SQL traceability table Po_Lifecycle: 0 row(s).`
- `SQL traceability table Mill_Printer: 4 row(s).`
- `SQL traceability table Station_Printer: 3 row(s).`
- `NdtBundle:MinSourceFileLastWriteUtc floor is 2026-08-30T12:53:57.0000000Z.` (or the re-measured floor)
- `Mill_Sequence mill 1 seeded Current_Sequence=…` **only if** the row was missing. After step 3 seed, the row exists → you will **not** see `seeded`; that is OK.
- `Mill_Sequence startup guard passed (mill 1)`
- `Fill-to-target cutover check passed (mill 1)`
- `Claimed Mill_Instance_Lease for mill 1 (InstanceId=…, TTL=45s).`
- `PoLifecycleSweepWorker started (Plc drain expiry + orphan sweep).`
- `PlcHandshakeWorker starting 1 mill loop(s) (default poll 500ms).`
- `Mill-1 (Mill 1): PoEndSource=Plc — …`
- `No TcpOpen mills configured — TCP transport idle.`
- `SlitMonitoringWorker started. Watching folder \\10.2.20.210\pas-sap\To SAP\TM\Input Slit`
- `Input Slit reconcile: … queued for backfill 0 …`

There is **no** `NDT runtime state initialized (… JSON …)` line. Remainder hydrates from `Bundle_Accumulation` (silent when empty).

Must **not** see:

- `Leftover mill-state JSON found`
- `SQL traceability tables missing`
- `SQL traceability columns missing`
- `Fill-to-target cutover blocked`
- `Bundle_Accumulation has open size-count rows`
- `Mill_Sequence for mill 1 is …; live bundles go to …`
- `Mill_Instance_Lease claim failed for mill 1: already owned`
- `bundle close failed: could not allocate sequence`

If the old monolith is still running, lease claim fails. Stop the monolith, wait 45s or `DELETE FROM dbo.Mill_Instance_Lease`, start M1 again.

#### 6.3 Mill-2 — `Start-Service NdtBundleService-M2`

Same as M1 with mill **2**, plus:

- `PlcHandshake skipped for Mill-2 (Mill 2): PlcHandshakeEnabled=false — no S7 connection (PoEndSource=Plc).`

(`PlcHandshake.Enabled` is true on the mill template so the worker starts, then skips S7 because `PlcHandshakeEnabled=false`.)

Guard + cutover + lease for mill 2. `Mill_Printer: 4 row(s).` `Station_Printer: 3 row(s).` Reconcile queued 0.

#### 6.4 Mill-3 — `Start-Service NdtBundleService-M3`

Same pattern, mill **3**, handshake skipped (`PlcHandshakeEnabled=false`, `PoEndSource=Plc`).

#### 6.5 Mill-4 — `Start-Service NdtBundleService-M4`

Same pattern, mill **4**:

- Handshake skipped (`PlcHandshakeEnabled=false`, `PoEndSource=File`)
- `No TcpOpen mills configured — TCP transport idle.` (`PoEndSource=File`; `TcpOpenPort` on the mill entry is unused.) File PO-end is enough for mill 4.

---

### 7. Post-reset `Mill_Sequence` and `Mill_Printer` verification (before first close)

```sql
USE JazeeraMES_Prod;
GO

DECLARE @yy CHAR(2) = RIGHT(CONVERT(CHAR(4), YEAR(GETDATE())), 2);

SELECT
    s.Mill_No,
    s.Current_Sequence,
    s.Updated_By,
    s.Reason,
    live.LiveMax,
    live.LiveMaxBundleNo
FROM dbo.Mill_Sequence s
OUTER APPLY (
    SELECT
        MAX(TRY_CONVERT(int, RIGHT(b.Bundle_No, 5))) AS LiveMax,
        MAX(b.Bundle_No) AS LiveMaxBundleNo
    FROM dbo.NDT_Bundle b
    WHERE b.Mill_No = s.Mill_No
      AND LEN(b.Bundle_No) = 10
      AND LEFT(b.Bundle_No, 2) = '12'
      AND SUBSTRING(b.Bundle_No, 3, 2) = @yy
      AND b.Total_NDT_Pcs > 0
      AND ISNULL(b.Voided, 0) = 0
) live
ORDER BY s.Mill_No;

SELECT COUNT(*) AS AccRows FROM dbo.Bundle_Accumulation;
SELECT COUNT(*) AS CtxRows FROM dbo.Bundle_Accumulation_Context;
SELECT COUNT(*) AS LifeRows FROM dbo.Po_Lifecycle;

SELECT Mill_No, Address, Port, Updated_By
FROM dbo.Mill_Printer
ORDER BY Mill_No;

SELECT Station_Code, Address, Port, Updated_By
FROM dbo.Station_Printer
ORDER BY Station_Code;
```

**Clean result after this reset (year 2026):**

| Mill_No | Current_Sequence | LiveMax | LiveMaxBundleNo |
|---|---|---|---|
| 1 | 0 | NULL | NULL |
| 2 | 0 | NULL | NULL |
| 3 | 0 | NULL | NULL |
| 4 | 0 | NULL | NULL |

Four `Mill_Sequence` rows, nothing else. `Updated_By` is `Migration` (SQL seed) or `StartupSeed` (app inserted a missing row).

`AccRows` = 0, `CtxRows` = 0, `LifeRows` = 0.

`Mill_Printer`: four rows, Address `192.168.0.125`, Port `9100`, `Updated_By` `Seed` (until an operator saves Settings).

`Station_Printer`: three rows (`VISUAL_REVISUAL`, `BIG_HYDRO`, `FOUR_HEAD_HYDRO`), same seed address. Visual and Revisual share one row. Station tags print at the inspection point, not the bundle mill.

First close on mill *n* is sequence **1**, tag `12` + `26` + mill digit + `00001` (mill 1: `1226100001`).

If `Current_Sequence` is not 0, **do not close**. You seeded before the DELETE, or leftover bundles remain. Fix table, then start.

If `Mill_Printer` is missing a mill, **do not close** that mill expecting a tag. Re-run script 20. Mills 2–4 with no row log `PrintFailed` / `no printer configured for mill N` (no fall-back). Mill-1 missing row uses `NdtTagPrinterAddress`.

If `Station_Printer` is missing a code, station tags for that point will not print. Re-run script 21. No mill fallback.

---

### 8. First-PO end-to-end

Pick a mill that will actually run (typically mill 1). Confirm `PO_Plan_WIP` has that PO + mill + pipe size (Shared import). Formation chart has that size.

1. Drop **one** new Input Slit file for that mill/PO into `Input Slit` (after lookback/archive is clean).
2. Mill log: slit ingested; remainder persists in `Bundle_Accumulation` (`Pcs > 0`); no allocate until threshold (mill 1 fill-to-target) or PLC slit-end (mill 1 handshake).
3. On first successful close:
   - `Mill_Sequence.Current_Sequence` for that mill = **1**
   - `NDT_Bundle` one row, `Bundle_No` = `1226n00001`, `Print_Status` = `Printed` (or `Pending`/`PrintFailed` if printer down — SQL row must still exist)
   - `Target_Ndt_Pcs` set on mill 1; mills 2–4 output batch `10001` (constant)
   - Tag at that mill’s `Mill_Printer` address (seed `192.168.0.125:9100`)
   - That mill/PO/size row is gone from `Bundle_Accumulation` after close
4. More slits until PO end (PLC trigger mill 1; file/TCP mill 4). Remainder bundle closes (`Bundle_Accumulation` still has pcs at PO-end). `Current_Sequence` steps by one per close. No `bundle close failed: could not allocate sequence`. No leftover `Bundle_Accumulation` rows for that PO after PO-end with SQL up.
5. Shared: SAP-status watcher sees the new output basename as Pending until SAP moves it (SAP is **not** picking up yet — file stays in pending; that is OK).

If any instance dies with `Leftover mill-state JSON found`, you skipped step 2.4.

---

### 9. Rollback (back to `69d0d75`)

The stack adds tables/columns the old binary does not need, but fill-to-target CHECKs, Voided CHECK, and mill-state tables can confuse a downgrade if you only swap the exe. **Restore the step-1 database backup.**

1. `Stop-Service NdtBundleService-M1, NdtBundleService-M2, NdtBundleService-M3, NdtBundleService-M4`
2. `Stop-Service NdtBundleService-Shared`
3. Restore `JazeeraMES_Prod` from the step-1 backup (this removes `Mill_Sequence`, `Mill_Instance_Lease`, `App_Setting`, `Bundle_Accumulation`, `Po_Lifecycle`, `Mill_Printer`, `Station_Printer`, CsvFill/Voided columns, fill hold/audit, etc., and puts data back).
4. Replace `C:\Apps\NdtBundleService\bin` with the `69d0d75` publish. Restore the old monolith content root / `appsettings` (that binary **does** use JSON mill-state files).
5. Start the **old monolith** Windows Service only. Leave the five new services stopped (or `sc.exe delete` them after rollback is proven).
6. If you did **not** restore the DB and only swapped the exe: old monolith will not use `Mill_Sequence`; it will number from JSON again. That can **collide** with any `NDT_Bundle` rows the new build inserted. Do not do a binary-only rollback after any mill has closed a bundle. Restore the DB.
7. `DELETE FROM dbo.Mill_Instance_Lease` is only needed if you did not restore the DB and a mill service still holds a row (TTL 45s). After a DB restore the table is gone or empty.

Folder archive from step 2.3 stays archived unless you need those files back in the inboxes.

---

## Quick abort signs (stop the fleet)

- Any instance: `Leftover mill-state JSON found`
- Any mill: `Fill-to-target cutover blocked`
- Any mill: `Bundle_Accumulation has open size-count rows` on a fresh start (wipe did not clear accumulation)
- Any mill: `Mill_Sequence for mill N is X; live bundles go to Y`
- Any mill: `Mill_Instance_Lease claim failed`
- Any mill: `queued for backfill` > 0 right after this reset
- Shared or mill: missing `Mill_Sequence` / `Bundle_Accumulation` / `Po_Lifecycle` / `Mill_Printer` / `Station_Printer` in SQL health
- Shared or mill: `SQL traceability columns missing` `Manual_Station_Run.Print_Status` / `Print_Error` (script 21 ALTER not applied)
- Shared or mill: `Mill_Printer: 0 row(s).` (seed missing)
- Shared: `Station_Printer: 0 row(s).` (seed missing; station tags will not print)
- `CK_NDT_Bundle_Csv_Fill_State` insert error mentioning `Voided` → Voided script did not run after CsvFill

---

### 10. Five complete `appsettings.Production.json` files (`9377bca`)

Paste each file to `C:\Apps\NdtBundleService\instances\<role>\appsettings.Production.json`. These are complete (not overlays). They do **not** contain `EnableNdtBundleRuntimeStatePersistence`, `NdtBundleRuntimeStateFile`, `MillPrinterSettingsFile`, or `RuntimeStatePruning`.

#### 10.1 Shared

```json
{
  "InstanceRole": {
    "Mode": "Shared",
    "OwnedMillNos": [],
    "InstanceDisplayName": "Shared",
    "EnableDashboardApi": true,
    "EnableMillWorkers": false,
    "EnablePoPlanWipImport": true
  },
  "ShowSwagger": true,
  "Kestrel": {
    "Endpoints": {
      "Http": {
        "Url": "http://*:5000"
      }
    }
  },
  "NdtBundle": {
    "PollIntervalSeconds": 5,
    "ShopId": "01",
    "InputSlitFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Input Slit",
    "InputSlitAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Input Slit Accepted",
    "PreferInputSlitFilesForRunningPo": true,
    "OutputBundleFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\Input Slit",
    "NdtInputSlitAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\NDT Input Slit Accepted",
    "NdtInputSlitRejectedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\NDT Input Slit Rejected",
    "NdtInputSlitSapStatusPollSeconds": 30,
    "EnableBundleSummaryCsvFiles": false,
    "EnableBundleZplPreviewFiles": false,
    "BundleSummaryOutputFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Bundles",
    "NdtProcessOutputFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Final Output\\Bundle",
    "PoPlanFolder": "\\\\10.2.20.210\\pas-sap\\From SAP\\TMFG_TMWIP\\PO Accepted",
    "PoPlanFolderRollingDays": 90,
    "PoPlanCsvPath": "",
    "FormationChartCsvPath": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\FormationChart.csv",
    "PipeSizeCsvPath": "",
    "MinSourceFileLastWriteUtc": "",
    "BundleLabelCsvPath": "",
    "UploadNdtBundleFilesFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\MES PAS NDT\\Bundle",
    "SlitAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Slitting\\Slit Accepted",
    "FgBundleFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle",
    "FgBundleAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle Accepted",
    "WaitForWipBundleAfterPoEnd": true,
    "WipOrderingUseEmbeddedTimestamp": true,
    "InputSlitProcessMills": [
      1
    ],
    "CsvFillQuietMinutes": 180,
    "RequireCleanFillCutover": false,
    "MillCsvBatchMode": {
      "1": {
        "Mode": "FillToTarget"
      },
      "2": {
        "Mode": "Constant",
        "Value": "10001"
      },
      "3": {
        "Mode": "Constant",
        "Value": "10001"
      },
      "4": {
        "Mode": "Constant",
        "Value": "10001"
      }
    },
    "EnableNdtTagZplAndPrint": true,
    "EnableManualStationStateFiles": false,
    "InitialMillBatchNumbers": {
      "1": "1226100000",
      "2": "1226200000",
      "3": "1226300000",
      "4": "1226400000"
    },
    "SyncRuntimeStateFromPrintedBundlesOnly": true,
    "NdtTagPrinterName": "",
    "NdtTagPrinterAddress": "192.168.0.125",
    "NdtTagPrinterPort": 9100,
    "NdtTagLabelWidthMm": 100,
    "NdtTagLabelLengthMm": 100,
    "NdtTagPrinterLocalBindAddress": "",
    "UseSqlServerForBundles": true,
    "StuckPrintThresholdMinutes": 10,
    "PreferSqlForReconcileReads": true,
    "BackfillReconciliationEnabled": true,
    "CloseTrigger": "PlcWithFileFallback",
    "PoEndFlushMode": "Immediate",
    "HooterCountSource": "App",
    "ReprintOnCountMismatch": false,
    "PlcCloseGraceSeconds": 60,
    "BackfillLookbackHours": 48,
    "ReconcileIntervalMinutes": 30,
    "PreferSqlForPoPlanWip": true,
    "ImportPoPlanWipFromFolder": true,
    "ImportPoPlanWipPollMinutes": 5,
    "PoPlanImportMinLastWriteUtc": "2026-06-01T00:00:00Z",
    "MergeWipBundlePipeSizesWhenUsingSqlPoPlan": false,
    "AllowCsvFallbackForBundleReads": true,
    "SqlServer": "AJS-SOH-VM-PAS-\\SQLEXPRESS",
    "SqlDatabase": "JazeeraMES_Prod",
    "ConnectionString": "Server=AJS-SOH-VM-PAS-\\SQLEXPRESS;Database=JazeeraMES_Prod;Trusted_Connection=True;TrustServerCertificate=True;",
    "MillSlitLive": {
      "Enabled": false,
      "ApplyToMillNo": 1,
      "WipBundleFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle",
      "WipBundleAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle Accepted",
      "S7": {
        "Host": "192.168.0.13",
        "Rack": 0,
        "Slot": 2,
        "CpuType": "S7300",
        "DbNumber": 251,
        "NdtCountByteOffset": 6
      }
    },
    "DashboardSettings": {
      "AdminPassword": "JazeeraMES@2026",
      "SessionHours": 8
    },
    "FileBasedPoEnd": {
      "AdvancePoPlanFileOnPoEnd": false,
      "ReconciliationEnabled": true,
      "ReconciliationIntervalMinutes": 5
    },
    "PlcHandshake": {
      "Enabled": false,
      "TelemetryOnly": false,
      "PollIntervalMs": 500,
      "AdvancePoPlanFileOnPoEnd": false,
      "InitialReconnectDelayMs": 1000,
      "MaxReconnectDelayMs": 30000,
      "MinimumTriggerFalsePollsBeforeRearm": 1,
      "RecoverLatchedTriggerAtStartup": true,
      "RunPoEndWorkflowOnStartupRecovery": false,
      "HandshakeAuditEnabled": true,
      "StuckTriggerAlarmSeconds": 30,
      "AckWriteRetryCount": 3,
      "AckWriteRetryInitialBackoffMs": 100,
      "NdtCountDb": 251,
      "NdtCountByteOffset": 6,
      "SlitEndTriggerByte": -1,
      "SlitEndTriggerBit": 0,
      "Mills": []
    },
    "PlcPoEnd": {
      "Enabled": false,
      "Driver": "S7",
      "DetectionMode": "CoilRisingEdge",
      "WriteMesAckOnlyOnWorkflowSuccess": true,
      "AdvancePoPlanFileOnPoEnd": false,
      "ModbusConnectTimeoutMs": 3000,
      "PoNumberFormatFromPlc": "{0}",
      "MinValidPoId": 1000000000,
      "MinSapPoNumberDigits": 10,
      "Mills": [
        {
          "MillNo": 1,
          "Host": "192.168.0.13",
          "Port": 102,
          "Rack": 0,
          "Slot": 2,
          "CpuType": "S7300",
          "S7PoEndAddress": "M40.6",
          "S7MesAckAddress": "M40.7",
          "MesAckPulseMs": 1000
        },
        {
          "MillNo": 2,
          "Host": "192.168.0.60",
          "Port": 102,
          "Rack": 0,
          "Slot": 2,
          "CpuType": "S7300",
          "S7PoEndAddress": "M40.6",
          "S7MesAckAddress": "M40.7",
          "MesAckPulseMs": 1000
        },
        {
          "MillNo": 3,
          "Host": "192.168.0.17",
          "Port": 102,
          "Rack": 0,
          "Slot": 2,
          "CpuType": "S7300",
          "S7PoEndAddress": "M20.6",
          "S7MesAckAddress": "M22.7",
          "MesAckPulseMs": 1000
        }
      ]
    }
  },
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning"
    },
    "File": {
      "Enabled": true,
      "Folder": "C:\\Apps\\NdtBundleService\\instances\\shared\\Logs",
      "FileNamePrefix": "ndtbundle-shared",
      "RetainFileCount": 31,
      "WriteToEventLog": true
    }
  }
}
```

#### 10.2 Mill-1

```json
{
  "InstanceRole": {
    "Mode": "Mill",
    "OwnedMillNos": [
      1
    ],
    "InstanceDisplayName": "Mill-1",
    "EnableDashboardApi": false,
    "EnableMillWorkers": true,
    "EnablePoPlanWipImport": false
  },
  "ShowSwagger": true,
  "Kestrel": {
    "Endpoints": {
      "Http": {
        "Url": "http://127.0.0.1:5001"
      }
    }
  },
  "NdtBundle": {
    "PollIntervalSeconds": 5,
    "ShopId": "01",
    "InputSlitFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Input Slit",
    "InputSlitAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Input Slit Accepted",
    "PreferInputSlitFilesForRunningPo": true,
    "OutputBundleFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\Input Slit",
    "NdtInputSlitAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\NDT Input Slit Accepted",
    "NdtInputSlitRejectedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\NDT Input Slit Rejected",
    "NdtInputSlitSapStatusPollSeconds": 30,
    "EnableBundleSummaryCsvFiles": false,
    "EnableBundleZplPreviewFiles": false,
    "BundleSummaryOutputFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Bundles",
    "NdtProcessOutputFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Final Output\\Bundle",
    "PoPlanFolder": "\\\\10.2.20.210\\pas-sap\\From SAP\\TMFG_TMWIP\\PO Accepted",
    "PoPlanFolderRollingDays": 90,
    "PoPlanCsvPath": "",
    "FormationChartCsvPath": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\FormationChart.csv",
    "PipeSizeCsvPath": "",
    "MinSourceFileLastWriteUtc": "2026-08-30T12:53:57Z",
    "BundleLabelCsvPath": "",
    "UploadNdtBundleFilesFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\MES PAS NDT\\Bundle",
    "SlitAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Slitting\\Slit Accepted",
    "FgBundleFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle",
    "FgBundleAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle Accepted",
    "WaitForWipBundleAfterPoEnd": true,
    "WipOrderingUseEmbeddedTimestamp": true,
    "InputSlitProcessMills": [
      1
    ],
    "CsvFillQuietMinutes": 180,
    "RequireCleanFillCutover": true,
    "MillCsvBatchMode": {
      "1": {
        "Mode": "FillToTarget"
      },
      "2": {
        "Mode": "Constant",
        "Value": "10001"
      },
      "3": {
        "Mode": "Constant",
        "Value": "10001"
      },
      "4": {
        "Mode": "Constant",
        "Value": "10001"
      }
    },
    "EnableNdtTagZplAndPrint": true,
    "EnableManualStationStateFiles": false,
    "InitialMillBatchNumbers": {
      "1": "1226100000",
      "2": "1226200000",
      "3": "1226300000",
      "4": "1226400000"
    },
    "SyncRuntimeStateFromPrintedBundlesOnly": true,
    "NdtTagPrinterName": "",
    "NdtTagPrinterAddress": "192.168.0.125",
    "NdtTagPrinterPort": 9100,
    "NdtTagLabelWidthMm": 100,
    "NdtTagLabelLengthMm": 100,
    "NdtTagPrinterLocalBindAddress": "",
    "UseSqlServerForBundles": true,
    "StuckPrintThresholdMinutes": 10,
    "PreferSqlForReconcileReads": true,
    "BackfillReconciliationEnabled": true,
    "CloseTrigger": "PlcWithFileFallback",
    "PoEndFlushMode": "Immediate",
    "HooterCountSource": "App",
    "ReprintOnCountMismatch": false,
    "PlcCloseGraceSeconds": 60,
    "BackfillLookbackHours": 48,
    "ReconcileIntervalMinutes": 30,
    "PreferSqlForPoPlanWip": true,
    "ImportPoPlanWipFromFolder": false,
    "ImportPoPlanWipPollMinutes": 5,
    "PoPlanImportMinLastWriteUtc": "2026-06-01T00:00:00Z",
    "MergeWipBundlePipeSizesWhenUsingSqlPoPlan": false,
    "AllowCsvFallbackForBundleReads": true,
    "SqlServer": "AJS-SOH-VM-PAS-\\SQLEXPRESS",
    "SqlDatabase": "JazeeraMES_Prod",
    "ConnectionString": "Server=AJS-SOH-VM-PAS-\\SQLEXPRESS;Database=JazeeraMES_Prod;Trusted_Connection=True;TrustServerCertificate=True;",
    "MillSlitLive": {
      "Enabled": false,
      "ApplyToMillNo": 1,
      "WipBundleFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle",
      "WipBundleAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle Accepted",
      "S7": {
        "Host": "192.168.0.13",
        "Rack": 0,
        "Slot": 2,
        "CpuType": "S7300",
        "DbNumber": 251,
        "NdtCountByteOffset": 6
      }
    },
    "DashboardSettings": {
      "AdminPassword": "JazeeraMES@2026",
      "SessionHours": 8
    },
    "FileBasedPoEnd": {
      "AdvancePoPlanFileOnPoEnd": false,
      "ReconciliationEnabled": true,
      "ReconciliationIntervalMinutes": 5
    },
    "PlcHandshake": {
      "Enabled": true,
      "TelemetryOnly": false,
      "PollIntervalMs": 500,
      "AdvancePoPlanFileOnPoEnd": false,
      "InitialReconnectDelayMs": 1000,
      "MaxReconnectDelayMs": 30000,
      "MinimumTriggerFalsePollsBeforeRearm": 1,
      "RecoverLatchedTriggerAtStartup": true,
      "RunPoEndWorkflowOnStartupRecovery": false,
      "HandshakeAuditEnabled": true,
      "StuckTriggerAlarmSeconds": 30,
      "AckWriteRetryCount": 3,
      "AckWriteRetryInitialBackoffMs": 100,
      "NdtCountDb": 251,
      "NdtCountByteOffset": 6,
      "SlitEndTriggerByte": -1,
      "SlitEndTriggerBit": 0,
      "Mills": [
        {
          "Name": "Mill-1",
          "MillNo": 1,
          "PoEndSource": "Plc",
          "PlcHandshakeEnabled": true,
          "IpAddress": "192.168.0.13",
          "Rack": 0,
          "Slot": 2,
          "SlotFallback": [
            1
          ],
          "CpuType": "S7300",
          "TriggerByte": 40,
          "TriggerBit": 6,
          "AckByte": 40,
          "AckBit": 7,
          "Hooter": {
            "Enabled": true,
            "PasEnableDbNumber": 260,
            "PasEnableByteOffset": 3,
            "PasEnableBit": 6,
            "AccumulatedWordOffset": 56,
            "ThresholdWordOffset": 58,
            "OutputByte": 6,
            "OutputBit": 7,
            "DurationMs": 10000
          }
        }
      ]
    },
    "PlcPoEnd": {
      "Enabled": false,
      "Driver": "S7",
      "DetectionMode": "CoilRisingEdge",
      "WriteMesAckOnlyOnWorkflowSuccess": true,
      "AdvancePoPlanFileOnPoEnd": false,
      "ModbusConnectTimeoutMs": 3000,
      "PoNumberFormatFromPlc": "{0}",
      "MinValidPoId": 1000000000,
      "MinSapPoNumberDigits": 10,
      "Mills": [
        {
          "MillNo": 1,
          "Host": "192.168.0.13",
          "Port": 102,
          "Rack": 0,
          "Slot": 2,
          "CpuType": "S7300",
          "S7PoEndAddress": "M40.6",
          "S7MesAckAddress": "M40.7",
          "MesAckPulseMs": 1000
        },
        {
          "MillNo": 2,
          "Host": "192.168.0.60",
          "Port": 102,
          "Rack": 0,
          "Slot": 2,
          "CpuType": "S7300",
          "S7PoEndAddress": "M40.6",
          "S7MesAckAddress": "M40.7",
          "MesAckPulseMs": 1000
        },
        {
          "MillNo": 3,
          "Host": "192.168.0.17",
          "Port": 102,
          "Rack": 0,
          "Slot": 2,
          "CpuType": "S7300",
          "S7PoEndAddress": "M20.6",
          "S7MesAckAddress": "M22.7",
          "MesAckPulseMs": 1000
        }
      ]
    }
  },
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning"
    },
    "File": {
      "Enabled": true,
      "Folder": "C:\\Apps\\NdtBundleService\\instances\\mill-1\\Logs",
      "FileNamePrefix": "ndtbundle-m1",
      "RetainFileCount": 31,
      "WriteToEventLog": true
    }
  }
}
```

#### 10.3 Mill-2

```json
{
  "InstanceRole": {
    "Mode": "Mill",
    "OwnedMillNos": [
      2
    ],
    "InstanceDisplayName": "Mill-2",
    "EnableDashboardApi": false,
    "EnableMillWorkers": true,
    "EnablePoPlanWipImport": false
  },
  "ShowSwagger": true,
  "Kestrel": {
    "Endpoints": {
      "Http": {
        "Url": "http://127.0.0.1:5002"
      }
    }
  },
  "NdtBundle": {
    "PollIntervalSeconds": 5,
    "ShopId": "01",
    "InputSlitFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Input Slit",
    "InputSlitAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Input Slit Accepted",
    "PreferInputSlitFilesForRunningPo": true,
    "OutputBundleFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\Input Slit",
    "NdtInputSlitAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\NDT Input Slit Accepted",
    "NdtInputSlitRejectedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\NDT Input Slit Rejected",
    "NdtInputSlitSapStatusPollSeconds": 30,
    "EnableBundleSummaryCsvFiles": false,
    "EnableBundleZplPreviewFiles": false,
    "BundleSummaryOutputFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Bundles",
    "NdtProcessOutputFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Final Output\\Bundle",
    "PoPlanFolder": "\\\\10.2.20.210\\pas-sap\\From SAP\\TMFG_TMWIP\\PO Accepted",
    "PoPlanFolderRollingDays": 90,
    "PoPlanCsvPath": "",
    "FormationChartCsvPath": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\FormationChart.csv",
    "PipeSizeCsvPath": "",
    "MinSourceFileLastWriteUtc": "2026-08-30T12:53:57Z",
    "BundleLabelCsvPath": "",
    "UploadNdtBundleFilesFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\MES PAS NDT\\Bundle",
    "SlitAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Slitting\\Slit Accepted",
    "FgBundleFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle",
    "FgBundleAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle Accepted",
    "WaitForWipBundleAfterPoEnd": true,
    "WipOrderingUseEmbeddedTimestamp": true,
    "InputSlitProcessMills": [
      2
    ],
    "CsvFillQuietMinutes": 180,
    "RequireCleanFillCutover": true,
    "MillCsvBatchMode": {
      "1": {
        "Mode": "FillToTarget"
      },
      "2": {
        "Mode": "Constant",
        "Value": "10001"
      },
      "3": {
        "Mode": "Constant",
        "Value": "10001"
      },
      "4": {
        "Mode": "Constant",
        "Value": "10001"
      }
    },
    "EnableNdtTagZplAndPrint": true,
    "EnableManualStationStateFiles": false,
    "InitialMillBatchNumbers": {
      "1": "1226100000",
      "2": "1226200000",
      "3": "1226300000",
      "4": "1226400000"
    },
    "SyncRuntimeStateFromPrintedBundlesOnly": true,
    "NdtTagPrinterName": "",
    "NdtTagPrinterAddress": "192.168.0.125",
    "NdtTagPrinterPort": 9100,
    "NdtTagLabelWidthMm": 100,
    "NdtTagLabelLengthMm": 100,
    "NdtTagPrinterLocalBindAddress": "",
    "UseSqlServerForBundles": true,
    "StuckPrintThresholdMinutes": 10,
    "PreferSqlForReconcileReads": true,
    "BackfillReconciliationEnabled": true,
    "CloseTrigger": "PlcWithFileFallback",
    "PoEndFlushMode": "Immediate",
    "HooterCountSource": "App",
    "ReprintOnCountMismatch": false,
    "PlcCloseGraceSeconds": 60,
    "BackfillLookbackHours": 48,
    "ReconcileIntervalMinutes": 30,
    "PreferSqlForPoPlanWip": true,
    "ImportPoPlanWipFromFolder": false,
    "ImportPoPlanWipPollMinutes": 5,
    "PoPlanImportMinLastWriteUtc": "2026-06-01T00:00:00Z",
    "MergeWipBundlePipeSizesWhenUsingSqlPoPlan": false,
    "AllowCsvFallbackForBundleReads": true,
    "SqlServer": "AJS-SOH-VM-PAS-\\SQLEXPRESS",
    "SqlDatabase": "JazeeraMES_Prod",
    "ConnectionString": "Server=AJS-SOH-VM-PAS-\\SQLEXPRESS;Database=JazeeraMES_Prod;Trusted_Connection=True;TrustServerCertificate=True;",
    "MillSlitLive": {
      "Enabled": false,
      "ApplyToMillNo": 1,
      "WipBundleFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle",
      "WipBundleAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle Accepted",
      "S7": {
        "Host": "192.168.0.13",
        "Rack": 0,
        "Slot": 2,
        "CpuType": "S7300",
        "DbNumber": 251,
        "NdtCountByteOffset": 6
      }
    },
    "DashboardSettings": {
      "AdminPassword": "JazeeraMES@2026",
      "SessionHours": 8
    },
    "FileBasedPoEnd": {
      "AdvancePoPlanFileOnPoEnd": false,
      "ReconciliationEnabled": true,
      "ReconciliationIntervalMinutes": 5
    },
    "PlcHandshake": {
      "Enabled": true,
      "TelemetryOnly": false,
      "PollIntervalMs": 500,
      "AdvancePoPlanFileOnPoEnd": false,
      "InitialReconnectDelayMs": 1000,
      "MaxReconnectDelayMs": 30000,
      "MinimumTriggerFalsePollsBeforeRearm": 1,
      "RecoverLatchedTriggerAtStartup": true,
      "RunPoEndWorkflowOnStartupRecovery": false,
      "HandshakeAuditEnabled": true,
      "StuckTriggerAlarmSeconds": 30,
      "AckWriteRetryCount": 3,
      "AckWriteRetryInitialBackoffMs": 100,
      "NdtCountDb": 251,
      "NdtCountByteOffset": 6,
      "SlitEndTriggerByte": -1,
      "SlitEndTriggerBit": 0,
      "Mills": [
        {
          "Name": "Mill-2",
          "MillNo": 2,
          "PoEndSource": "Plc",
          "PlcHandshakeEnabled": false,
          "IpAddress": "192.168.0.60",
          "Rack": 0,
          "Slot": 2,
          "SlotFallback": [
            1
          ],
          "CpuType": "S7300",
          "TriggerByte": 40,
          "TriggerBit": 6,
          "AckByte": 40,
          "AckBit": 7
        }
      ]
    },
    "PlcPoEnd": {
      "Enabled": false,
      "Driver": "S7",
      "DetectionMode": "CoilRisingEdge",
      "WriteMesAckOnlyOnWorkflowSuccess": true,
      "AdvancePoPlanFileOnPoEnd": false,
      "ModbusConnectTimeoutMs": 3000,
      "PoNumberFormatFromPlc": "{0}",
      "MinValidPoId": 1000000000,
      "MinSapPoNumberDigits": 10,
      "Mills": [
        {
          "MillNo": 1,
          "Host": "192.168.0.13",
          "Port": 102,
          "Rack": 0,
          "Slot": 2,
          "CpuType": "S7300",
          "S7PoEndAddress": "M40.6",
          "S7MesAckAddress": "M40.7",
          "MesAckPulseMs": 1000
        },
        {
          "MillNo": 2,
          "Host": "192.168.0.60",
          "Port": 102,
          "Rack": 0,
          "Slot": 2,
          "CpuType": "S7300",
          "S7PoEndAddress": "M40.6",
          "S7MesAckAddress": "M40.7",
          "MesAckPulseMs": 1000
        },
        {
          "MillNo": 3,
          "Host": "192.168.0.17",
          "Port": 102,
          "Rack": 0,
          "Slot": 2,
          "CpuType": "S7300",
          "S7PoEndAddress": "M20.6",
          "S7MesAckAddress": "M22.7",
          "MesAckPulseMs": 1000
        }
      ]
    }
  },
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning"
    },
    "File": {
      "Enabled": true,
      "Folder": "C:\\Apps\\NdtBundleService\\instances\\mill-2\\Logs",
      "FileNamePrefix": "ndtbundle-m2",
      "RetainFileCount": 31,
      "WriteToEventLog": true
    }
  }
}
```

#### 10.4 Mill-3

```json
{
  "InstanceRole": {
    "Mode": "Mill",
    "OwnedMillNos": [
      3
    ],
    "InstanceDisplayName": "Mill-3",
    "EnableDashboardApi": false,
    "EnableMillWorkers": true,
    "EnablePoPlanWipImport": false
  },
  "ShowSwagger": true,
  "Kestrel": {
    "Endpoints": {
      "Http": {
        "Url": "http://127.0.0.1:5003"
      }
    }
  },
  "NdtBundle": {
    "PollIntervalSeconds": 5,
    "ShopId": "01",
    "InputSlitFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Input Slit",
    "InputSlitAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Input Slit Accepted",
    "PreferInputSlitFilesForRunningPo": true,
    "OutputBundleFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\Input Slit",
    "NdtInputSlitAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\NDT Input Slit Accepted",
    "NdtInputSlitRejectedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\NDT Input Slit Rejected",
    "NdtInputSlitSapStatusPollSeconds": 30,
    "EnableBundleSummaryCsvFiles": false,
    "EnableBundleZplPreviewFiles": false,
    "BundleSummaryOutputFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Bundles",
    "NdtProcessOutputFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Final Output\\Bundle",
    "PoPlanFolder": "\\\\10.2.20.210\\pas-sap\\From SAP\\TMFG_TMWIP\\PO Accepted",
    "PoPlanFolderRollingDays": 90,
    "PoPlanCsvPath": "",
    "FormationChartCsvPath": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\FormationChart.csv",
    "PipeSizeCsvPath": "",
    "MinSourceFileLastWriteUtc": "2026-08-30T12:53:57Z",
    "BundleLabelCsvPath": "",
    "UploadNdtBundleFilesFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\MES PAS NDT\\Bundle",
    "SlitAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Slitting\\Slit Accepted",
    "FgBundleFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle",
    "FgBundleAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle Accepted",
    "WaitForWipBundleAfterPoEnd": true,
    "WipOrderingUseEmbeddedTimestamp": true,
    "InputSlitProcessMills": [
      3
    ],
    "CsvFillQuietMinutes": 180,
    "RequireCleanFillCutover": true,
    "MillCsvBatchMode": {
      "1": {
        "Mode": "FillToTarget"
      },
      "2": {
        "Mode": "Constant",
        "Value": "10001"
      },
      "3": {
        "Mode": "Constant",
        "Value": "10001"
      },
      "4": {
        "Mode": "Constant",
        "Value": "10001"
      }
    },
    "EnableNdtTagZplAndPrint": true,
    "EnableManualStationStateFiles": false,
    "InitialMillBatchNumbers": {
      "1": "1226100000",
      "2": "1226200000",
      "3": "1226300000",
      "4": "1226400000"
    },
    "SyncRuntimeStateFromPrintedBundlesOnly": true,
    "NdtTagPrinterName": "",
    "NdtTagPrinterAddress": "192.168.0.125",
    "NdtTagPrinterPort": 9100,
    "NdtTagLabelWidthMm": 100,
    "NdtTagLabelLengthMm": 100,
    "NdtTagPrinterLocalBindAddress": "",
    "UseSqlServerForBundles": true,
    "StuckPrintThresholdMinutes": 10,
    "PreferSqlForReconcileReads": true,
    "BackfillReconciliationEnabled": true,
    "CloseTrigger": "PlcWithFileFallback",
    "PoEndFlushMode": "Immediate",
    "HooterCountSource": "App",
    "ReprintOnCountMismatch": false,
    "PlcCloseGraceSeconds": 60,
    "BackfillLookbackHours": 48,
    "ReconcileIntervalMinutes": 30,
    "PreferSqlForPoPlanWip": true,
    "ImportPoPlanWipFromFolder": false,
    "ImportPoPlanWipPollMinutes": 5,
    "PoPlanImportMinLastWriteUtc": "2026-06-01T00:00:00Z",
    "MergeWipBundlePipeSizesWhenUsingSqlPoPlan": false,
    "AllowCsvFallbackForBundleReads": true,
    "SqlServer": "AJS-SOH-VM-PAS-\\SQLEXPRESS",
    "SqlDatabase": "JazeeraMES_Prod",
    "ConnectionString": "Server=AJS-SOH-VM-PAS-\\SQLEXPRESS;Database=JazeeraMES_Prod;Trusted_Connection=True;TrustServerCertificate=True;",
    "MillSlitLive": {
      "Enabled": false,
      "ApplyToMillNo": 1,
      "WipBundleFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle",
      "WipBundleAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle Accepted",
      "S7": {
        "Host": "192.168.0.13",
        "Rack": 0,
        "Slot": 2,
        "CpuType": "S7300",
        "DbNumber": 251,
        "NdtCountByteOffset": 6
      }
    },
    "DashboardSettings": {
      "AdminPassword": "JazeeraMES@2026",
      "SessionHours": 8
    },
    "FileBasedPoEnd": {
      "AdvancePoPlanFileOnPoEnd": false,
      "ReconciliationEnabled": true,
      "ReconciliationIntervalMinutes": 5
    },
    "PlcHandshake": {
      "Enabled": true,
      "TelemetryOnly": false,
      "PollIntervalMs": 500,
      "AdvancePoPlanFileOnPoEnd": false,
      "InitialReconnectDelayMs": 1000,
      "MaxReconnectDelayMs": 30000,
      "MinimumTriggerFalsePollsBeforeRearm": 1,
      "RecoverLatchedTriggerAtStartup": true,
      "RunPoEndWorkflowOnStartupRecovery": false,
      "HandshakeAuditEnabled": true,
      "StuckTriggerAlarmSeconds": 30,
      "AckWriteRetryCount": 3,
      "AckWriteRetryInitialBackoffMs": 100,
      "NdtCountDb": 251,
      "NdtCountByteOffset": 6,
      "SlitEndTriggerByte": -1,
      "SlitEndTriggerBit": 0,
      "Mills": [
        {
          "Name": "Mill-3",
          "MillNo": 3,
          "PoEndSource": "Plc",
          "PlcHandshakeEnabled": false,
          "IpAddress": "192.168.0.17",
          "Rack": 0,
          "Slot": 2,
          "SlotFallback": [
            1
          ],
          "CpuType": "S7300",
          "TriggerByte": 20,
          "TriggerBit": 6,
          "AckByte": 22,
          "AckBit": 7
        }
      ]
    },
    "PlcPoEnd": {
      "Enabled": false,
      "Driver": "S7",
      "DetectionMode": "CoilRisingEdge",
      "WriteMesAckOnlyOnWorkflowSuccess": true,
      "AdvancePoPlanFileOnPoEnd": false,
      "ModbusConnectTimeoutMs": 3000,
      "PoNumberFormatFromPlc": "{0}",
      "MinValidPoId": 1000000000,
      "MinSapPoNumberDigits": 10,
      "Mills": [
        {
          "MillNo": 1,
          "Host": "192.168.0.13",
          "Port": 102,
          "Rack": 0,
          "Slot": 2,
          "CpuType": "S7300",
          "S7PoEndAddress": "M40.6",
          "S7MesAckAddress": "M40.7",
          "MesAckPulseMs": 1000
        },
        {
          "MillNo": 2,
          "Host": "192.168.0.60",
          "Port": 102,
          "Rack": 0,
          "Slot": 2,
          "CpuType": "S7300",
          "S7PoEndAddress": "M40.6",
          "S7MesAckAddress": "M40.7",
          "MesAckPulseMs": 1000
        },
        {
          "MillNo": 3,
          "Host": "192.168.0.17",
          "Port": 102,
          "Rack": 0,
          "Slot": 2,
          "CpuType": "S7300",
          "S7PoEndAddress": "M20.6",
          "S7MesAckAddress": "M22.7",
          "MesAckPulseMs": 1000
        }
      ]
    }
  },
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning"
    },
    "File": {
      "Enabled": true,
      "Folder": "C:\\Apps\\NdtBundleService\\instances\\mill-3\\Logs",
      "FileNamePrefix": "ndtbundle-m3",
      "RetainFileCount": 31,
      "WriteToEventLog": true
    }
  }
}
```

#### 10.5 Mill-4

```json
{
  "InstanceRole": {
    "Mode": "Mill",
    "OwnedMillNos": [
      4
    ],
    "InstanceDisplayName": "Mill-4",
    "EnableDashboardApi": false,
    "EnableMillWorkers": true,
    "EnablePoPlanWipImport": false
  },
  "ShowSwagger": true,
  "Kestrel": {
    "Endpoints": {
      "Http": {
        "Url": "http://127.0.0.1:5004"
      }
    }
  },
  "NdtBundle": {
    "PollIntervalSeconds": 5,
    "ShopId": "01",
    "InputSlitFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Input Slit",
    "InputSlitAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Input Slit Accepted",
    "PreferInputSlitFilesForRunningPo": true,
    "OutputBundleFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\Input Slit",
    "NdtInputSlitAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\NDT Input Slit Accepted",
    "NdtInputSlitRejectedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\NDT Input Slit Rejected",
    "NdtInputSlitSapStatusPollSeconds": 30,
    "EnableBundleSummaryCsvFiles": false,
    "EnableBundleZplPreviewFiles": false,
    "BundleSummaryOutputFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Bundles",
    "NdtProcessOutputFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Final Output\\Bundle",
    "PoPlanFolder": "\\\\10.2.20.210\\pas-sap\\From SAP\\TMFG_TMWIP\\PO Accepted",
    "PoPlanFolderRollingDays": 90,
    "PoPlanCsvPath": "",
    "FormationChartCsvPath": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\NDT Input Slit\\FormationChart.csv",
    "PipeSizeCsvPath": "",
    "MinSourceFileLastWriteUtc": "2026-08-30T12:53:57Z",
    "BundleLabelCsvPath": "",
    "UploadNdtBundleFilesFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\NDT\\MES PAS NDT\\Bundle",
    "SlitAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Slitting\\Slit Accepted",
    "FgBundleFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle",
    "FgBundleAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle Accepted",
    "WaitForWipBundleAfterPoEnd": true,
    "WipOrderingUseEmbeddedTimestamp": true,
    "InputSlitProcessMills": [
      4
    ],
    "CsvFillQuietMinutes": 180,
    "RequireCleanFillCutover": true,
    "MillCsvBatchMode": {
      "1": {
        "Mode": "FillToTarget"
      },
      "2": {
        "Mode": "Constant",
        "Value": "10001"
      },
      "3": {
        "Mode": "Constant",
        "Value": "10001"
      },
      "4": {
        "Mode": "Constant",
        "Value": "10001"
      }
    },
    "EnableNdtTagZplAndPrint": true,
    "EnableManualStationStateFiles": false,
    "InitialMillBatchNumbers": {
      "1": "1226100000",
      "2": "1226200000",
      "3": "1226300000",
      "4": "1226400000"
    },
    "SyncRuntimeStateFromPrintedBundlesOnly": true,
    "NdtTagPrinterName": "",
    "NdtTagPrinterAddress": "192.168.0.125",
    "NdtTagPrinterPort": 9100,
    "NdtTagLabelWidthMm": 100,
    "NdtTagLabelLengthMm": 100,
    "NdtTagPrinterLocalBindAddress": "",
    "UseSqlServerForBundles": true,
    "StuckPrintThresholdMinutes": 10,
    "PreferSqlForReconcileReads": true,
    "BackfillReconciliationEnabled": true,
    "CloseTrigger": "PlcWithFileFallback",
    "PoEndFlushMode": "Immediate",
    "HooterCountSource": "App",
    "ReprintOnCountMismatch": false,
    "PlcCloseGraceSeconds": 60,
    "BackfillLookbackHours": 48,
    "ReconcileIntervalMinutes": 30,
    "PreferSqlForPoPlanWip": true,
    "ImportPoPlanWipFromFolder": false,
    "ImportPoPlanWipPollMinutes": 5,
    "PoPlanImportMinLastWriteUtc": "2026-06-01T00:00:00Z",
    "MergeWipBundlePipeSizesWhenUsingSqlPoPlan": false,
    "AllowCsvFallbackForBundleReads": true,
    "SqlServer": "AJS-SOH-VM-PAS-\\SQLEXPRESS",
    "SqlDatabase": "JazeeraMES_Prod",
    "ConnectionString": "Server=AJS-SOH-VM-PAS-\\SQLEXPRESS;Database=JazeeraMES_Prod;Trusted_Connection=True;TrustServerCertificate=True;",
    "MillSlitLive": {
      "Enabled": false,
      "ApplyToMillNo": 1,
      "WipBundleFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle",
      "WipBundleAcceptedFolder": "\\\\10.2.20.210\\pas-sap\\To SAP\\TM\\Bundle Accepted",
      "S7": {
        "Host": "192.168.0.13",
        "Rack": 0,
        "Slot": 2,
        "CpuType": "S7300",
        "DbNumber": 251,
        "NdtCountByteOffset": 6
      }
    },
    "DashboardSettings": {
      "AdminPassword": "JazeeraMES@2026",
      "SessionHours": 8
    },
    "FileBasedPoEnd": {
      "AdvancePoPlanFileOnPoEnd": false,
      "ReconciliationEnabled": true,
      "ReconciliationIntervalMinutes": 5
    },
    "PlcHandshake": {
      "Enabled": true,
      "TelemetryOnly": false,
      "PollIntervalMs": 500,
      "AdvancePoPlanFileOnPoEnd": false,
      "InitialReconnectDelayMs": 1000,
      "MaxReconnectDelayMs": 30000,
      "MinimumTriggerFalsePollsBeforeRearm": 1,
      "RecoverLatchedTriggerAtStartup": true,
      "RunPoEndWorkflowOnStartupRecovery": false,
      "HandshakeAuditEnabled": true,
      "StuckTriggerAlarmSeconds": 30,
      "AckWriteRetryCount": 3,
      "AckWriteRetryInitialBackoffMs": 100,
      "NdtCountDb": 251,
      "NdtCountByteOffset": 6,
      "SlitEndTriggerByte": -1,
      "SlitEndTriggerBit": 0,
      "Mills": [
        {
          "Name": "Mill-4",
          "MillNo": 4,
          "PoEndSource": "File",
          "PlcHandshakeEnabled": false,
          "IpAddress": "192.168.0.19",
          "TcpOpenPort": 2000,
          "TcpOpenConnectTimeoutMs": 5000,
          "TcpOpenReceiveTimeoutMs": 0,
          "Rack": 0,
          "Slot": 2,
          "SlotFallback": [
            1
          ],
          "CpuType": "S7300",
          "TriggerByte": 41,
          "TriggerBit": 6,
          "AckByte": 41,
          "AckBit": 7
        }
      ]
    },
    "PlcPoEnd": {
      "Enabled": false,
      "Driver": "S7",
      "DetectionMode": "CoilRisingEdge",
      "WriteMesAckOnlyOnWorkflowSuccess": true,
      "AdvancePoPlanFileOnPoEnd": false,
      "ModbusConnectTimeoutMs": 3000,
      "PoNumberFormatFromPlc": "{0}",
      "MinValidPoId": 1000000000,
      "MinSapPoNumberDigits": 10,
      "Mills": [
        {
          "MillNo": 1,
          "Host": "192.168.0.13",
          "Port": 102,
          "Rack": 0,
          "Slot": 2,
          "CpuType": "S7300",
          "S7PoEndAddress": "M40.6",
          "S7MesAckAddress": "M40.7",
          "MesAckPulseMs": 1000
        },
        {
          "MillNo": 2,
          "Host": "192.168.0.60",
          "Port": 102,
          "Rack": 0,
          "Slot": 2,
          "CpuType": "S7300",
          "S7PoEndAddress": "M40.6",
          "S7MesAckAddress": "M40.7",
          "MesAckPulseMs": 1000
        },
        {
          "MillNo": 3,
          "Host": "192.168.0.17",
          "Port": 102,
          "Rack": 0,
          "Slot": 2,
          "CpuType": "S7300",
          "S7PoEndAddress": "M20.6",
          "S7MesAckAddress": "M22.7",
          "MesAckPulseMs": 1000
        }
      ]
    }
  },
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning"
    },
    "File": {
      "Enabled": true,
      "Folder": "C:\\Apps\\NdtBundleService\\instances\\mill-4\\Logs",
      "FileNamePrefix": "ndtbundle-m4",
      "RetainFileCount": 31,
      "WriteToEventLog": true
    }
  }
}
```
