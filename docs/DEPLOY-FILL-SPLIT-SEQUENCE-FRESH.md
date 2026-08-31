# Fresh deploy: fill-to-target + five instances + Mill_Sequence

Commits (in order, deploy together as `9e79619`):

| Commit | What it is |
|---|---|
| `69d0d75` | **Pre-deployment / rollback target** (last production-safe monolith before these three) |
| `a91efbd` | Fill-to-target CSV assignment |
| `d22b620` | Five Windows Services (Shared + Mill-1..4), mill lease |
| `9e79619` | `Mill_Sequence` as close numbering; write-then-clear; bundle merge columns |

This is a **full reset**. SAP has not switched pickup folders. Do not preserve NDT operational data.

Share root used below:

`\\10.2.20.210\pas-sap\`

UNC folder `NDT Input Slit` means:

`\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Input Slit`

Database: `JazeeraMES_Prod` on `AJS-SOH-VM-PAS-\SQLEXPRESS`.

Service install root: `C:\Apps\NdtBundleService\` (`bin\` + `instances\`).

---

## Numbered checklist

### 1. Backup and rollback prep

1. Confirm current production binary is still **`69d0d75`** (or note the exact commit/folder you will restore). Keep a copy of that publish folder, e.g. `C:\Apps\NdtBundleService\bin-69d0d75\`.
2. Full backup of `JazeeraMES_Prod` (schema + data). Name it with the date. This is the rollback image. Do not skip it.
3. Snapshot (copy aside, do not delete yet) of:
   - `C:\Apps\NdtBundleService\` (current monolith `bin` + content root / `appsettings`)
   - Windows Service name/binPath of the running monolith (`sc.exe qc NdtBundleService` or whatever name is installed)
4. Stop mill work: no new slits, no PO-end, no dashboard reconcile. Then stop the **current monolith** Windows Service so it cannot write during reset.

Rollback later = restore this backup + this `bin` + the old single service. See step 12.

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
| `\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Bundles` | Bundle summary CSVs |
| `\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Final Output\Bundle` | NDT process CSVs |
| `\\10.2.20.210\pas-sap\To SAP\TM\NDT\MES PAS NDT\Bundle` | Upload-to-SAP bundle CSVs |

Do **not** recreate `Input Slit` or `Input Slit Accepted` as empty.

#### 2.4 State files to **delete** (every JSON the mill writes)

All under `\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Input Slit\` (and any copy next to the old monolith content root):

| File | Must delete |
|---|---|
| `NdtBundleRuntimeState.json` | Yes (monolith leftover) |
| `NdtBundleRuntimeState-M1.json` … `M4.json` | Yes (if they exist from a prior split attempt) |
| `PoLifecycleState.json` | Yes |
| `PoLifecycleState-M1.json` … `M4.json` | Yes |
| `ManualStationState\` (entire folder) | Yes if present (`EnableManualStationStateFiles` is false on mill templates; delete anyway) |

**Printer JSON** (`MillPrinterSettings.json`, `MillPrinterSettings-M1.json` … `M4.json`): mill-state JSON is no longer used. **Delete leftover printer JSON** along with runtime/lifecycle JSON. Printer IPs live in `dbo.Mill_Printer` (seed all four mills at `192.168.0.125:9100`). Shared Settings `PUT /api/Settings/printers` writes SQL; mill-n re-reads within ~2s. Mill-1 falls back to `NdtTagPrinterAddress` only when its SQL row is missing. Mills 2–4 have **no** fallback to another mill's printer.

Shared dashboard and mill instances share the same `Mill_Printer` table. Do not keep per-process JSON copies.

#### 2.5 Skip leftover SAP slit files (`MinSourceFileLastWriteUtc`)

`Input Slit` and `Input Slit Accepted` stay populated (read-only SAP sources). The mill **never** moves or deletes them.

Ingest reads **only** `Input Slit`. `Input Slit Accepted` is dashboard / running-PO only.

Production `BackfillLookbackHours` is **48**. After the SQL wipe, `Input_Slit_Row` is empty, so any inbox file whose `LastWriteTimeUtc` is within 48 hours **and** on or after `MinSourceFileLastWriteUtc` is queued as a new slit. Leave the mill floor **one second after the newest leftover** in `Input Slit`.

Mill-1..4 templates currently use `"MinSourceFileLastWriteUtc": "2026-08-30T12:53:57Z"` (newest leftover at generate time was `12:53:56Z`). **Re-measure immediately before mill start** and bump all four mill files if a newer leftover exists:

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

Run against `JazeeraMES_Prod` in SSMS, **after** the DELETE in step 2.1 so `Mill_Sequence` seeds from an empty `NDT_Bundle`. All scripts are additive / IF-missing. Skip a file only if you have already applied it in this session.

**Ordering that matters**

- `NDT_Bundle_Alter_CsvFill.sql` **must run before** `NDT_Bundle_Alter_Voided.sql`. CsvFill creates `Csv_Fill_State` and `CK_NDT_Bundle_Csv_Fill_State` **without** `'Voided'`. Voided **drops** that CHECK (if present) and recreates it **with** `'Voided'`. If you reverse them, Voided’s CHECK add fails because `Csv_Fill_State` is missing. If you run CsvFill **after** Voided, CsvFill will **not** replace the CHECK (it only adds when missing) — Voided’s list is the one you want, so CsvFill-then-Voided is required.
- `App_Setting_AddTable.sql` has **no** dependency on the CsvFill CHECK. It can sit anywhere after the database exists. Keep it with the five-instance scripts (before mill start).
- `Mill_Sequence.sql` needs `NDT_Bundle` to exist (already does). After a wipe, seed is `0` for mills 1–4.
- `Ppc_Correction_Item_Alter_ReplacementBatch.sql` needs `Ppc_Correction_Item` (already in prod). Run after that table exists.
- `Mill_Instance_Lease.sql` is independent; needed before Mill-1..4 start.

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

**B. The three commits — run in this order**

12. `docs/NDT_Bundle_Alter_CsvFill.sql` (`a91efbd`) — columns `Target_Ndt_Pcs`, `Csv_Filled`, `Csv_Fill_State`, `Csv_Last_Row_AtUtc`; CHECK **without** Voided; tables `NDT_Csv_Fill_Hold`, `NDT_Csv_Fill_Audit`; backfill UPDATE is a no-op on empty `NDT_Bundle`.
13. `docs/App_Setting_AddTable.sql` (`d22b620`) — `App_Setting` + row `ZplPhysicalPrintEnabled = true`. Independent of the CHECK.
14. `docs/Mill_Instance_Lease.sql` (`d22b620`) — `Mill_Instance_Lease` (empty until mill claim).
15. `docs/Mill_Sequence.sql` (`9e79619`) — `Mill_Sequence` + `Mill_Sequence_Audit`; insert mills 1–4 with `Current_Sequence = 0` (empty live max).
16. `docs/NDT_Bundle_Alter_Voided.sql` (`9e79619`) — Voided columns; **drop + recreate** `CK_NDT_Bundle_Csv_Fill_State` including `'Voided'`.
17. `docs/Ppc_Correction_Item_Alter_ReplacementBatch.sql` (`9e79619`) — `Replacement_NDT_Batch_No`.
18. `docs/Bundle_Accumulation_AddTable.sql` — `Bundle_Accumulation` + `Bundle_Accumulation_Context` (open remainder; CHECK Pcs > 0).
19. `docs/Po_Lifecycle_AddTable.sql` — `Po_Lifecycle` + `Po_Lifecycle_Audit`.
20. `docs/Mill_Printer_AddTable.sql` — seed mills 1–4 at `192.168.0.125:9100`.

Confirm CHECK after 16:

```sql
SELECT definition
FROM sys.check_constraints
WHERE name = N'CK_NDT_Bundle_Csv_Fill_State';
-- must include N'Voided'
```

---

### 4. `Split-MillStateFiles.ps1`

**Deleted.** Runtime, lifecycle, and printer state are SQL tables (`Bundle_Accumulation`, `Po_Lifecycle`, `Mill_Printer`). There is no JSON split and no migration. If leftover `NdtBundleRuntimeState*.json` / `PoLifecycleState*.json` / `MillPrinterSettings*.json` remain under the NDT Input Slit folder, startup throws (fresh reset required).

---

### 5. Publish, instance config, `Install-NdtBundleInstances.ps1`

1. Publish `9e79619`:

   ```powershell
   dotnet publish src\NdtBundleService\NdtBundleService.csproj -c Release -o C:\Apps\NdtBundleService\bin
   ```

2. Copy `deploy\instances\shared`, `mill-1`, `mill-2`, `mill-3`, `mill-4` into `C:\Apps\NdtBundleService\instances\`.

3. Confirm mill templates no longer point at `NdtBundleRuntimeState-M{n}.json` / `MillPrinterSettings-M{n}.json`. Leftover files under `\\10.2.20.210\pas-sap\To SAP\TM\NDT\NDT Input Slit` must be deleted or startup fails.

4. Confirm overlays (already in templates):

   | Instance | Port | Role |
   |---|---|---|
   | Shared | `http://*:5000` | Dashboard, PO import, SAP-status watcher, upload scheduler. `RequireCleanFillCutover=false`. No mill workers. |
   | Mill-n | `http://127.0.0.1:500n` | Workers for mill n only. `RequireCleanFillCutover=true`. |

5. Mill-1 `MillCsvBatchMode` = FillToTarget. Mills 2–4 = Constant `10001`. Do not change for this cutover.

6. Service account must have UNC read/write and SQL Windows login to `JazeeraMES_Prod`. Mapped `Z:\` is invisible to Local System — UNC only.

7. As Administrator:

   ```powershell
   .\scripts\Install-NdtBundleInstances.ps1 -BasePath C:\Apps\NdtBundleService
   ```

   Creates/updates `NdtBundleService-Shared`, `NdtBundleService-M1`…`M4`, recovery restart 60s/60s/60s. **Does not start them.**

8. Disable or leave stopped the **old monolith** service so it cannot claim a mill. Two processes on the same mill = fatal lease fail.

9. Dashboard: point the UI at Shared `http://<host>:5000`. Mill ports 5001–5004 stay localhost-only.

---

### 6. Staged startup (mandatory order)

Start **one**, confirm logs, then the next. Do not start mills before Shared. Do not start M2 before M1 is green (lease/SQL/cutover). Logs:

Each mill instance writes to `C:\Apps\NdtBundleService\instances\mill-n\Logs\` (`ndtbundle-m{n}-.log`). Shared writes to `instances\shared\Logs\`.

Line prefix: `[Shared/-]` or `[Mill/n]`.

#### 6.1 Shared — `Start-Service NdtBundleService-Shared`

Must see:

- `SQL traceability configured for Server=… Database=JazeeraMES_Prod`
- `SQL traceability connected to …, database JazeeraMES_Prod`
- `SQL traceability table Mill_Sequence: 4 row(s).` (and other tables; `NDT_Bundle` 0)
- `PO_Plan_WIP folder import starting (folder \\10.2.20.210\pas-sap\From SAP\TMFG_TMWIP\PO Accepted; …)`
- `PO_Plan_WIP folder import finished: scanned …`
- `PO plan caches warmed on startup.`
- `NDT Input Slit SAP status watcher active. Pending: …\NDT Input Slit\Input Slit; Accepted: …; Rejected: …`

Must **not** see:

- `PlcHandshakeWorker starting`
- `SlitMonitoringWorker started`
- `Claimed Mill_Instance_Lease`
- `Fill-to-target cutover blocked`

If `SQL traceability tables missing … Mill_Sequence`, stop and finish step 3.

#### 6.2 Mill-1 — `Start-Service NdtBundleService-M1`

Must see (`[Mill/1]`):

- Same SQL connected lines (or at least no “not reachable”)
- `Mill_Sequence mill 1 seeded Current_Sequence=…` **only if** the row was missing. After step 3 seed, the row exists → you will **not** see `seeded`; that is OK.
- `Mill_Sequence startup guard passed (mill 1)`
- `Fill-to-target cutover check passed (mill 1)`
- `Claimed Mill_Instance_Lease for mill 1 (InstanceId=…, TTL=45s).`
- `NDT runtime state initialized (0 PO/mill slot(s)). Bundle sequence is allocated from Mill_Sequence at close.` (or loaded 0 slots)
- `PlcHandshakeWorker starting 1 mill loop(s) (default poll 500ms).`
- `Mill-1 (Mill 1): PoEndSource=Plc — …`
- `SlitMonitoringWorker started. Watching folder \\10.2.20.210\pas-sap\To SAP\TM\Input Slit`
- `Input Slit reconcile: … queued for backfill 0 …`

Must **not** see:

- `Fill-to-target cutover blocked`
- `Mill_Sequence for mill 1 is …; live bundles go to …`
- `Mill_Instance_Lease claim failed for mill 1: already owned`
- `bundle close failed: could not allocate sequence`

If the old monolith is still running, lease claim fails. Stop the monolith, wait 45s or `DELETE FROM dbo.Mill_Instance_Lease`, start M1 again.

#### 6.3 Mill-2 — `Start-Service NdtBundleService-M2`

Same as M1 with mill **2**, plus:

- `PlcHandshake skipped for Mill-2 (Mill 2): PlcHandshakeEnabled=false — no S7 connection (PoEndSource=Plc).`

(`PlcHandshake.Enabled` is true on the mill template so the worker starts, then skips S7 because `PlcHandshakeEnabled=false`.)

Guard + cutover + lease for mill 2. Reconcile queued 0.

#### 6.4 Mill-3 — `Start-Service NdtBundleService-M3`

Same pattern, mill **3**, handshake skipped (`PlcHandshakeEnabled=false`, `PoEndSource=Plc`).

#### 6.5 Mill-4 — `Start-Service NdtBundleService-M4`

Same pattern, mill **4**:

- Handshake skipped (`PlcHandshakeEnabled=false`, `PoEndSource=File`)
- `No TcpOpen mills configured — TCP transport idle.` (mill-4 overlay mill entry has no `TcpOpenPort`; the instance `Mills` array replaces the base list). File PO-end is enough for mill 4.

---

### 7. Post-reset `Mill_Sequence` verification (before first close)

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
```

**Clean result after this reset (year 2026):**

| Mill_No | Current_Sequence | LiveMax | LiveMaxBundleNo |
|---|---|---|---|
| 1 | 0 | NULL | NULL |
| 2 | 0 | NULL | NULL |
| 3 | 0 | NULL | NULL |
| 4 | 0 | NULL | NULL |

Four rows, nothing else. `Updated_By` is `Migration` (SQL seed) or `StartupSeed` (app inserted a missing row).

First close on mill *n* is sequence **1**, tag `12` + `26` + mill digit + `00001` (mill 1: `1226100001`).

If `Current_Sequence` is not 0, **do not close**. You seeded before the DELETE, or leftover bundles remain. Fix table, then start.

---

### 8. First-PO end-to-end

Pick a mill that will actually run (typically mill 1). Confirm `PO_Plan_WIP` has that PO + mill + pipe size (Shared import). Formation chart has that size.

1. Drop **one** new Input Slit file for that mill/PO into `Input Slit` (after lookback/archive is clean).
2. Mill log: slit ingested; `sizeCounts` persist; no allocate until threshold (mill 1 fill-to-target) or PLC slit-end (mill 1 handshake).
3. On first successful close:
   - `Mill_Sequence.Current_Sequence` for that mill = **1**
   - `NDT_Bundle` one row, `Bundle_No` = `1226n00001`, `Print_Status` = `Printed` (or `Pending`/`PrintFailed` if printer down — SQL row must still exist)
   - `Target_Ndt_Pcs` set on mill 1; mills 2–4 output batch `10001` (constant)
   - Tag at the mill printer
4. More slits until PO end (PLC trigger mill 1; file/TCP mill 4). Remainder bundle closes (`sizeCounts > 0` at PO-end). `Current_Sequence` steps by one per close. No `bundle close failed: could not allocate sequence`. No leftover `sizeCounts` after PO-end with SQL up.
5. Shared: SAP-status watcher sees the new output basename as Pending until SAP moves it (SAP is **not** picking up yet — file stays in pending; that is OK).

If mill 1 cutover check failed on leftover JSON, you skipped step 2.4.

---

### 9. Rollback (back to `69d0d75`)

The three commits add tables/columns the old binary does not need, but fill-to-target CHECKs and Voided CHECK can confuse a downgrade if you only swap the exe. **Restore the step-1 database backup.**

1. `Stop-Service NdtBundleService-M1, NdtBundleService-M2, NdtBundleService-M3, NdtBundleService-M4`
2. `Stop-Service NdtBundleService-Shared`
3. Restore `JazeeraMES_Prod` from the step-1 backup (this removes `Mill_Sequence`, `Mill_Instance_Lease`, `App_Setting`, CsvFill/Voided columns, fill hold/audit, etc., and puts data back).
4. Replace `C:\Apps\NdtBundleService\bin` with the `69d0d75` publish. Restore the old monolith content root / `appsettings`.
5. Start the **old monolith** Windows Service only. Leave the five new services stopped (or `sc.exe delete` them after rollback is proven).
6. If you did **not** restore the DB and only swapped the exe: old monolith will not use `Mill_Sequence`; it will number from JSON again. That can **collide** with any `NDT_Bundle` rows the new build inserted. Do not do a binary-only rollback after any mill has closed a bundle. Restore the DB.
7. `DELETE FROM dbo.Mill_Instance_Lease` is only needed if you did not restore the DB and a mill service still holds a row (TTL 45s). After a DB restore the table is gone or empty.

Folder archive from step 2.3 stays archived unless you need those files back in the inboxes.

---

## Quick abort signs (stop the fleet)

- Any mill: `Fill-to-target cutover blocked`
- Any mill: `Mill_Sequence for mill N is X; live bundles go to Y`
- Any mill: `Mill_Instance_Lease claim failed`
- Any mill: `queued for backfill` > 0 right after this reset
- Shared: missing `Mill_Sequence` in SQL health
- `CK_NDT_Bundle_Csv_Fill_State` insert error mentioning `Voided` → Voided script did not run after CsvFill
