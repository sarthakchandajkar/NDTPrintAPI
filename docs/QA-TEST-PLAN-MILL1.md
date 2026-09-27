# QA Test Plan — Mill-1 (manual, pre–roll-change)

Checklist for a person against a **running mill-1-test** instance. This is not an automated test suite.

| | |
|---|---|
| **Instance** | mill-1-test (Mill-1 workers only, `OwnedMillNos: [1]`) |
| **Database** | `JazeeraMES_Test` |
| **Folders** | under `C:\NdtTest\` (see Environment) |
| **Optional PLC** | bench S7-300 running FC120 Networks 79/80 |
| **Year** | 2026 → first Mill-1 tag is **`1226100001`** (`12` + `YY` + mill `1` + 5-digit seq) |

**Priority:** run **Section A** then **Section B** before live rollout. **Section C** only when the bench PLC is ready — each C case is skippable independently.

**Pass rule:** every Expected result row must match the stated SQL / log / UI string. Do not mark PASS on “looks OK.”

---

## Environment

### Assumed mill-1-test paths

Confirm these (or equivalent) in the instance `appsettings` before starting:

| Config key | Assumed value |
|---|---|
| `NdtBundle:SqlDatabase` / connection | `JazeeraMES_Test` |
| `NdtBundle:InputSlitFolder` | `C:\NdtTest\Input Slit` |
| `NdtBundle:InputSlitAcceptedFolder` | `C:\NdtTest\Input Slit Accepted` |
| `NdtBundle:OutputBundleFolder` | `C:\NdtTest\NDT Input Slit\Input Slit` |
| `NdtBundle:FormationChartCsvPath` | `C:\NdtTest\FormationChart.csv` (copy of bundled chart) |
| `NdtBundle:InputSlitProcessMills` | `[1]` |
| `NdtBundle:MillCsvBatchMode:1:Mode` | `FillToTarget` |
| `NdtBundle:InitialMillBatchNumbers:1` | `1226100000` |
| `NdtBundle:PlcCloseGraceSeconds` | `60` |
| `NdtBundle:CsvFillQuietMinutes` | `180` (lower temporarily for A7 only) |
| `NdtBundle:PlcCsvDiscrepancyReviewThresholdPercent` | `20` |
| Logs | instance log folder (e.g. `C:\Apps\NdtBundleService\instances\mill-1-test\Logs`) |

### Section A / B close-trigger config (no PLC required)

For **A** and **B**, set either:

- `NdtBundle:CloseTrigger` = **`File`**, or  
- `CloseTrigger` = `PlcWithFileFallback` **and** `PlcHandshake:Enabled` = **`false`** (S7 treated unhealthy → file path allowed immediately)

With that config, a size reaching Formation Chart threshold **closes on the next poll** (default `PollIntervalSeconds` = 5). It does **not** wait 60 seconds.

> **60s grace note:** The log line containing `PlcCloseGraceSeconds safety-net` fires only when `CloseTrigger=PlcWithFileFallback`, S7 is **healthy**, and PLC never closes while file-side count ≥ threshold for ≥ 60s. That path is covered under **C6** (healthy-missed) / optional A1-grace. Unhealthy / disconnected S7 does **not** wait 60s — it allows immediate file-path close.

### Formation Chart (default test size)

Use pipe size **`6`** → **`RequiredNdtPcs = 20`** (from `FormationChart.csv`). Put `6` in column **`NDT Short Length Pipe`**.

### Sample Input Slit CSV template

Drop files into `C:\NdtTest\Input Slit`. Filename pattern (basename preserved on output):

`{coil}_{slitNo}_{yyMMdd}_{po}.csv`  
Example: `QA0001_01_260906_1000099991.csv`

Required header (exact names; aliases noted in code for PO / Mill only):

```text
PO Number,Slit No,NDT Pipes,Rejected P,Slit Start Time,Slit Finish Time,Mill No,NDT Short Length Pipe,Rejected Short Length Pipe
```

Example data row (10 NDT pcs, mill 1, size 6):

```text
1000099991,01,10,0,2026-09-06 08:00:00,2026-09-06 08:05:00,1,6,0
```

**Output CSV** (same basename under `OutputBundleFolder`) appends column **`NDT Batch No`** (spaces, not underscores).

### Touch file timestamps

If `MinSourceFileLastWriteUtc` is set in config, ensure each dropped file’s LastWrite is **after** that cutoff (copy/create fresh files; do not reuse ancient samples).

### Dashboard / API

| | |
|---|---|
| Dashboard | mill-1-test / shared dashboard URL pointing at this instance |
| Reconcile | `/reconcile` → **Reconcile Bundle** |
| Settings | `/settings` (password gate) → tabs **Mill sequence** / **Printers** |
| Logs | Search instance `ndtbundle-*.log` (or console) for the quoted strings below |

### Bench PLC (Section C only)

| Signal | Address |
|---|---|
| PO-change trigger | **M40.6** |
| MES ack | **M40.7** |
| LineRun | **DB250.DBX2.0** |
| Slit1.PO_ID / Slit2.PO_ID | **DB50.DBW0** / **DB50.DBW8** (ladder only; MES does not read these) |
| NDT count | **DB251** DBW @ offset **6** |
| Slit ID | **DB251** DBW @ offset **10** |
| Host (prod mill-1; point test config at bench) | typically `192.168.0.13`, rack 0, slot 2, S7300 |

Ladder N79 latches M40.6 when LineRun ∧ Slit2.PO_ID ≥ 1 ∧ Slit1.PO_ID ≠ Slit2.PO_ID. N80 clears M40.6 on M40.7 rising edge (~T11 ≈ 5s).

---

## Reset between tests (R0)

Run **before every case** (and after A6 / B5 Force experiments that leave sequence dirty). Stop the service first if a case left it crashed.

### R0.1 — SQL clean (Mill-1 scoped where safe)

```sql
USE JazeeraMES_Test;
GO

-- Children / generated first (FK: Output_Slit_Row → NDT_Bundle)
DELETE FROM dbo.Output_Slit_Row
WHERE NDT_Batch_No IN (
  SELECT Bundle_No FROM dbo.NDT_Bundle WHERE Mill_No = 1
)
OR NDT_Batch_No LIKE N'12261%'
OR NDT_Batch_No LIKE N'12261%V';

DELETE FROM dbo.NDT_Csv_Fill_Hold WHERE Mill_No = 1;          -- skip if table/column missing
DELETE FROM dbo.NDT_Csv_Fill_Audit WHERE Bundle_No LIKE N'12261%'; -- skip if missing

DELETE FROM dbo.Input_Slit_Row WHERE Mill_No = 1;
DELETE FROM dbo.Input_Slit_File_Seen;                         -- or filter by path under C:\NdtTest\

IF OBJECT_ID(N'dbo.Handshake_Event', N'U') IS NOT NULL
  DELETE FROM dbo.Handshake_Event WHERE Mill_No = 1;

DELETE FROM dbo.NDT_Bundle WHERE Mill_No = 1;

IF OBJECT_ID(N'dbo.Bundle_Accumulation', N'U') IS NOT NULL
  DELETE FROM dbo.Bundle_Accumulation WHERE Mill_No = 1;
IF OBJECT_ID(N'dbo.Bundle_Accumulation_Context', N'U') IS NOT NULL
  DELETE FROM dbo.Bundle_Accumulation_Context WHERE Mill_No = 1;

IF OBJECT_ID(N'dbo.Mill_Sequence_Audit', N'U') IS NOT NULL
  DELETE FROM dbo.Mill_Sequence_Audit WHERE Mill_No = 1;

-- Seed Mill-1 sequence to 0 so next allocate → 1226100001
IF OBJECT_ID(N'dbo.Mill_Sequence', N'U') IS NOT NULL
BEGIN
  IF EXISTS (SELECT 1 FROM dbo.Mill_Sequence WHERE Mill_No = 1)
    UPDATE dbo.Mill_Sequence
    SET Current_Sequence = 0,
        Updated_By = N'QA-R0',
        Reason = N'Reset between tests',
        Updated_AtUtc = SYSUTCDATETIME()
    WHERE Mill_No = 1;
  ELSE
    INSERT INTO dbo.Mill_Sequence (Mill_No, Current_Sequence, Updated_By, Reason)
    VALUES (1, 0, N'QA-R0', N'Reset between tests');
END
GO
```

### R0.2 — Confirm clean

```sql
SELECT COUNT(*) AS Bundles FROM dbo.NDT_Bundle WHERE Mill_No = 1;           -- expect 0
SELECT Current_Sequence FROM dbo.Mill_Sequence WHERE Mill_No = 1;           -- expect 0
SELECT COUNT(*) AS AccRows FROM dbo.Bundle_Accumulation WHERE Mill_No = 1;  -- expect 0
```

### R0.3 — Folders

```powershell
# Stop service first if files are locked
Remove-Item "C:\NdtTest\Input Slit\*" -Force -ErrorAction SilentlyContinue
Remove-Item "C:\NdtTest\NDT Input Slit\Input Slit\*" -Force -ErrorAction SilentlyContinue
# Do NOT leave legacy mill-state JSON under OutputBundleFolder or its parent:
# NdtBundleRuntimeState*.json, PoLifecycleState*.json, MillPrinterSettings*.json
Get-ChildItem "C:\NdtTest\NDT Input Slit" -Recurse -Filter "*.json" | Remove-Item -Force
```

### R0.4 — Start service

Start mill-1-test. Confirm log contains slit worker start (no leftover-JSON / Mill_Sequence refuse). Proceed to the next case.

---

# Section A — File-driven (no PLC) — MUST RUN

**Config:** `CloseTrigger=File` **or** `PlcHandshake:Enabled=false`. Do not rely on bench PLC.

Use PO **`1000099991`**, Mill **`1`**, size **`6`** (threshold **20**) unless a case says otherwise.

---

## A1 — First bundle close → `1226100001`

| | |
|---|---|
| **ID** | A1 |
| **Proves** | First Mill-1 close allocates tag `1226100001`, stamps output CSV, advances `Mill_Sequence` to 1, clears `Bundle_Accumulation`. |
| **Precondition** | R0 complete. Service running. `Current_Sequence = 0`. Empty accumulation. |

**Steps**

1. Drop two slit files for PO `1000099991`, mill `1`, size `6`, **10** NDT pcs each (sum = 20 = threshold). Unique basenames (e.g. `QA_A1_01_…csv`, `QA_A1_02_…csv`).
2. Wait one poll cycle (~5–15s) after the second file appears.
3. Run the SQL and CSV checks below.

**Expected (verifiable)**

| Check | Exact expectation |
|---|---|
| Bundle row | `Bundle_No = '1226100001'`, `Mill_No = 1`, `PO_Number = '1000099991'`, `Total_NDT_Pcs = 20`, `Target_Ndt_Pcs = 20`, `Csv_Filled = 0` initially then stamps as later files arrive; after A1 close alone expect fill init `PlcClosed` / target 20 |
| Sequence | `Mill_Sequence.Current_Sequence = 1` |
| Accumulation | **0** rows for Mill_No = 1 |
| Output CSV | Same basename(s) under `C:\NdtTest\NDT Input Slit\Input Slit\` with column **`NDT Batch No`** = `1226100001` on stamped rows |
| Log | Contains: `Closing size-based bundle for PO 1000099991 Mill 1 Size 6 threshold=20 total=20` (file path; may include `(includes slit overshoot)`) |
| Log | Contains: `Initialized fill target for bundle 1226100001: Target_Ndt_Pcs=20` |

**Confirm SQL**

```sql
SELECT Bundle_No, Total_NDT_Pcs, Target_Ndt_Pcs, Csv_Filled, Csv_Fill_State, Close_Source
FROM dbo.NDT_Bundle WHERE Mill_No = 1;
-- Expect: Bundle_No=1226100001, Total_NDT_Pcs=20, Target_Ndt_Pcs=20

SELECT Current_Sequence FROM dbo.Mill_Sequence WHERE Mill_No = 1;
-- Expect: 1

SELECT * FROM dbo.Bundle_Accumulation WHERE Mill_No = 1;
-- Expect: empty
```

**Confirm CSV / log**

```powershell
Select-String -Path "C:\NdtTest\NDT Input Slit\Input Slit\QA_A1*.csv" -Pattern "1226100001"
# Log (file close — NOT the 60s grace WRN under Section A config):
# Closing size-based bundle for PO 1000099991 Mill 1 Size 6 threshold=20 total=20
```

**Optional A1-grace (bench healthy, skip if no PLC):** With `CloseTrigger=PlcWithFileFallback`, S7 healthy, and **no** PLC slit-end/PO close, feed sum ≥ 20, wait **≥ 60s**. Expect WRN:

`Missed PLC close for PO 1000099991 Mill 1 Size 6: file-side count … ≥ threshold 20 for 60s with healthy S7; executing file-driven close (PlcCloseGraceSeconds safety-net).`

---

## A2 — Partial fill (`CsvFilling`)

| | |
|---|---|
| **ID** | A2 |
| **Proves** | After a closed bundle exists, files below remaining target stamp partially: `CsvFilling`, no terminal close of fill, pointer not advanced to next incomplete. |
| **Precondition** | R0, then complete **A1** (or equivalent: open fill target `1226100001` with `Target_Ndt_Pcs=20`, `Csv_Filled=0`, `Csv_Fill_State` incomplete). |

**Steps**

1. Drop **one** new slit file with **NDT Pipes = 8** (same PO/mill/size), unique basename `QA_A2_01_….csv`.
2. Wait one poll.
3. Confirm fill state; confirm **no** new bundle `1226100002`.

**Expected**

| Check | Exact expectation |
|---|---|
| SQL | `Csv_Filled = 8`, `Target_Ndt_Pcs = 20`, `Csv_Fill_State = 'CsvFilling'`, `Count_Discrepancy = 0` |
| Bundles | Still only `1226100001` (no `1226100002`) |
| Log | `CSV fill stamp for bundle 1226100001: +8 → filled=8/20 state=CsvFilling.` |

**Confirm SQL**

```sql
SELECT Bundle_No, Csv_Filled, Target_Ndt_Pcs, Csv_Fill_State, Count_Discrepancy
FROM dbo.NDT_Bundle WHERE Bundle_No = N'1226100001';
-- Expect: 8, 20, CsvFilling, 0

SELECT Bundle_No FROM dbo.NDT_Bundle WHERE Mill_No = 1 AND Bundle_No = N'1226100002';
-- Expect: 0 rows
```

---

## A3 — Fill to target (`CsvComplete`) → next bundle `1226100002`

| | |
|---|---|
| **ID** | A3 |
| **Proves** | Remaining capacity stamps to exact target → `CsvComplete`; next size close allocates `1226100002`. |
| **Precondition** | After A2: `1226100001` has `Csv_Filled=8`, `Target=20`. Or R0 + A1 + one 8-pc stamp. |

**Steps**

1. Drop file with **NDT Pipes = 12** (`QA_A3_fill_….csv`) → fills 8+12=20.
2. Wait for stamp; confirm `CsvComplete`.
3. Drop two more files totaling **20** pcs (10+10) to force a **new** size close.
4. Confirm new tag `1226100002` and `Current_Sequence = 2`.

**Expected**

| Check | Exact expectation |
|---|---|
| After step 1 | `Csv_Filled = 20`, `Csv_Fill_State = 'CsvComplete'`, `Count_Discrepancy = 0` |
| Log | `CSV fill stamp for bundle 1226100001: +12 → filled=20/20 state=CsvComplete.` |
| After step 3 | New row `Bundle_No = '1226100002'`, `Current_Sequence = 2` |
| Output | New/remaining output rows show `NDT Batch No` = `1226100002` |

**Confirm SQL**

```sql
SELECT Bundle_No, Csv_Filled, Target_Ndt_Pcs, Csv_Fill_State
FROM dbo.NDT_Bundle WHERE Bundle_No = N'1226100001';
-- Expect: 20, 20, CsvComplete

SELECT Bundle_No, Total_NDT_Pcs, Target_Ndt_Pcs
FROM dbo.NDT_Bundle WHERE Bundle_No = N'1226100002';
-- Expect: one row, totals 20

SELECT Current_Sequence FROM dbo.Mill_Sequence WHERE Mill_No = 1;
-- Expect: 2
```

---

## A4 — Overshoot (whole file, never split)

| | |
|---|---|
| **ID** | A4 |
| **Proves** | A file larger than remaining capacity stamps **entirely** onto the current bundle → `CsvOvershoot`, `Count_Discrepancy = 1`, fill pointer advances so the next incomplete can start. |
| **Precondition** | R0 + close a bundle with `Target_Ndt_Pcs = 20`, `Csv_Filled = 0` (A1). Do **not** partially fill first. |

**Steps**

1. Drop one file with **NDT Pipes = 25** (`QA_A4_….csv`) while `1226100001` is the incomplete target.
2. Wait one poll.

**Expected**

| Check | Exact expectation |
|---|---|
| SQL | `Csv_Filled = 25`, `Target_Ndt_Pcs = 20`, `Csv_Fill_State = 'CsvOvershoot'`, `Count_Discrepancy = 1` |
| Output CSV | Entire file’s rows carry `NDT Batch No = 1226100001` (no split across two batch numbers) |
| Log | `CSV fill overshoot for bundle 1226100001: target=20 filled=25.` |
| Log | Stamp line with `state=CsvOvershoot` |
| Manual_Review | `1` if \|25−20\|/20 × 100 = 25% **>** `PlcCsvDiscrepancyReviewThresholdPercent` (default 20) |

**Confirm SQL**

```sql
SELECT Bundle_No, Csv_Filled, Target_Ndt_Pcs, Csv_Fill_State, Count_Discrepancy, Manual_Review
FROM dbo.NDT_Bundle WHERE Bundle_No = N'1226100001';
-- Expect: 25, 20, CsvOvershoot, 1, 1
```

---

## A5 — Restart mid-accumulation (SQL survives process recycle)

| | |
|---|---|
| **ID** | A5 |
| **Proves** | `Bundle_Accumulation` persists across service stop/start; remaining slits close the same bundle correctly. **Not previously proven outside unit tests.** |
| **Precondition** | R0. Service running. |

**Steps**

1. Drop **one** file with **NDT Pipes = 12** (below threshold 20). Unique basename `QA_A5_part_….csv`.
2. Wait one poll. Confirm accumulation **before** restart:

```sql
SELECT Mill_No, Po_Number, Size_Key, Pcs
FROM dbo.Bundle_Accumulation WHERE Mill_No = 1;
-- Expect: Pcs = 12, Size_Key related to 6, Po_Number = 1000099991
```

3. **Stop** mill-1-test Windows service / process completely.
4. Re-run the accumulation SELECT — still `Pcs = 12`.
5. **Start** the service.
6. Drop a second file with **NDT Pipes = 8** (`QA_A5_rest_….csv`).
7. Wait for close.

**Expected**

| Check | Exact expectation |
|---|---|
| After restart, before step 6 | Accumulation still `Pcs = 12` (hydrated from SQL) |
| After step 6 | Bundle `1226100001`, `Total_NDT_Pcs = 20` |
| Accumulation | **empty** after close |
| Sequence | `Current_Sequence = 1` |
| Log | Close log for PO/mill/size with `total=20` |

**Confirm SQL (final)**

```sql
SELECT Bundle_No, Total_NDT_Pcs FROM dbo.NDT_Bundle WHERE Mill_No = 1;
-- Expect: 1226100001 / 20

SELECT COUNT(*) FROM dbo.Bundle_Accumulation WHERE Mill_No = 1;
-- Expect: 0
```

---

## A6 — Startup guards (legacy JSON + Mill_Sequence behind live)

| | |
|---|---|
| **ID** | A6 |
| **Proves** | Startup refuses leftover mill-state JSON (names path) and refuses when `Mill_Sequence` lags live bundles. |
| **Precondition** | Service **stopped**. |

### A6a — Legacy JSON

**Steps**

1. Create file:  
   `C:\NdtTest\NDT Input Slit\Input Slit\NdtBundleRuntimeState-M1.json`  
   (content can be `{}`).
2. Start the service.

**Expected**

| Check | Exact expectation |
|---|---|
| Process | Fails / exits; does not stay healthy |
| Log ERR | `Leftover mill-state JSON found (no migration):` … includes the full path ending in `NdtBundleRuntimeState-M1.json` |
| Exception text | `Leftover mill-state JSON found (fresh reset required, no migration):` + path(s) |

**Cleanup before A6b:** delete the JSON; do not leave it for later tests.

### A6b — Mill_Sequence behind live max

**Steps**

1. With service stopped and no leftover JSON, insert a live Mill-1 bundle and set sequence behind it:

```sql
USE JazeeraMES_Test;
GO
-- Minimal live bundle (adjust columns if your schema requires more NOT NULL fields)
INSERT INTO dbo.NDT_Bundle (Bundle_No, PO_Number, Mill_No, Total_NDT_Pcs, PrintedAt /* + required cols */)
VALUES (N'1226100005', N'1000099991', 1, 20, SYSUTCDATETIME());
-- If insert fails, copy column list from an existing prod row shape / prior A1 row.

UPDATE dbo.Mill_Sequence
SET Current_Sequence = 2, Updated_By = N'QA-A6', Reason = N'Behind live max'
WHERE Mill_No = 1;
-- Live max seq from 1226100005 is 5 > 2
```

2. Start the service.

**Expected**

| Check | Exact expectation |
|---|---|
| Process | Startup fails |
| Exception / log | Contains: `Mill_Sequence for mill 1 is 2; live bundles go to 5 (1226100005). Sequence was allocated outside Mill_Sequence.` |

**Cleanup:** R0 (delete Mill-1 bundles, reset sequence to 0, clear folders), then start service cleanly.

---

## A7 — Quiet-period escape (`CsvShort`)

| | |
|---|---|
| **ID** | A7 |
| **Proves** | Incomplete fill past quiet window advances as `CsvShort` (flagged) so the next bundle is not blocked. |
| **Precondition** | R0. Temporarily set `NdtBundle:CsvFillQuietMinutes` to **`1`** (or `2`) in mill-1-test config and **restart** service. Restore to `180` after the case. |

**Steps**

1. Close first bundle (A1-style) → `1226100001`, `Target_Ndt_Pcs=20`.
2. Stamp **one** file with **8** pcs → `CsvFilling`, `Csv_Filled=8`.
3. Do **not** drop more files. Wait **> `CsvFillQuietMinutes`** (e.g. > 1 minute if set to 1) past `Csv_Last_Row_AtUtc`.
4. Confirm quiet advance, then feed 20 pcs for a new close → must allocate **`1226100002`** (not stuck on incomplete `1226100001`).

**Expected**

| Check | Exact expectation |
|---|---|
| SQL | `Csv_Fill_State = 'CsvShort'`, `Csv_Filled = 8`, `Target_Ndt_Pcs = 20`, `Count_Discrepancy = 1` |
| Manual_Review | `1` if \|8−20\|/20 = 60% > 20% threshold |
| Log | `CSV fill advanced CsvShort for bundle 1226100001: filled=8 target=20 forcePoEnd=…` |
| Next close | `1226100002` created |

**Confirm SQL**

```sql
SELECT Bundle_No, Csv_Filled, Target_Ndt_Pcs, Csv_Fill_State, Count_Discrepancy, Manual_Review, Csv_Last_Row_AtUtc
FROM dbo.NDT_Bundle WHERE Bundle_No = N'1226100001';
-- Expect: CsvShort, Count_Discrepancy=1

SELECT Bundle_No FROM dbo.NDT_Bundle WHERE Bundle_No = N'1226100002';
-- Expect: 1 row after next close
```

**Restore** `CsvFillQuietMinutes` to **180** and restart before Section B / production-like runs.

---

# Section B — Operator actions (dashboard) — MUST RUN

Never skip: these paths are not covered by browser E2E in CI. Use `/reconcile` and `/settings` against mill-1-test.

---

## B1 — Manual reconcile before any CSV fill

| | |
|---|---|
| **ID** | B1 |
| **Proves** | Operator corrects total before CSV stamps; target updates; tag reprints with Reprint; `Manual_Recon = 1`. |
| **Precondition** | R0 + A1-style close so `1226100001` exists with `Csv_Filled = 0`, `Total_NDT_Pcs = 20`. Printer reachable optional (note print fail separately). |

**Steps**

1. Open `/reconcile` → **Turn Reconcile ON**.
2. Reconcile type: **Output NDT Batch No. (current)**. Select / enter `1226100001`.
3. **Corrected total (NDT pipes)** = **18**.
4. Click **Reconcile & Reprint**.

**Expected**

| Check | Exact expectation |
|---|---|
| UI toast | `Bundle manually reconciled and tag reprinted.` (or `…but tag reprint failed.` if printer down) |
| Print message (if OK) | `Bundle tag (Reprint) sent to printer.` / ZPL contains `Reprint` |
| API/log | `Manual bundle reconcile for 1226100001: original=20 corrected=18 by=…` |
| SQL | `Manual_Recon = 1`, `Total_NDT_Pcs = 18`, `Target_Ndt_Pcs = 18`, `Manual_Recon_Original_Total = 20` |
| Flags | `Count_Discrepancy` / `Manual_Review` remain 0 when `Csv_Filled = 0` |

**Confirm SQL**

```sql
SELECT Bundle_No, Total_NDT_Pcs, Target_Ndt_Pcs, Manual_Recon, Manual_Recon_Original_Total,
       Csv_Filled, Count_Discrepancy, Manual_Review
FROM dbo.NDT_Bundle WHERE Bundle_No = N'1226100001';
-- Expect: 18, 18, 1, 20, 0, 0, 0
```

---

## B2 — Manual reconcile below already-filled

| | |
|---|---|
| **ID** | B2 |
| **Proves** | Correcting target **below** `Csv_Filled` sets discrepancy + manual review; later CSVs do **not** change locked `Total_NDT_Pcs`. |
| **Precondition** | R0 + bundle `1226100001` with `Target_Ndt_Pcs=20`, stamp files until `Csv_Filled = 15` (`CsvFilling`). |

**Steps**

1. On `/reconcile`, set corrected total to **10** (< 15 filled). **Reconcile & Reprint**.
2. Confirm flags immediately.
3. Drop another slit file that would stamp more NDT onto the PO (or attach late CSV). Wait one poll.
4. Confirm total still locked; `Post_Recon_Csv_Sum` may rise.

**Expected**

| Check | Exact expectation |
|---|---|
| After step 1 | `Manual_Recon=1`, `Total_NDT_Pcs=10`, `Target_Ndt_Pcs=10`, `Csv_Fill_State='CsvOvershoot'`, `Count_Discrepancy=1`, `Manual_Review=1` |
| After step 3 | `Total_NDT_Pcs` still **10**; `Post_Recon_Csv_Sum` may be **> 10** |

**Confirm SQL**

```sql
-- Immediately after reconcile:
SELECT Bundle_No, Total_NDT_Pcs, Target_Ndt_Pcs, Csv_Filled, Csv_Fill_State,
       Count_Discrepancy, Manual_Review, Manual_Recon
FROM dbo.NDT_Bundle WHERE Bundle_No = N'1226100001';
-- Expect: Total=10, CsvOvershoot, Count_Discrepancy=1, Manual_Review=1, Manual_Recon=1

-- After later CSVs:
SELECT Total_NDT_Pcs, Post_Recon_Csv_Sum, Manual_Recon
FROM dbo.NDT_Bundle WHERE Bundle_No = N'1226100001';
-- Expect: Total_NDT_Pcs unchanged at 10; Post_Recon_Csv_Sum may increase
```

---

## B3 — Merge into previous when source is mill-highest

| | |
|---|---|
| **ID** | B3 |
| **Proves** | Merging the highest live Mill-1 tag tombstones source as `…V`, updates target totals, rolls sequence back, next allocate **reuses** the source number. |
| **Precondition** | R0. Create two live bundles on same PO/mill, e.g. `1226100001` (target) then `1226100002` (source, mill-highest). `Current_Sequence = 2`. |

**Steps**

1. `/reconcile` → select source **`1226100002`**.
2. Preview shows **Merge into previous live bundle** targeting `1226100001`.
3. Enter **Reason** (required). Check **Confirm merge into 1226100001 (source will be voided)**.
4. Click **Merge into previous**.

**Expected**

| Check | Exact expectation |
|---|---|
| UI / API `sequenceMessage` | `Next bundle reuses 1226100002.` |
| Success text | `Merged 1226100002 into 1226100001 ({resultingTotal} pcs). Next bundle reuses 1226100002.` |
| Source row | `Bundle_No = '1226100002V'`, `Voided = 1`, `Original_Bundle_No = '1226100002'`, `Merged_Into_Bundle_No = '1226100001'`, `Csv_Fill_State = 'Voided'` |
| Sequence | `Current_Sequence = 1` (rolled back by 1) |
| Audit | `Mill_Sequence_Audit` row `Event_Type = 'MergeRollback'` |
| Next close | Allocates **`1226100002`** again (reuse) |

**Confirm SQL**

```sql
SELECT Bundle_No, Voided, Original_Bundle_No, Merged_Into_Bundle_No, Total_NDT_Pcs, Csv_Fill_State
FROM dbo.NDT_Bundle
WHERE Bundle_No IN (N'1226100001', N'1226100002V') OR Original_Bundle_No = N'1226100002';

SELECT Current_Sequence FROM dbo.Mill_Sequence WHERE Mill_No = 1;
-- Expect: 1

SELECT TOP 3 Event_Type, Old_Sequence, New_Sequence, Reason, AtUtc
FROM dbo.Mill_Sequence_Audit WHERE Mill_No = 1 ORDER BY AtUtc DESC;
-- Expect: Event_Type = MergeRollback
```

---

## B4 — Merge when a higher bundle exists (gap, no rollback)

| | |
|---|---|
| **ID** | B4 |
| **Proves** | Merging a non-highest source does **not** roll sequence; response names the gap and the next number. |
| **Precondition** | Three live bundles: `…001` (merge target), `…002` (merge source), `…003` (higher live). `Current_Sequence = 3`. |

**Steps**

1. Merge **`1226100002`** into **`1226100001`** (same UI as B3).
2. Record UI `sequenceMessage` and `Current_Sequence`.

**Expected**

| Check | Exact expectation |
|---|---|
| UI / API | `1226100002 will remain a gap; next bundle is 1226100004.` (next = Current_Sequence+1 → 4) |
| Sequence | `Current_Sequence` still **3** (no rollback) |
| Source | Tombstoned `1226100002V`, `Voided=1` |
| Higher bundle | `1226100003` still live |

**Confirm SQL**

```sql
SELECT Current_Sequence FROM dbo.Mill_Sequence WHERE Mill_No = 1;
-- Expect: 3 (unchanged)

SELECT Bundle_No, Voided FROM dbo.NDT_Bundle WHERE Bundle_No IN (N'1226100002V', N'1226100003');
-- Expect: 1226100002V Voided=1; 1226100003 Voided=0
```

---

## B5 — Sequence set: refuse without Force; apply with Force + reason

| | |
|---|---|
| **ID** | B5 |
| **Proves** | Setting sequence below live max without Force is refused; with Force + reason it applies and audits. |
| **Precondition** | At least one live Mill-1 bundle so live max ≥ 1 (e.g. after A1: live max = 1, `Current_Sequence = 1`). Settings login done. |

**Steps**

1. `/settings` → **Mill sequence**.
2. **Without** checking **Force below live max**: set Mill 1 **Set to** `0`, Reason `QA refuse`, click **Set**.
3. Observe error.
4. Check **Force below live max (second confirmation)**, Reason `QA force below live max`, **Set to** `0`, click **Set**.

**Expected**

| Check | Exact expectation |
|---|---|
| Step 2 API/UI | Refuse text: `Refuse to set mill 1 Current_Sequence to 0: live bundles go to 1. Pass forceBelowLiveMax=true with a second confirmation.` |
| Step 2 SQL | `Current_Sequence` unchanged; no new `Event_Type='Set'` audit for the refuse |
| Step 4 | Success: `Mill 1: sequence 1 → 0. Next bundle 1226100001.` (or equivalent next-bundle string) |
| Warning | May include: `Set below live max 1; next close may collide with an existing tag.` |
| Audit | `Event_Type = 'Set'`, `Old_Sequence = 1`, `New_Sequence = 0`, `Reason` = operator text, `Updated_By` ≈ `Settings` |

**Confirm SQL**

```sql
SELECT Current_Sequence, Updated_By, Reason FROM dbo.Mill_Sequence WHERE Mill_No = 1;
-- After Force: Current_Sequence = 0

SELECT TOP 5 Audit_ID, Old_Sequence, New_Sequence, Event_Type, Updated_By, Reason, AtUtc
FROM dbo.Mill_Sequence_Audit WHERE Mill_No = 1 ORDER BY AtUtc DESC;
-- Expect newest Event_Type = Set, New_Sequence = 0
```

**Cleanup:** R0 before next cases (Force below live max is hazardous for later A/B numbering).

---

## B6 — Printer settings hot-reload + blank station (no mill fallback)

| | |
|---|---|
| **ID** | B6 |
| **Proves** | Mill printer IP change takes effect within ~2s without service restart; blanking a station printer fails naming that station and does **not** fall back to the mill printer. |
| **Precondition** | Settings access. Known mill-1 printer row and station `VISUAL_REVISUAL`. |

**Steps**

1. `/settings` → **Printers**. Note current Mill-1 **IP / host**.
2. Change Mill-1 IP to a **different reachable** test printer (or intentional bad IP for negative check). **Save printers**.
3. Wait **≥ 2 seconds** (cache TTL). Click **Test** / **Print test tag** for Mill-1 — must use the **new** address (Wireshark / printer queue / log target IP).
4. Clear **Visual/Revisual** station **Address** (blank). **Save printers**.
5. Wait ≥ 2s. Trigger a Visual/Revisual test print / station tag from the UI.

**Expected**

| Check | Exact expectation |
|---|---|
| After step 2–3 | `dbo.Printer` for `Printer_Key=N'MILL_1'` shows new Address; traffic/logs use new IP within ~2s; **no service restart** |
| Save API | `Printer settings saved.` |
| Step 5 UI/API | Exact: `Printer not configured for Visual/Revisual` |
| Log | `Printer not configured for Visual/Revisual. Tag will not be sent for batch …` (or settings 400 appending `. Enter an address, click Save printers, then try Print test tag.`) |
| Fallback | Must **not** print via Mill-1 `Printer` `MILL_1` / `NdtTagPrinterAddress` |

**Confirm SQL**

```sql
SELECT Printer_Key, Address, Port FROM dbo.Printer WHERE Printer_Key = N'MILL_1';

SELECT Printer_Key, Address, Port
FROM dbo.Printer
WHERE Printer_Key = N'VISUAL_REVISUAL';
-- After blanking: Address empty / NULL / 0.0.0.0 per how UI saves “blank”
```

Restore working printer rows after the case.

---

# Section C — PLC scenarios — SKIP IF BENCH S7-300 NOT READY

**Config for C:** `CloseTrigger=PlcWithFileFallback`, `PlcHandshake:Enabled=true`, mill entry `PlcHandshakeEnabled=true`, bench IP/rack/slot correct, `RecoverLatchedTriggerAtStartup=true`, `RunPoEndWorkflowOnStartupRecovery=false`, `PollIntervalMs=500`, `StuckTriggerAlarmSeconds=30`, `PlcCloseGraceSeconds=60`.

Mark each case **SKIP** on the summary sheet if the bench is unavailable.

---

## C1 — Clean PO change (one rising edge, ack, clear)

| | |
|---|---|
| **ID** | C1 |
| **Proves** | Slit1 ≠ Slit2 with LineRun on produces exactly one M40.6 rising edge; MES acks within ~1s; M40.6 clears and stays clear; no stuck/idle-recovery warnings. |
| **Precondition** | Bench online, handshake armed, LineRun ON, M40.6 currently FALSE. |

**Steps**

1. Ensure LineRun **DB250.DBX2.0 = 1**.
2. Set Slit2.PO_ID (DB50.DBW8) to a valid ≥ 1 value different from Slit1.PO_ID (DB50.DBW0) so N79 latches **M40.6**.
3. Watch MES logs and PLC bits for ~10s.
4. Confirm M40.6 returns FALSE and stays FALSE without further PO change.

**Expected**

| Check | Exact expectation |
|---|---|
| Log (order) | `Mill-1: PO change rising edge detected on M40.6 — Mill 1, PO_Id …, NDT …, CorrelationId ….` |
| Log | `Mill-1: MES ack sent (TRUE) — Mill 1, …` within **~1s** of rising edge (same poll; typically ≪1s) |
| Log | `Mill-1: PO change event enqueued — …` |
| Log | `Mill-1: trigger M40.6 cleared by PLC.` then `Mill-1: wrote ack M40.7=FALSE — handshake complete.` |
| Absent | **No** `stuck trigger alarm — M40.6 TRUE for >30s…` |
| Absent | **No** `idle stuck trigger recovery — M40.6 TRUE for >30s…` |
| PLC | Exactly **one** FALSE→TRUE on M40.6 for this change |

---

## C2 — Fast re-latch after ack

| | |
|---|---|
| **ID** | C2 |
| **Proves** | After a completed handshake, a second Slit1≠Slit2 change is detected and acked (not deaf). |
| **Precondition** | C1 just completed; handshake armed (`MinimumTriggerFalsePollsBeforeRearm=1`). |

**Steps**

1. Immediately change Slit registers again so N79 latches M40.6 a second time.
2. Confirm a **second** rising-edge + ack pair with a **new** CorrelationId.

**Expected**

| Check | Exact expectation |
|---|---|
| Log | Second `PO change rising edge detected on M40.6` with new `CorrelationId` |
| Log | Second `MES ack sent (TRUE)` |
| Log | Second `wrote ack M40.7=FALSE — handshake complete.` |

---

## C3 — Startup with M40.6 latched

| | |
|---|---|
| **ID** | C3 |
| **Proves** | Latched trigger at startup is cleared with MES ack only; PO-end workflow is **not** run. |
| **Precondition** | Service **stopped**. Force **M40.6 = TRUE** on bench. `RunPoEndWorkflowOnStartupRecovery=false`. |

**Steps**

1. Start mill-1-test with M40.6 already TRUE.
2. Watch startup logs; confirm no full PO-end close/print from this recovery.

**Expected**

| Check | Exact expectation |
|---|---|
| Log | `Mill-1: trigger M40.6 latched at startup; clearing PLC latch with MES ack only (PO end workflow skipped).` |
| Log | `Mill-1: startup recovery on latched M40.6 — MES ack only; PO end workflow not run.` |
| Log | `Mill-1: startup recovery complete; handshake armed for next PO change.` |
| PLC | M40.6 clears after ack handshake |
| Absent | No PO-end bundle close attributed solely to startup recovery |

---

## C4 — Slit-end detection → accumulation growth

| | |
|---|---|
| **ID** | C4 |
| **Proves** | Changing DB251 Slit ID (@10) with NDT count (@6) drives `PlcSlitEndBundleCloser`; accumulation grows by that count (sub-threshold). |
| **Precondition** | R0. Handshake healthy. Known SAP PO available for mill WIP/slit context as required by your bench setup. Size threshold 20. |

**Steps**

1. Note accumulation empty.
2. Set **DB251 @6 (NDT)** = **7**. Change **DB251 @10 (Slit ID)** to a new value (e.g. 1 → 2) to fire slit-end.
3. Wait ≤ a few poll cycles.

**Expected**

| Check | Exact expectation |
|---|---|
| Log | `Mill-1: slit-end (Slit ID change ({prev}→{live})) added 7 NDT → remainder 7 (threshold=20, was 0).` (numbers match your write) |
| SQL | `Bundle_Accumulation.Pcs = 7` for Mill 1 / PO / size |
| No full close yet | No new `NDT_Bundle` if 7 < threshold |

**Confirm SQL**

```sql
SELECT Mill_No, Po_Number, Size_Key, Pcs, Last_Activity_Utc
FROM dbo.Bundle_Accumulation WHERE Mill_No = 1;
-- Expect: Pcs = 7 (or sum of repeated slit-ends)
```

---

## C5 — PLC-vs-CSV divergence (18 vs 46) — production incident replay

| | |
|---|---|
| **ID** | C5 |
| **Proves** | PLC closed total **18** with eventual CSV sum **46** escalates `Manual_Review` / discrepancy; **stored total stays 18**. Reproduces the persistent unexplained production incident on demand. |
| **Precondition** | R0. PLC path can close a bundle with **Total_NDT_Pcs = Target_Ndt_Pcs = 18** (e.g. PLC slit-end/PO close with NDT=18, or close then adjust only if your tools allow — prefer real PLC close at 18). |

**Steps**

1. Produce closed bundle `1226100001` with **`Total_NDT_Pcs = 18`**, fill target initialized to 18, `Manual_Review = 0`.
2. Feed Input Slit files whose stamped NDT sum for that batch reaches **46** (e.g. files 20+20+6, whole-file stamps / overshoot path as designed).
3. Wait until fill processing completes / review escalation runs.

**Expected**

| Check | Exact expectation |
|---|---|
| Math | \|46−18\|/18 ≈ 155% **>** 20% threshold |
| SQL | `Total_NDT_Pcs = 18` (**unchanged**), `Csv_Filled = 46` (or slit sum 46), `Count_Discrepancy = 1`, `Manual_Review = 1`, typically `Csv_Fill_State = 'CsvOvershoot'` |
| Log | `CSV fill discrepancy exceeds review threshold for bundle 1226100001: target=18 filled=46 thresholdPercent=20. Manual_Review set.` |
| Semantics | Totals are **not** auto-corrected to 46 (`UpdatesStoredTotal = false`) |

**Confirm SQL**

```sql
SELECT Bundle_No, Total_NDT_Pcs, Target_Ndt_Pcs, Csv_Filled, Csv_Fill_State,
       Count_Discrepancy, Manual_Review
FROM dbo.NDT_Bundle WHERE Bundle_No = N'1226100001';
-- Expect: Total_NDT_Pcs=18, Csv_Filled=46, Count_Discrepancy=1, Manual_Review=1
```

---

## C6 — PLC unhealthy mid-bundle → file-path close

| | |
|---|---|
| **ID** | C6 |
| **Proves** | When S7 goes unhealthy, `PlcWithFileFallback` allows the **file path** to close at threshold (safety fallback). |
| **Precondition** | R0. Start with PLC **healthy**. Optionally accumulate some pcs via files while PLC owns closes. |

### C6a — Unhealthy (disconnect) — immediate file-path close

**Steps**

1. With healthy S7, drop files until file-side count would reach threshold **or** leave accumulation just below threshold.
2. Disconnect: `POST /api/Settings/plc/mill/1/disconnect` **or** unplug/block bench host.
3. Confirm log: `Mill-1: S7 connection unhealthy (host …).`
4. Ensure file-side count ≥ 20 (drop remaining slits if needed). Wait one poll (**do not expect a 60s wait**).

**Expected**

| Check | Exact expectation |
|---|---|
| Health | `IsHealthy=false` / unhealthy WRN |
| Close | Bundle closes via file path on the next processing cycle after threshold |
| Log | Close line **without** requiring `for 60s with healthy S7` (unhealthy → `AllowFileThresholdClose` immediately) |
| SQL | New `NDT_Bundle` with expected total; accumulation cleared |

### C6b — Healthy missed PLC close — true 60s grace (optional companion)

If the bench stays **healthy** but never fires slit-end/PO close:

1. Feed files until file-side ≥ threshold.
2. Wait **≥ 60s**.

**Expected log (exact template):**

```text
Missed PLC close for PO {PO} Mill 1 Size {Size}: file-side count {Count} ≥ threshold {Threshold} for 60s with healthy S7; executing file-driven close (PlcCloseGraceSeconds safety-net).
Closing size-based bundle for PO {PO} Mill 1 Size {Size} threshold={Threshold} total={Total} (PlcCloseGrace file-side safety-net)
```

Mark C6a vs C6b separately on the summary sheet.

---

# Pass / fail summary sheet

Copy this table to the run record. Date / build / tester at top.

| Field | Value |
|---|---|
| Date | |
| Tester | |
| mill-1-test build / commit | |
| DB | `JazeeraMES_Test` |
| Bench PLC ready? | Yes / No |
| `CloseTrigger` (A/B) | File / PlcWithFileFallback+PLC off |
| `CsvFillQuietMinutes` during A7 | |

| ID | Result | Notes (failing check / actual value) |
|---|---|---|
| R0 reset practiced | PASS / FAIL | |
| **A1** First bundle `1226100001` | PASS / FAIL / SKIP | |
| **A1-grace** 60s healthy (optional) | PASS / FAIL / SKIP | |
| **A2** Partial `CsvFilling` | PASS / FAIL / SKIP | |
| **A3** `CsvComplete` + `1226100002` | PASS / FAIL / SKIP | |
| **A4** Overshoot whole-file | PASS / FAIL / SKIP | |
| **A5** Restart mid-accumulation | PASS / FAIL / SKIP | |
| **A6a** Legacy JSON refuse | PASS / FAIL / SKIP | |
| **A6b** Sequence behind live | PASS / FAIL / SKIP | |
| **A7** Quiet `CsvShort` | PASS / FAIL / SKIP | |
| **B1** Manual recon + Reprint | PASS / FAIL / SKIP | |
| **B2** Recon below filled + lock | PASS / FAIL / SKIP | |
| **B3** Merge highest + rollback | PASS / FAIL / SKIP | |
| **B4** Merge gap / no rollback | PASS / FAIL / SKIP | |
| **B5** Sequence Force refuse/apply | PASS / FAIL / SKIP | |
| **B6** Printer 2s + blank station | PASS / FAIL / SKIP | |
| **C1** Clean PO change | PASS / FAIL / SKIP | |
| **C2** Fast re-latch | PASS / FAIL / SKIP | |
| **C3** Startup latched M40.6 | PASS / FAIL / SKIP | |
| **C4** Slit-end accumulation | PASS / FAIL / SKIP | |
| **C5** PLC 18 vs CSV 46 | PASS / FAIL / SKIP | |
| **C6a** Unhealthy file close | PASS / FAIL / SKIP | |
| **C6b** 60s healthy grace | PASS / FAIL / SKIP | |

**Roll-change gate:** All of **A1–A7** and **B1–B6** = PASS. Section C = PASS or explicitly SKIP with bench unavailable sign-off.

| Sign-off | Name | Date |
|---|---|---|
| QA | | |
| Dev / owner | | |
