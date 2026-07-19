# Cursor Prompt — NdtBundleService: Fix Print Latency, PO-End Ingestion Stall, Missing Files & Orphaned Bundles (Mill-1)

## Role

You are a senior .NET engineer working on `NDTPrintAPI` (repo root: `C:\Users\Sarthak\source\repos\NDTPrintAPI`, service project `src\NdtBundleService`). The service is a .NET Worker Service (Kestrel API on :5000, Serilog logging, SQL Server `JazeeraMES_Prod` on `localhost\SQLEXPRESS`) deployed to `C:\Apps\NdtBundleService`. It prints NDT pipe-bundle tags for Mill-1 of the MBM plant, driven by a Siemens S7 PLC (192.168.0.13, rack 0 slot 2) and by Input Slit CSVs written by L2 into `\\10.2.20.210\pas-sap\To SAP\TM\Input Slit`.

A full production PO run (PO 1000060163 → PO 1000059986, 2026-07-14) was observed end-to-end and the Serilog output analyzed. **Four production defects were confirmed with root causes.** Your job: verify each root cause in code, then implement the fixes below in the specified phases. Do not refactor beyond what is specified. All behavioral changes must be config-gated with backward-compatible defaults noted per fix.

---

## 1. System snapshot (as evidenced by the logs — verify against code)

| Component (namespace `NdtBundleService.Services.*`) | Responsibility observed in logs |
|---|---|
| `SlitMonitoringWorker` | Polls Input Slit folder (~5–7 s cadence), parses slit CSVs, drives bundling, writes output CSVs to `...\To SAP\TM\NDT\NDT Input Slit\Input Slit`, records `Input_Slit_Row` / `Output_Slit_Row` |
| `NdtBundleEngine` | Opens/closes size-based bundles per PO/mill/size against threshold; allocates bundle numbers (`12261000NN`) |
| `NdtBundleRepository` / `NdtBundleRuntimeStateStore` | `NDT_Bundle` persistence, `Print_Status`, "Synced bundle total from slit sum", sequence floors |
| `CsvBundleOutputWriter`, `NdtZplTagPrinter`, `NetworkPrinterSender` | Bundle CSV, ZPL generation, raw TCP print to 192.168.0.125:9100 |
| `PlcHandshake.PlcHandshakeService` | Single S7 connection; polls trigger **M40.6** / writes ack **M40.7** at 500 ms; hooter writes MW56/MW58, Q3.6 pulse, PAS enable DB260.DBX3.6 |
| `PlcHandshake.PlcPoEnd.PlcPoEndQueueWorker` → `PoEndWorkflowService` | Dequeues PO-end events, resolves SAP PO, flushes partial bundle |
| `S7MillNdtCountReader` | **Opens its own second S7 connection** to read live NDT count (DB251 @6) — currently fails 100% (`S7MillNdtCountReader.cs:line 40`) |
| `WipBundleRunningPoProvider` | Running-PO per mill from WIP bundle filenames in TM Bundle folder; after PO end enters a **blocking wait** state |
| `FileBasedPoChange.FileBasedPoChangeWorker` | File-based PO end (Mill-4 only; `PoEndSource=File`) — **do not break this** |
| `PoPlanWipImporter` / `PoPlanWipEnrichmentProvider` | PO plan cache from SAP "PO Accepted" folder (736 POs cached at startup) — useful for PO validation |

Config facts from logs: Mill-1 `PoEndSource=Plc`, `PlcHandshakeEnabled=true`; Mills 2–4 handshake disabled; NDT Input Slit processing limited to mill 1; test threshold MW58=10.

PLC ladder (screenshot, Networks 79/80, "PO Change Trigger for NDT"): N79 sets M40.6 when `LineRun ∧ Slit2.PO_ID ≥ 1 ∧ Slit1.PO_ID ≠ Slit2.PO_ID` (DB250.DBX2.0, DB50.DBW0/DBW8), one-shot via M40.6 NC contact. N80 resets M40.6 via T11 `S_ODT S5T#5S` driven by M40.7. **Log evidence shows this ladder behaved exactly as designed — no PLC change is required or in scope.**

---

## 2. Incident evidence (anchor your work to these; timestamps +04:00, 2026-07-14)

### E1 — Print delay is L2 CSV latency + dead real-time path, not app latency
```
15:13:48  (plant HMI) NDT count reached 11 ≥ threshold 10 at slit end
15:17:21.874  SlitMonitoringWorker: Processing Input Slit file ...\2510118_03_260714_1000060163.csv   ← CSV only arrived now
15:17:23.659  NdtBundleEngine: Closing size-based bundle 1 ... threshold=10 total=11
15:17:24.024  NdtZplTagPrinter: Printed NDT tag 1226100001 (11 pcs)                                   ← app latency ≈ 2.5 s
```
Second data point: new-PO slit finished ~20:35 in plant; its CSV `2604321_09_..._1000059986.csv` arrived ~20:48 — **13 min L2 lag**.
```
[WRN] S7MillNdtCountReader: S7 NDT read failed (host 192.168.0.13 DB251 @6).      ← 25 occurrences, 0 successes
S7.Net.PlcException: Couldn't establish the connection to 192.168.0.13.
   at ... S7MillNdtCountReader.TryReadNdtPipesCountAsync(...) in ...\S7MillNdtCountReader.cs:line 40
14:40:18.431  PlcHandshakeService: Mill-1: S7 connected to 192.168.0.13 rack 0 slot 2.  ← the OTHER connection is fine
```
→ Two independent S7 clients from one process to one PLC; the second is refused (connection-resource exhaustion on the CP — same failure class as the Mill-4 CP 343-1 Lean 4-connection limit already solved in this codebase).

### E2 — The M40.6/M40.7 handshake completed correctly (contrary to plant-side impression)
```
19:23:43.718  Mill-1: PO change rising edge detected on M40.6 — PO_Id 6, NDT 5, CorrelationId c3f71836...
19:23:43.748  Mill-1: MES ack sent (TRUE)
19:23:43.749  PO end event enqueued / 19:23:43.751 dequeued
19:23:43.752  PLC PO_Id 6 is not a plausible SAP PO ... resolving from Input Slit CSV
19:23:49.643  Mill-1: trigger M40.6 cleared by PLC          ← T11 5 s timer, as designed
19:23:49.672  Mill-1: wrote ack M40.7=FALSE — handshake complete
19:25:02.405  Processing ...\2510117_03_..._1000060163.csv  ← workflow WAITED ~100 s for the next CSV to resolve the PO
19:25:25.407  PO resolved from latest Input Slit CSV as 1000060163
19:25:25.414  Closing partial size-based bundle 6 ... due to PO end    → 1226100006 (8 pcs) printed 19:25:25.680
19:25:28.478  WipBundleRunningPoProvider: Mill 1: PO end for 1000060163; waiting for new WIP bundle file in TM Bundle folder
```

### E3 — 85-minute ingestion gate + head-of-line retry loop
```
19:25:30 → 20:50:18   Mill-1 bundling fully gated ("waiting for new WIP bundle file")
20:36:05.783  Processing ...\2510117_06_..._1000060163.csv   ← late old-PO file (L2 lag ~73 min after PO end)
20:36→20:50   "skipping bundling for slit row" + "File will be retried on next poll" — 124 retries, every ~7 s
20:50:18.575  Mill 1: new running PO 1000059986 accepted from WIP bundle file WIP_01_1000059986_2601019449_260714_204842.csv
              ← upstream TM Bundle system only CREATED this file at 20:48:42 (first bundle weigh of new PO)
20:50:23.86x  2510117_06 finally written; Synced bundle total for 1226100007 from slit sum: 0 → 6 (force=false)
20:50:23.920  Closing size-based bundle 8 for PO 1000059986 ... total=13 → 1226100008 printed 20:50:24.244
```

### E4 — The 36-vs-20 file gap predates PO end: startup baseline exclusion into an empty DB
```
14:40:16.26x  SqlTraceabilityStartupCheck: table NDT_Bundle: 0 row(s) ... Input_Slit_Row: 0 row(s)   ← fresh DB
14:40:23.620  SlitMonitoringWorker: Input Slit: recorded 28 slit file(s) already in ...\Input Slit at service start
              (no NDT output for those versions). New paths or same path with a newer LastWriteTimeUtc will be processed.
```
Exactly 20 distinct `*_1000060163.csv` files were ever processed and all 20 were written to the NDT output folder. The other ~16 old-PO files were in the folder before 14:40 and were baselined out → the missing 56 pipes (plant 123 vs app 67; app's 67 = printed bundles 11+10+10+12+10+8 = 61, plus 6 in orphaned 1226100007).

### E5 — Orphaned bundle 1226100007
Created implicitly at 20:50:23 when 6 late pipes of the **already-ended** PO 1000060163 were bundled; below threshold (6 < 10); the PO's end-flush had already run at 19:25 → no closure path → stuck `Print_Status=Pending` forever, tag never printed, number appears "skipped" between printed 06 and 08. Also note the end-flush at 19:25 was **premature**: it tagged 8 pcs while 6 more were still in L2's pipeline — the physical tail bundle of ~14 pipes got a wrong tag.

Zero `[ERR]` lines exist in the log. Every defect above is designed-in behavior, not a crash.

---

## 3. Root causes

- **RC-1** — Bundle close/print is triggered solely by Input Slit CSV arrival; L2 exports CSVs 3–73 min after the physical slit end. The intended real-time source (`S7MillNdtCountReader`) opens a second S7 connection that is always refused, and the failure is swallowed as a per-poll WRN with silent fallback to file-driven behavior.
- **RC-2** — After a PO end, `WipBundleRunningPoProvider` hard-gates ALL bundling for the mill until the upstream TM Bundle system writes a new WIP bundle file — an external event that occurred 85 min later. The gate is unnecessary for correctness because slit files self-identify their PO (the code already prefers file PO: *"Input Slit PO … used for NDT output (WIP bundle PO … ignored)"*).
- **RC-3** — The gated-file retry design re-processes the same file every ~7 s (head-of-line blocking, 124 retries) instead of parking it with backoff and continuing with other files.
- **RC-4** — The PO-end flush runs immediately at the PLC trigger while the file pipeline still holds in-flight rows for that PO; there is no drain window and no final sweep, so (a) the tail tag under-counts and (b) late rows re-open a new bundle on an ended PO that nothing will ever close (→ 1226100007).
- **RC-5** — Startup seeds all pre-existing folder files as "already processed" with no reconciliation against `Input_Slit_Row`, so any file written while the service is down (or before a fresh deploy) is permanently invisible (→ 16 files / 56 pipes).
- **RC-6** — The ended-PO SAP number is resolved from "the latest Input Slit CSV", which (a) added ~100 s latency waiting for the next CSV and (b) is a race: if the first post-trigger CSV belongs to the NEW PO, the workflow would flush the wrong PO. The service already knows the running PO in state and should use it.

---

## 4. Required fixes

> Investigate first (Section 5), then implement in phase order (Section 6). Every new behavior behind a config key under `NdtBundle:`; defaults listed. Additive DB migrations only. Preserve existing Serilog message shapes unless a change is called out.

### F-1 — Single shared S7 connection per mill (unblocks the real-time path)
1. Introduce `IS7ConnectionProvider` (one per mill): owns the single `S7.Net.Plc` instance, exposes `ReadAsync`/`WriteAsync` guarded by a `SemaphoreSlim(1,1)`, auto-reconnect with exponential backoff (1 s → 30 s cap), and connection-state events. Register as singleton keyed by mill.
2. Refactor `PlcHandshakeService` (trigger/ack/hooter I/O) and `S7MillNdtCountReader` to consume the provider. **Delete the independent `Plc.Open()` at `S7MillNdtCountReader.cs:~40`.**
3. Failure logging: log the failed→ok and ok→failed transitions once each (INF/WRN) plus a periodic summary — not a stack trace per poll.
4. Config: `Plc:Mill1:Host/Rack/Slot` (reuse existing), `Plc:ReconnectBackoffSeconds`.
- **Acceptance:** with the service running its normal handshake loop, `TryReadNdtPipesCountAsync` succeeds continuously; zero `Couldn't establish the connection` entries in a 1-hour soak.

### F-2 — PLC-event-driven bundle close & immediate print, CSV as reconciliation
1. New hosted monitor (or extend the existing 500 ms handshake loop) reading per-poll: live NDT count (DB251 @6 — make DB/offset config: `Plc:Mill1:NdtCountDb=251`, `NdtCountOffset=6`) and the slit registers already known (DB50.DBW0 Slit1.PO_ID, DB50.DBW8 Slit2.PO_ID). Detect **slit end** (confirm the exact PLC signal with the FOX team — candidate: count latch/reset edge or slit-register rollover; make the address config-driven and leave a `// TODO(FOX): confirm slit-end signal` marker).
2. On slit end with `liveCount >= threshold`: call new `NdtBundleEngine.CloseBundleFromPlc(po, mill, size, plcCount)` → allocate number, write bundle CSV, insert `NDT_Bundle` with `Close_Source='Plc'`, `Awaiting_Csv_Recon=1`, print immediately.
3. When the slit CSV arrives later, attach its rows to the already-closed bundle (match PO+mill+size+open sequence window) and run the existing "Synced bundle total from slit sum" logic. If CSV sum ≠ PLC count → WRN + set `Count_Discrepancy=1`; optional corrected reprint behind `ReprintOnCountMismatch=false`.
4. Mode switch: `NdtBundle:CloseTrigger = File | Plc | PlcWithFileFallback` (**default `PlcWithFileFallback`**). If the S7 provider reports unhealthy, the file-driven path continues exactly as today — no regression risk.
5. Hooter: once live count is available, feed MW56 from the PLC count (or write MW58 only and let the count be PLC-native) so the plant hooter no longer lags the file pipeline. Keep behavior config-gated: `HooterCountSource = App | Plc` (default `App` until validated).
- **Acceptance:** tag at the printer ≤ 15 s after the plant count crosses threshold at slit end (PLC path healthy); on PLC outage, behavior identical to current file-driven flow. Replaying E1: print at ~15:13:5x instead of 15:17:24.

### F-3 — Remove the WIP gate from ingestion; per-file retry with backoff; faster PO adoption
1. In `SlitMonitoringWorker`, **route every slit row by the PO parsed from the file** (existing logic) and delete the `waiting for new WIP bundle file ... skipping bundling` hard-stop from the bundling path. `WipBundleRunningPoProvider` remains the source for hooter targeting and the fallback when a file's PO is absent/invalid.
2. Per-file retry state: on a deferrable outcome, park the file with backoff (5 s → 30 s → 2 min cap, `FileRetryBackoff` config) and continue to the next pending file. One file must never block the queue. Cap retry logging to one line per backoff step.
3. New-PO adoption: accept a new running PO for the mill when the first Input Slit file references a PO ≠ current **and** the PO validates against the cached PO plan (`PoPlanWipEnrichmentProvider`, 736 POs). WIP bundle file remains authoritative confirmation when it arrives; log adoption source (`Slit-file` vs `WIP-file`). Config: `PoAdoptionFromSlitFiles=true`.
- **Acceptance:** replaying E3: `2510117_06` is bundled at 20:36 (its arrival), the new PO's first bundle prints at ~20:48 (CSV arrival; ~20:35 with F-2), and there is no interval in which incoming valid files are skipped. No file logs more than ~6 retry lines total.

### F-4 — PO-end drain window, deferred/final flush, orphan sweep
1. Add PO lifecycle state per mill+PO: `Running → Draining(endedAtUtc) → Closed`. On PLC PO end: mark `Draining`, keep accepting that PO's late files as normal bundling.
2. Flush policy `PoEndFlushMode = Immediate | AfterDrain` (**default `AfterDrain`**). `AfterDrain`: the partial-bundle flush runs when the drain window (`PoEndDrainMinutes`, default **120** — observed L2 lag up to 73 min) expires **or** an operator forces it (see remediation endpoint) — the tail bundle then reflects all late rows (E2/E5 would have produced one correct 14-pc tail tag). `Immediate` preserves today's behavior **plus** a drain-expiry sweep that closes-and-prints any bundle re-opened by late rows.
3. Orphan detector (safety net regardless of mode): periodic job flags any open bundle with pcs > 0 whose PO is `Closed` → WRN + auto close-and-print (config `AutoCloseOrphanBundles=true`) or mark `Manual_Review=1`.
4. Rows arriving for a PO already `Closed` past drain: record traceability rows, route bundle decision to the orphan policy — never silently drop, never leave permanently open.
- **Acceptance:** after any PO transition, no bundle number remains open for an ended PO; every allocated number ends `Printed` or explicitly `Manual_Review`. Replaying E5: 1226100007 is closed and printed automatically.

### F-5 — Startup & periodic backfill reconciliation (kills the 36-vs-20 class)
1. Replace "record pre-existing files, never process" with reconciliation: enumerate the Input Slit folder within `BackfillLookbackHours` (default 48); for each file version (path + `LastWriteTimeUtc`) containing rows for configured mills and **absent from `Input_Slit_Row`** → ingest. Keep the existing version-dedupe.
2. Bundling policy during backfill by PO state: `Running`/`Draining` → normal bundling; `Closed` → traceability rows + orphan policy (F-4.4). Guard against double-printing after a fresh DB: before auto-bundling a backfilled file, check the NDT Bundles output folder scan (already performed at startup by `NdtBundleRepository`) for an existing bundle CSV covering those rows; if ambiguous → `Manual_Review`, do not print.
3. Run the same reconciliation every `ReconcileIntervalMinutes` (default 30) to self-heal watcher gaps and service downtime.
- **Acceptance:** restart the service mid-PO with files written during downtime → all are ingested within one reconcile cycle; Input Slit vs NDT Input Slit counts match for mill-1 rows; SQL row counts match folder contents. E4 replay: the 16 pre-start files are ingested (traceability) instead of vanishing.

### F-6 — PO-end resolution & handshake hardening
1. Resolve the **ended** PO from the mill's current running-PO state (the service already knew 1000060163); use "latest Input Slit CSV" only as fallback + cross-check, logging any mismatch. Removes the ~100 s wait and the wrong-PO race (RC-6).
2. Persist a handshake audit table `Handshake_Event` (mill, edge ts, ack ts, cleared ts, ack-dropped ts, plc PO_Id, plc NDT count, correlationId, outcome) — this incident's "ack was never sent" confusion becomes answerable with data.
3. Watchdogs: M40.6 TRUE > `StuckTriggerAlarmSeconds` (default 30) without a completed handshake → WRN + alert; ack write failure → retry ×3 with backoff → ERR + alert. Keep the existing ack-first ordering (ack before workflow) — it is correct.
4. Log the PLC-reported NDT count vs the flushed partial count at PO end (19:23 trigger said `NDT 5`, flush said 8 — capture both for FOX diagnostics).
- **Acceptance:** every handshake produces one audit row; simulated stuck trigger and ack-write failure raise alerts.

---

## 5. Investigate first — confirm each RC in code before editing

1. `S7MillNdtCountReader.cs` (~line 40): confirm it constructs/opens its own `Plc`; identify every other S7 client in the process (RC-1).
2. `SlitMonitoringWorker`: locate (a) the startup baseline seeding that produced the "recorded 28 slit file(s) … (no NDT output for those versions)" message (RC-5), (b) the gate producing "waiting for new WIP bundle file … skipping bundling" (RC-2), (c) the retry that produced "File will be retried on next poll" every ~7 s (RC-3).
3. `WipBundleRunningPoProvider`: the PO-end wait state and baseline WIP stamp logic (RC-2).
4. `NdtBundleEngine` + `NdtBundleRuntimeStateStore`: where a first row for a PO/mill/size opens a bundle and allocates the next number — confirm nothing prevents this for an ended PO (RC-4 / 1226100007).
5. `PlcPoEndQueueWorker`: the "resolving from Input Slit CSV" path and its wait loop (RC-6).
6. Confirm Mill-4's `FileBasedPoChangeWorker` shares none of the code paths you are changing, or gate changes so `PoEndSource=File` mills are unaffected.
Report findings (file:line per RC) in the PR description before implementing.

## 6. Delivery phases

- **Phase 1 (correctness, low risk):** F-3 (gate removal + backoff), F-6.1 (state-based PO resolution), F-4 (drain + orphan sweep). These alone fix the 85-min stall, the premature/wrong tail tag, and the orphaned number.
- **Phase 2 (data completeness):** F-5 backfill/reconciliation.
- **Phase 3 (latency):** F-1 shared S7 provider, then F-2 PLC-driven close/print (`PlcWithFileFallback`), F-6.2–.4 audit + watchdogs.
- Each phase: unit tests (engine close paths: file-driven, plc-driven, drain flush, orphan sweep; PO adoption; backoff), plus an **incident replay test** — a fixture reproducing this log's timeline (file arrival times, PLC events) asserting: `2510117_06` processed at 20:36; no >85-min gate; tail bundle correct; no perpetually-open bundle; new PO's first bundle printed at CSV arrival (Phase 1–2) / slit end (Phase 3). Abstract S7 behind an interface for tests (follow the existing `Mill4TcpOpenCommTransport` transport pattern).

## 7. Guardrails / non-goals

- No PLC program changes — Networks 79/80 are verified correct; anything PLC-side is a FOX-team note, not code.
- Do not alter Mill-4's file-based PO change flow or the `PoEndSource` config contract.
- DB migrations additive only (`Close_Source`, `Awaiting_Csv_Recon`, `Count_Discrepancy`, `Manual_Review`, PO lifecycle state, `Handshake_Event`).
- Printer path (`NetworkPrinterSender`, 192.168.0.125:9100) is working and out of scope.
- Keep 500 ms handshake polling; do not increase PLC load beyond one extra word-read per poll for the live count.

## 8. One-time production remediation (document in PR, execute after deploy)

1. Close and print orphan **1226100007** (6 pcs, PO 1000060163, Mill 1) — implement `POST /api/bundles/{bundleNo}/close-and-print` on the existing Kestrel API as the maintenance path (guarded by config `EnableMaintenanceEndpoints`), rather than a raw SQL update.
2. Run the F-5 backfill once for 2026-07-14 to ingest the 16 pre-start `*_1000060163.csv` files as traceability rows; their pipes were already physically handled, so bundling goes through `Manual_Review` — not auto-print.
3. Verify post-fix counts: `Input Slit` folder files for the PO == `NDT Input Slit` outputs == `Input_Slit_Row` rows; `SUM(pcs)` across the PO's bundles == plant count.
