# NDT Input Slit SAP Status Tracking — Design (Approved with Tightenings)

Status: **Phase 1 approved for implementation** (2026-07-27). Phases 2+ blocked until the
basename-preservation validation spike is run against the live folders and the UNC paths below are
verified reachable from the service account.

## Requirement

SAP pulls NDT Input Slit output CSVs from the pending folder and moves each file to either the
Accepted or the Rejected folder:

| SAP status | Folder |
|------------|--------|
| Pending    | `Z:\To SAP\TM\NDT\NDT Input Slit\Input Slit` (existing `NdtBundle:OutputBundleFolder`) |
| Accepted   | `Z:\To SAP\TM\NDT\NDT Input Slit\NDT Input Slit Accepted` |
| Rejected   | `Z:\To SAP\TM\NDT\NDT Input Slit\NDT Input Slit Rejected` |

(`Z:` = `\\10.2.20.210\pas-sap`; production config uses the UNC form.)

- **Accepted** = data posted to SAP; frozen from this system's side.
- **Rejected** = operator copies the file out, edits the copy, and drops it into the pending
  folder; SAP reprocesses. The copy left in Rejected is never edited in place.
- **Files in the Accepted and Rejected folders must never be edited** — by this system or by
  operators (confirmed 2026-07-27). This already holds structurally: every CSV write path
  (`SlitMonitoringWorker` output write, `UpdateOutputCsvFilesForSlitAsync`, delete-slits) operates
  only on `OutputBundleFolder`; Phase 2 makes it an explicit invariant.
- Filenames do **not** change when SAP moves a file (confirmed by operations 2026-07-27), so the
  file basename is the durable match key across all three folders.
- Accepted folders are **not retained indefinitely**: comparable Accepted folders currently hold
  ~6 months of files. Therefore "file absent from all three folders" must keep the last recorded
  status (a file that disappears from Accepted stays Accepted).

A slit file's data may only be corrected locally while it is Pending or Rejected-in-flight — never
once Accepted.

## Hard requirements (from review, 2026-07-27)

1. **`manual-bundle-reconcile` is permanently exempt from all SAP-Accepted gating.** It must never
   be blocked — under any circumstance — by `Ppc_Correction_Pending`, by any file's SAP status, or
   by any future extension of this feature. Operators must always be able to adjust the bundle
   total immediately during production. This is a design invariant, not an open question.
2. **`Output_Slit_Sap_Status_Event` (append-only audit log) is required in Phase 1**, not optional.
   Every status insert, transition, resubmit, and ignored regression is recorded.
3. **The Accepted-file block on the ingest/backfill path guards both writes.** When the worker
   re-processes an input file whose output basename is Accepted, the gate must block **both** the
   disk CSV re-emit into the pending folder (`File.WriteAllLinesAsync` in `SlitMonitoringWorker` —
   re-emitting an Accepted basename would cause SAP to post the same file twice) **and** the
   corresponding `Output_Slit_Row` insert (`RecordOutputSlitRowsAsync`), so ingest can never
   double-count a frozen SAP posting. Scope note (amended 2026-07-27 after process clarification):
   this ingest-path block does **not** apply to operator slit reconcile — see "Accepted-file
   correction flow" below.
4. **`Manual_Review` and `Ppc_Correction_Pending` must render as visually and functionally distinct
   indicators** on the reconcile page. Acceptance criterion for Phase 2: a test (or screenshot)
   showing one bundle with both flags open simultaneously, with unambiguous, separately labeled
   badges and separate actions (Manual_Review → review/clear flow; PPC pending → correction items
   panel). `Manual_Review` is not surfaced in the dashboard today, so Phase 2 adds both.

## Decisions from answered questions

- **Accepted-file correction flow (amended 2026-07-27):** the established process for OK/prime
  pipes is: when a slit file is already Accepted and a correction is needed, the operator
  reconciles the slits/bundles **locally** and then emails PPC, who applies the change manually in
  SAP. The Phase 2/3 design formalizes this rather than blocking it:
  - Operator slit reconcile on an Accepted file's data is **allowed locally** (SQL
    `Output_Slit_Row` + bundle total). The on-disk CSV in Accepted is never touched — which is
    what happens today: the CSV rewrite scans only the pending folder, finds nothing, and the API
    reports an SQL-only update.
  - Instead of returning 409, the system **auto-creates a pre-filled `Ppc_Correction_Item`**
    (bundle, file, slit, old value, corrected value) and sets `Ppc_Correction_Pending` on the
    bundle — replacing the manual, untracked "email PPC" step with a tracked open status. Nothing
    is sent automatically.
- **Gating scope (Q4):** file-destructive slit operations on Accepted files remain hard-blocked:
  `delete-slits`, bundle void, and remediation (Q6) — removing rows for a SAP-posted file breaks
  the bundle-total invariant unrecoverably. Bundle-level `manual-bundle-reconcile` is never
  blocked (hard requirement 1).
- **Invariant (Q5):** the NDT pipe count of a bundle must equal the sum of NDT pipes across its
  contributing NDT Input Slit output files. Consequence: when a Rejected file is edited and
  resubmitted with a changed pipe count, `Output_Slit_Row` must be brought back in line with the
  resubmitted file content (Phase 4 ExactMatch bypass + drift detection), otherwise the invariant
  silently breaks.
- **Delete/remediation (Q6):** Accepted files also block `delete-slits` and bundle
  void/remediation. For ad-hoc remediation SQL scripts in `docs/`, every future script that touches
  `Output_Slit_Row` or voids bundles must begin with a guard SELECT against
  `Output_Slit_Sap_Status` for the affected `Source_File` basenames and abort (RAISERROR) when any
  is Accepted. Template to include in new scripts:

```sql
-- SAP-Accepted guard: abort when any touched output slit file is already posted to SAP.
IF EXISTS (
    SELECT 1
    FROM dbo.Output_Slit_Sap_Status s
    WHERE s.Status = N'Accepted'
      AND s.File_Name IN (N'<file1.csv>', N'<file2.csv>'))
    RAISERROR(N'One or more output slit files are SAP-Accepted; remediate via PPC correction instead.', 16, 1);
```

## Status model

Per output file basename, one row in `Output_Slit_Sap_Status`:

```
(none) ──write──▶ Pending ──SAP──▶ Accepted   (terminal, frozen)
                    │  ▲
                    │  └──resubmit (Resubmit_Count += 1)
                    └───SAP──▶ Rejected
```

Observation rules per poll cycle (basename presence across the three folders):

| Disk state | Derived status |
|------------|----------------|
| In Accepted folder (regardless of others) | `Accepted` |
| Else in pending folder (a stale copy may remain in Rejected when the operator copies rather than moves) | `Pending` |
| Else in Rejected folder | `Rejected` |
| Absent from all three | Keep last recorded status (SAP archives Accepted files after ~6 months) |

Transition handling (`OutputSlitSapStatusPolicy`):

| Current | Observed | Action |
|---------|----------|--------|
| (no row) | any | Insert + `Initial` event |
| X | X | No event; refresh `File_LastWriteTimeUtc`/`Observed_Folder` when the file version is newer |
| `Rejected` | `Pending` | Transition + `Resubmit_Count += 1` + `Resubmitted` event |
| `Accepted` | anything else | **No status change** (frozen); `RegressionIgnored` audit event (deduped per file version) + warning log |
| other | other | Transition + `Transition` event |

## Phase 1 (this change) — read-only observation

- `NdtBundleOptions`: `NdtInputSlitAcceptedFolder`, `NdtInputSlitRejectedFolder`,
  `NdtInputSlitSapStatusPollSeconds` (0 → falls back to `PollIntervalSeconds`).
- SQL migration `docs/Output_Slit_Sap_Status_AddTable.sql`: `Output_Slit_Sap_Status` (current
  status per basename) **and** `Output_Slit_Sap_Status_Event` (append-only audit). Both are listed
  in `SqlTraceabilityHealth` required tables; health reports them missing until the migration runs.
- `NdtInputSlitSapStatusWorker`: polls the three folders (polling, not `FileSystemWatcher` —
  matches the UNC-reliability pattern of `SlitMonitoringWorker`), derives statuses, applies them
  via `IOutputSlitSapStatusRepository`. A cycle is skipped entirely if any configured folder cannot
  be enumerated (no partial views). `MinSourceFileLastWriteUtc` is honored so ancient files are not
  baselined. First run backfills a baseline from whatever is currently in the folders.
- Seed-on-write: `SlitMonitoringWorker` marks each freshly written output CSV `Pending`
  immediately, so a status row exists before SAP pulls the file.
- **No gating, no dashboard changes, no behavior changes to ingest/reconcile in Phase 1.**

## Phase 2 (implemented 2026-07-27)

- **API enrichment:** `GET /api/Reconcile/bundles/{batch}/slits` joins
  `Output_Slit_Row.Source_File` basename → `Output_Slit_Sap_Status` and returns per slit:
  `sapStatus` (strongest across contributing files: Accepted > Rejected > Pending;
  `OutputSlitSapStatusPolicy.Strongest`), `sapStatusAtUtc`, `resubmitCount`, and `sourceFiles`
  (basename + per-file status). The bundle object gains `manualReview` (read via
  `INdtBundleRepository.IsManualReviewFlaggedAsync`, tolerant of the missing column). SQL-only and
  best-effort: with SQL disabled or tables unmigrated, fields are null/absent and nothing fails.
- **Reconcile-page badges:** per-slit SAP Status column (green Accepted / red Rejected, with
  resubmit count / gray Pending; tooltip shows file names + since-when). `Manual_Review` renders as
  a purple "Manual review" banner — visually distinct from the amber Manual-Recon-lock banner and
  reserved-for-Phase-3 PPC-pending styling (hard requirement 4; the both-flags-simultaneously
  acceptance test completes in Phase 3 when `Ppc_Correction_Pending` exists).
- **Ingest-path gate (hard requirement 3):** two layers in `SlitMonitoringWorker`:
  - `InputSlitBackfillCoverage.Evaluate` now treats a batch-stamped same-basename file in
    `NdtInputSlitAcceptedFolder` as `ExactMatch` (backfill → TraceabilityOnly, no re-emit). A
    Rejected-folder copy is deliberately **not** coverage — rejected data never posted.
  - At the output write site, `IsOutputSapAcceptedAsync` blocks **both** the pending-folder CSV
    re-emit and the `Output_Slit_Row` insert (+ slit-sum total sync) when the basename is Accepted;
    input-side `Input_Slit_Row` traceability still records. Best-effort: SQL down → no gate, CSV
    flow unaffected.
- **`delete-slits` 409 gate:** when any requested slit is backed by an Accepted file, the API
  returns 409 with the file list before touching anything; the dashboard also disables the delete
  checkbox for Accepted slits.
- **`reconcile-slit` on Accepted data stays allowed** (SQL-only, as today). The response now names
  the Accepted file(s) (`sapAcceptedFiles`) and appends a "request the SAP-side fix from PPC" note;
  the edit panel shows the same notice. This is the hook Phase 3 turns into a tracked
  `Ppc_Correction_Item`.
- **Never-write invariant:** the SAP status watcher refuses to run (single error log) when the
  pending/Accepted/Rejected paths are not pairwise distinct
  (`NdtInputSlitSapStatusWorker.FoldersOverlap`); no system write path targets the Accepted or
  Rejected folders.
- `manual-bundle-reconcile` untouched (hard requirement 1).
- Tests: `OutputSlitSapStatusPhase2Tests` (strongest-status rule, Accepted-folder coverage,
  Rejected-folder non-coverage, unstamped-file non-coverage, folder-overlap detection).

## Phase 3 (implemented 2026-07-27)

- **SQL:** `docs/Ppc_Correction_Item_AddTable.sql` creates `Ppc_Correction_Item`
  (bundle / file / slit / `Old_NDT_Pipes` / `Corrected_NDT_Pipes` / Open|Cleared + audit fields).
  A filtered unique index enforces one Open item per (bundle, file, slit): repeated local
  corrections update `Corrected_NDT_Pipes` on the existing Open row and preserve the original
  `Old_NDT_Pipes` — the value SAP still has. Listed in `SqlTraceabilityHealth` required tables.
- **`Ppc_Correction_Pending` is derived, never stored:** a bundle is pending iff any Open item
  exists for its batch number (`CountOpenItemsForBatchAsync`). No flag column on `NDT_Bundle`, so
  the status can never drift; clearing the last item clears the bundle automatically.
- **Auto-create on allowed local correction:** `reconcile-slit` captures the pre-update slit value,
  and when the slit's data comes from SAP-Accepted file(s), upserts one Open item per file via
  `IPpcCorrectionRepository` after applying the local (MES-only) correction as usual. The response
  reports `ppcCorrectionItemsCreated/Updated` and the message tells the operator the bundle is now
  PPC-correction-pending. If recording fails (SQL down / table missing), the correction still
  succeeds and the message says to email PPC manually. **Nothing is ever sent automatically.**
- **API:** `GET /api/Reconcile/bundles/{batch}/slits` bundle object gains `ppcCorrectionPending` +
  `ppcOpenCorrectionCount`; `GET /api/Reconcile/bundles/{batch}/ppc-corrections?includeCleared=`
  lists items; `POST /api/Reconcile/ppc-corrections/{id}/clear` (optional `clearedBy`/`note`) marks
  an item Cleared after PPC confirms the SAP-side fix (404 when already cleared).
- **Dashboard:** orange "PPC correction pending" banner with the open items table
  (file, slit, SAP value → corrected, created, per-item "PPC confirmed — clear" button with
  confirm prompt) — visually and functionally distinct from the purple "Manual review" banner and
  the amber Manual-Recon-lock banner; all can render simultaneously.
- **Hard requirement 4 acceptance:** `PpcCorrectionPhase3Tests.Bundle_slits_reports_manual_review_and_ppc_pending_simultaneously_as_distinct_flags`
  proves one bundle exposes `ManualReview = true` and `PpcCorrectionPending = true` at the same
  time as separately named flags; further tests cover item auto-create with old/corrected values,
  update-not-stack on repeat reconciles, and clear semantics.
- `manual-bundle-reconcile` remains untouched (hard requirement 1) — it neither creates items nor
  is ever blocked by `Ppc_Correction_Pending`.

## Phase 4 (implemented 2026-07-27)

Scope: ExactMatch bypass for resubmitted files (`Resubmit_Count > 0` / prior `Rejected`) and
resubmit content-drift detection against `Output_Slit_Row`, preserving the bundle-total invariant
(Q5). `Ambiguous → Manual_Review` and the `Manual_Recon` lock semantics from
`ClosedPoSlitIngestPolicy` are unchanged by resubmits.

- **Ingest gate extended (the ExactMatch bypass):**
  `OutputSlitSapStatusPolicy.DecideIngestGate` now decides the write gate at the
  `SlitMonitoringWorker` output write site. Only a plain first-pass **Pending** basename (or an
  untracked one) may be re-emitted / have `Output_Slit_Row` inserted:
  - `Accepted` — frozen, as in Phase 2 (hard requirement 3).
  - `Rejected` (in flight) — the operator copy-edit-resubmit flow owns the basename; a system
    re-emit would auto-resubmit unedited data into the pending folder and race the operator's fix.
  - `Resubmit_Count > 0` — the operator-edited pending copy is authoritative and must never be
    clobbered by a regenerated file, even though its content differs from what the system would
    produce (hence "bypass": the ingest treats the file as covered without a content match). The
    coverage layer needs no change — `InputSlitBackfillCoverage` already keys ExactMatch on
    basename + batch stamps, not content, so a resubmitted pending file is ExactMatch for backfill.
  Each gated case logs a distinct warning; SQL-side writes (`Output_Slit_Row` insert + slit-sum
  total sync) are gated together with the disk write, and input-side `Input_Slit_Row` traceability
  still records. Best-effort as before: SQL down → no gate, CSV flow unaffected.
- **Resubmit content-drift detection:** `ApplyObservationsAsync` now reports which basenames
  recorded a Resubmit transition (Rejected → Pending) in the cycle
  (`OutputSlitSapStatusApplyResult`), and `NdtInputSlitSapStatusWorker` runs
  `ResubmitDriftService.DetectAndReconcileAsync` for each. The service:
  - parses the resubmitted pending CSV into per-(batch, slit) NDT-pipes sums
    (`ResubmitDriftPlanner.ParseOutputCsvSums`; unstamped rows ignored — never posted to SAP);
  - reads SQL sums for the basename
    (`INdtBundleRepository.GetOutputSlitRowSumsForSourceFileAsync`, basename `LIKE` matching);
  - diffs them (`ResubmitDriftPlanner.Compute`, pure): **value drift** on a matching (batch, slit)
    is applied to SQL through the same path as operator slit reconcile
    (`IReconcileSyncService.SyncAfterSlitReconcileAsync`) — the resubmitted file wins, since it is
    what SAP will now post; **row additions/removals** in the edited file are logged as anomalies
    only, never auto-applied;
  - preserves Q5: every batch with an applied change gets its bundle total re-synced from slits
    (`TrySyncBundleTotalFromSlitsAsync(force: true)`) — except **`Manual_Recon`-locked bundles**,
    which keep their locked total and only refresh `Post_Recon_Csv_Sum` (lock semantics unchanged);
  - audits an applied sync as a `ResubmitDriftSynced` row in `Output_Slit_Sap_Status_Event`
    (value details in the service log).
  No-drift resubmits, a vanished pending file (SAP pulled it between poll and check), and a
  basename with no `Output_Slit_Row` rows are all logged no-ops. `Ambiguous → Manual_Review` is
  untouched — Phase 4 adds no new Manual_Review triggers.
- `manual-bundle-reconcile` remains untouched (hard requirement 1); nothing in Phase 4 writes to
  the Accepted/Rejected folders (never-write invariant).
- Tests: `ResubmitDriftPhase4Tests` — ingest-gate decisions (untracked/first-pass Pending pass;
  Accepted/Rejected-in-flight/resubmitted gated), CSV sum parsing (per-(batch, slit) summing,
  unstamped-row skip), drift plan (changes vs file-only vs SQL-only), and drift service end-to-end
  (sync via slit-reconcile path with the file's value, forced bundle-total sync when unlocked,
  Post_Recon_Csv_Sum-only refresh when Manual_Recon locked, audit event, and no-op cases).
