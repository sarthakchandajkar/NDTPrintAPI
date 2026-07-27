using Microsoft.Extensions.Logging.Abstractions;
using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// Phase 4 resubmitted-rejected-file handling (docs/NDT_Input_Slit_SAP_Status_Design.md):
/// - ingest gate: only a plain first-pass Pending (or untracked) basename may be re-emitted;
///   Accepted, Rejected-in-flight, and resubmitted (Resubmit_Count &gt; 0) basenames are gated
///   (the ExactMatch bypass — the operator-edited copy is authoritative and never clobbered);
/// - resubmit content drift: the resubmitted pending CSV is diffed against Output_Slit_Row;
///   value drift re-syncs SQL via the operator-slit-reconcile path and bundle totals follow
///   (Q5 invariant), while Manual_Recon-locked bundles keep their total (lock semantics unchanged);
/// - row add/remove anomalies in the edited file are logged only, never auto-applied.
/// </summary>
public sealed class ResubmitDriftPhase4Tests
{
    private const string BatchNo = "1226100001";
    private const string FileName = "2604361_01_260726_1000060363.csv";

    // ---- Ingest gate decision (OutputSlitSapStatusPolicy.DecideIngestGate) ----

    [Fact]
    public void Untracked_file_is_not_gated() =>
        Assert.Equal(OutputSlitIngestGate.None, OutputSlitSapStatusPolicy.DecideIngestGate(null));

    [Fact]
    public void First_pass_pending_is_not_gated() =>
        Assert.Equal(
            OutputSlitIngestGate.None,
            OutputSlitSapStatusPolicy.DecideIngestGate(new OutputSlitSapFileStatus(OutputSlitSapStatus.Pending, DateTime.UtcNow, 0)));

    [Fact]
    public void Accepted_is_gated_as_accepted() =>
        Assert.Equal(
            OutputSlitIngestGate.Accepted,
            OutputSlitSapStatusPolicy.DecideIngestGate(new OutputSlitSapFileStatus(OutputSlitSapStatus.Accepted, DateTime.UtcNow, 0)));

    [Fact]
    public void Rejected_in_flight_is_gated() =>
        Assert.Equal(
            OutputSlitIngestGate.RejectedInFlight,
            OutputSlitSapStatusPolicy.DecideIngestGate(new OutputSlitSapFileStatus(OutputSlitSapStatus.Rejected, DateTime.UtcNow, 0)));

    [Fact]
    public void Resubmitted_pending_is_gated_as_resubmitted() =>
        Assert.Equal(
            OutputSlitIngestGate.Resubmitted,
            OutputSlitSapStatusPolicy.DecideIngestGate(new OutputSlitSapFileStatus(OutputSlitSapStatus.Pending, DateTime.UtcNow, 1)));

    // ---- Output CSV parsing (ResubmitDriftPlanner.ParseOutputCsvSums) ----

    [Fact]
    public void Parse_sums_rows_per_batch_and_slit_and_skips_unstamped_rows()
    {
        var sums = ResubmitDriftPlanner.ParseOutputCsvSums(new[]
        {
            "PO Number,Slit No,NDT Pipes,NDT Batch No",
            "1000060363,01,7,1226100001",
            "1000060363,01,5,1226100001",   // same (batch, slit) → summed
            "1000060363,02,8,1226100002",
            "1000060363,03,4,",             // unstamped → never posted to SAP → ignored
            string.Empty
        });

        Assert.NotNull(sums);
        Assert.Equal(2, sums!.Count);
        Assert.Equal(12, sums[("1226100001", "01")]);
        Assert.Equal(8, sums[("1226100002", "02")]);
    }

    [Fact]
    public void Parse_returns_null_for_empty_file() =>
        Assert.Null(ResubmitDriftPlanner.ParseOutputCsvSums(Array.Empty<string>()));

    // ---- Drift plan (ResubmitDriftPlanner.Compute) ----

    [Fact]
    public void Compute_reports_value_changes_and_row_add_remove_anomalies()
    {
        var file = new Dictionary<(string, string), int>
        {
            [(BatchNo, "01")] = 12,   // value drift: SQL has 10
            [(BatchNo, "02")] = 8,    // unchanged
            [(BatchNo, "03")] = 5     // file-only: row added in the edited file
        };
        var sql = new Dictionary<(string, string), int>
        {
            [(BatchNo, "01")] = 10,
            [(BatchNo, "02")] = 8,
            [(BatchNo, "04")] = 3     // SQL-only: row removed in the edited file
        };

        var plan = ResubmitDriftPlanner.Compute(file, sql);

        Assert.True(plan.HasDrift);
        var change = Assert.Single(plan.Changes);
        Assert.Equal((BatchNo, "01", 10, 12), (change.BatchNo, change.SlitNo, change.SqlNdtPipes, change.FileNdtPipes));
        Assert.Equal((BatchNo, "03", 5), Assert.Single(plan.FileOnly));
        Assert.Equal((BatchNo, "04", 3), Assert.Single(plan.SqlOnly));
    }

    [Fact]
    public void Compute_reports_no_drift_when_file_matches_sql()
    {
        var same = new Dictionary<(string, string), int> { [(BatchNo, "01")] = 10 };
        Assert.False(ResubmitDriftPlanner.Compute(same, same).HasDrift);
    }

    // ---- Drift service end-to-end (temp pending file + fakes) ----

    [Fact]
    public async Task Drift_syncs_sql_via_slit_reconcile_path_and_bundle_total_for_unlocked_bundle()
    {
        using var pending = new TempPendingFolder(
            FileName,
            "PO Number,Slit No,NDT Pipes,NDT Batch No",
            $"1000060363,01,12,{BatchNo}",
            $"1000060363,02,8,{BatchNo}");
        var bundleRepo = new FakeDriftBundleRepo
        {
            SqlSums = { (BatchNo, "01", 10), (BatchNo, "02", 8) }
        };
        var reconcileSync = new RecordingReconcileSync();
        var sapRepo = new RecordingSapRepo();
        var service = new ResubmitDriftService(
            bundleRepo, reconcileSync, sapRepo, NullLogger<ResubmitDriftService>.Instance);

        var result = await service.DetectAndReconcileAsync(pending.Folder, FileName, CancellationToken.None);

        Assert.NotNull(result);
        Assert.Equal(1, result!.SlitsSynced);
        // Only the drifted slit is re-synced, with the resubmitted file's value.
        Assert.Equal((BatchNo, "01", 12), Assert.Single(reconcileSync.SlitSyncs));
        // Q5: the bundle total follows the new slit sum (unlocked bundle → forced slit-sum sync).
        Assert.Equal(BatchNo, Assert.Single(bundleRepo.TotalSyncs));
        Assert.Empty(bundleRepo.PostReconSumRefreshes);
        Assert.Equal(BatchNo, Assert.Single(result.BatchTotalsSynced));
        // Auditable marker in Output_Slit_Sap_Status_Event.
        Assert.Equal(FileName, Assert.Single(sapRepo.DriftEvents));
    }

    [Fact]
    public async Task Drift_on_manual_recon_locked_bundle_keeps_total_and_refreshes_post_recon_sum_only()
    {
        using var pending = new TempPendingFolder(
            FileName,
            "PO Number,Slit No,NDT Pipes,NDT Batch No",
            $"1000060363,01,12,{BatchNo}");
        var bundleRepo = new FakeDriftBundleRepo
        {
            ManualReconLocked = true,
            SqlSums = { (BatchNo, "01", 10) }
        };
        var reconcileSync = new RecordingReconcileSync();
        var service = new ResubmitDriftService(
            bundleRepo, reconcileSync, new RecordingSapRepo(), NullLogger<ResubmitDriftService>.Instance);

        var result = await service.DetectAndReconcileAsync(pending.Folder, FileName, CancellationToken.None);

        Assert.NotNull(result);
        // Per-slit SQL still follows the file (same as operator slit reconcile on a locked bundle) …
        Assert.Equal((BatchNo, "01", 12), Assert.Single(reconcileSync.SlitSyncs));
        // … but the locked bundle total is untouched; only Post_Recon_Csv_Sum refreshes.
        Assert.Empty(bundleRepo.TotalSyncs);
        Assert.Equal(BatchNo, Assert.Single(bundleRepo.PostReconSumRefreshes));
        Assert.Equal(BatchNo, Assert.Single(result!.ManualReconLockedBatches));
    }

    [Fact]
    public async Task No_drift_means_no_sync_calls_and_no_event()
    {
        using var pending = new TempPendingFolder(
            FileName,
            "PO Number,Slit No,NDT Pipes,NDT Batch No",
            $"1000060363,01,10,{BatchNo}");
        var bundleRepo = new FakeDriftBundleRepo { SqlSums = { (BatchNo, "01", 10) } };
        var reconcileSync = new RecordingReconcileSync();
        var sapRepo = new RecordingSapRepo();
        var service = new ResubmitDriftService(
            bundleRepo, reconcileSync, sapRepo, NullLogger<ResubmitDriftService>.Instance);

        var result = await service.DetectAndReconcileAsync(pending.Folder, FileName, CancellationToken.None);

        Assert.NotNull(result);
        Assert.False(result!.Plan.HasDrift);
        Assert.Empty(reconcileSync.SlitSyncs);
        Assert.Empty(bundleRepo.TotalSyncs);
        Assert.Empty(sapRepo.DriftEvents);
    }

    [Fact]
    public async Task Missing_pending_file_or_missing_sql_rows_is_a_noop()
    {
        using var pending = new TempPendingFolder(
            FileName,
            "PO Number,Slit No,NDT Pipes,NDT Batch No",
            $"1000060363,01,12,{BatchNo}");
        var reconcileSync = new RecordingReconcileSync();
        var service = new ResubmitDriftService(
            new FakeDriftBundleRepo(), reconcileSync, new RecordingSapRepo(), NullLogger<ResubmitDriftService>.Instance);

        // SQL has no rows for the basename (e.g. ingest predates SQL traceability) → no sync.
        Assert.Null(await service.DetectAndReconcileAsync(pending.Folder, FileName, CancellationToken.None));
        // Pending file vanished (SAP pulled it between poll and drift check) → no sync.
        Assert.Null(await service.DetectAndReconcileAsync(pending.Folder, "does-not-exist.csv", CancellationToken.None));
        Assert.Empty(reconcileSync.SlitSyncs);
    }

    // ---- helpers / fakes ----

    private sealed class TempPendingFolder : IDisposable
    {
        public string Folder { get; }

        public TempPendingFolder(string fileName, params string[] lines)
        {
            Folder = Path.Combine(Path.GetTempPath(), "ndt-phase4-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Folder);
            File.WriteAllLines(Path.Combine(Folder, fileName), lines);
        }

        public void Dispose()
        {
            try
            {
                Directory.Delete(Folder, recursive: true);
            }
            catch
            {
                // best-effort temp cleanup
            }
        }
    }

    private sealed class RecordingReconcileSync : IReconcileSyncService
    {
        public List<(string BatchNo, string SlitNo, int NdtPipes)> SlitSyncs { get; } = new();

        public Task SyncAfterBundleTotalReconcileAsync(string ndtBatchNo, string poNumber, int newBundleTotalPcs, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task<int> SyncAfterSlitReconcileAsync(string ndtBatchNo, string slitNo, int newNdtPipes, CancellationToken cancellationToken)
        {
            SlitSyncs.Add((ndtBatchNo, slitNo, newNdtPipes));
            return Task.FromResult(1);
        }

        public Task SyncAfterManualStationReconcileAsync(
            ManualTagStation station, ManualStationReconcileSnapshot snapshot, string? ndtProcessCsvPath, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class RecordingSapRepo : IOutputSlitSapStatusRepository
    {
        public List<string> DriftEvents { get; } = new();

        public bool Enabled => true;

        public Task<OutputSlitSapStatusApplyResult> ApplyObservationsAsync(IReadOnlyList<OutputSlitSapStatusObservation> observations, CancellationToken cancellationToken) =>
            Task.FromResult(OutputSlitSapStatusApplyResult.Empty);

        public Task RecordOutputFileWrittenAsync(string fileName, DateTime? fileLastWriteTimeUtc, string outputFolder, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task RecordResubmitDriftSyncedEventAsync(string fileName, string pendingFolder, CancellationToken cancellationToken)
        {
            DriftEvents.Add(fileName);
            return Task.CompletedTask;
        }

        public Task<IReadOnlyDictionary<string, OutputSlitSapFileStatus>> GetStatusesForFilesAsync(
            IReadOnlyCollection<string> fileNames, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<string, OutputSlitSapFileStatus>>(
                new Dictionary<string, OutputSlitSapFileStatus>(StringComparer.OrdinalIgnoreCase));
    }

    /// <summary>Records drift-relevant calls; every other repository member is inert.</summary>
    private sealed class FakeDriftBundleRepo : INdtBundleRepository
    {
        public bool ManualReconLocked { get; init; }
        public List<(string BatchNo, string SlitNo, int NdtPipes)> SqlSums { get; } = new();
        public List<string> TotalSyncs { get; } = new();
        public List<string> PostReconSumRefreshes { get; } = new();

        public Task<IReadOnlyList<(string BatchNo, string SlitNo, int NdtPipes)>> GetOutputSlitRowSumsForSourceFileAsync(
            string sourceFileBaseName, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string, string, int)>>(SqlSums);

        public Task<bool> IsManualReconLockedAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult(ManualReconLocked);

        public Task<int> TryUpdatePostReconCsvSumAsync(string batchNo, CancellationToken cancellationToken)
        {
            PostReconSumRefreshes.Add(batchNo);
            return Task.FromResult(1);
        }

        public Task<int> TrySyncBundleTotalFromSlitsAsync(string batchNo, bool forceFromSlits, CancellationToken cancellationToken)
        {
            Assert.True(forceFromSlits); // drift sync must force the total from the new slit sums
            TotalSyncs.Add(batchNo);
            return Task.FromResult(1);
        }

        public Task<NdtBundleRecord?> GetByBatchNoAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(null);
        public Task<IReadOnlyList<(string SlitNo, int NdtPipes)>> GetSlitsForBatchAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string, int)>>(Array.Empty<(string, int)>());
        public Task<int> UpdateOutputCsvFilesForSlitAsync(string batchNo, string slitNo, int newPipes, CancellationToken cancellationToken) =>
            Task.FromResult(0);
        public Task RecordBundleAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RecordBundlePendingPrintAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task UpdateBundlePrintStatusAsync(string bundleNo, string printStatus, string? printError, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<IReadOnlyList<NdtBundleRecord>> GetStuckPrintsAsync(TimeSpan olderThan, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
        public Task<IReadOnlyList<NdtBundleRecord>> GetBundlesAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
        public Task<NdtBundleRecord?> GetLatestPrintedBundleForMillAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(null);
        public Task<bool> HasPrintedBundleForPoAsync(int millNo, string poNumber, CancellationToken cancellationToken) =>
            Task.FromResult(false);
        public Task<int> MarkManualReviewAsync(string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult(0);
        public Task TrySetPlcCloseMetadataAsync(int engineBatchSequence, int millNo, CancellationToken cancellationToken) =>
            Task.CompletedTask;
        public Task<(string BundleNo, int EngineSequence, int PlcTotal)?> TryGetAwaitingPlcReconBatchAsync(
            string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<(string BundleNo, int EngineSequence, int PlcTotal)?>(null);
        public Task<IReadOnlyList<PlcCsvReconAwaitingBundle>> ListAwaitingPlcReconBatchesAsync(
            string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<PlcCsvReconAwaitingBundle>>(Array.Empty<PlcCsvReconAwaitingBundle>());
        public Task<PlcCsvReconResult?> TryFinalizePlcReconBundleAsync(
            string bundleNo, int slitSum, int reconWindowMinutes, DateTime utcNow, bool force, CancellationToken cancellationToken) =>
            Task.FromResult<PlcCsvReconResult?>(null);
        public Task<IReadOnlyList<PlcCsvReconResult>> TryFinalizeReadyPlcReconBundlesAsync(
            string poNumber, int millNo, int reconWindowMinutes, DateTime utcNow, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<PlcCsvReconResult>>(Array.Empty<PlcCsvReconResult>());
        public Task<PlcCsvReconResult?> TryReconcilePlcClosedBundleAsync(string poNumber, int millNo, int slitSum, CancellationToken cancellationToken) =>
            Task.FromResult<PlcCsvReconResult?>(null);
        public Task<PlcCsvReconResult?> TryForceFinalizeAwaitingReconOnReopenAsync(string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<PlcCsvReconResult?>(null);
        public Task UpdateBundlePipesAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<int> UpdateOutputCsvFilesForBundleAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task UpdateBundleTotalInDatabaseAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<bool> UpdateBundleSummaryCsvAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) =>
            Task.FromResult(false);
        public Task<(int RowsRemoved, IReadOnlyList<RemovedSlitRowTraceRef> TraceRefs)> DeletePerSlitOutputRowsForBatchSlitsAsync(
            string batchNo, IReadOnlyList<string> slitNos, CancellationToken cancellationToken) =>
            Task.FromResult<(int, IReadOnlyList<RemovedSlitRowTraceRef>)>((0, Array.Empty<RemovedSlitRowTraceRef>()));
    }
}
