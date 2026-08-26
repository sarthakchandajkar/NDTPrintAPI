using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.PoLifecycle;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// PO 1000060288 / Mill-1 incident: PO closed + manual-recon locked, late real-time slit must not open bundle 1226100002.
/// </summary>
public sealed class ClosedPoLateSlitReplayTests
{
    private const string Po = "1000060288";
    private const int Mill = 1;
    private const string LockedBundle = "1226100001";

    [Fact]
    public void RealTimePath_closed_po_routes_traceability_only_not_normal_bundle()
    {
        var action = ClosedPoSlitIngestPolicy.DecideForRow(
            isBackfill: false,
            BackfillCoverageKind.None,
            PoLifecyclePhase.Closed,
            MillPoEndSource.Plc,
            autoCloseOrphanBundles: true);

        Assert.Equal(BackfillBundlingAction.TraceabilityOnly, action);
        Assert.NotEqual(BackfillBundlingAction.NormalBundle, action);
        Assert.NotEqual(BackfillBundlingAction.ManualReview, action);
    }

    [Fact]
    public void BackfillPath_closed_none_is_traceability_only_not_orphan_auto_close()
    {
        var action = ClosedPoSlitIngestPolicy.DecideForRow(
            isBackfill: true,
            BackfillCoverageKind.None,
            PoLifecyclePhase.Closed,
            MillPoEndSource.Plc,
            autoCloseOrphanBundles: true);

        Assert.Equal(BackfillBundlingAction.TraceabilityOnly, action);
    }

    [Fact]
    public void Running_po_real_time_none_remains_normal_bundle()
    {
        var action = ClosedPoSlitIngestPolicy.DecideForRow(
            isBackfill: false,
            BackfillCoverageKind.None,
            PoLifecyclePhase.Running,
            MillPoEndSource.Plc,
            autoCloseOrphanBundles: true);

        Assert.Equal(BackfillBundlingAction.NormalBundle, action);
    }

    [Fact]
    public async Task IncidentReplay_manual_recon_locked_attaches_late_row_no_new_sequence()
    {
        var repo = new IncidentReplayBundleRepo(LockedBundle);
        var record = new InputSlitRecord
        {
            PoNumber = Po,
            MillNo = Mill,
            SlitNo = "04",
            NdtPipes = 3
        };

        var action = ClosedPoSlitIngestPolicy.DecideForRow(
            isBackfill: false,
            BackfillCoverageKind.None,
            PoLifecyclePhase.Closed,
            MillPoEndSource.Plc,
            autoCloseOrphanBundles: true);
        Assert.Equal(BackfillBundlingAction.TraceabilityOnly, action);

        var route = await ClosedPoTraceabilityBatchResolver.ResolveAsync(new StubCsvFill(repo), record, Mill, "2", CancellationToken.None);

        Assert.False(route.RequiresManualReview);
        Assert.Equal(LockedBundle, route.BatchNoFormatted);

        if (await repo.IsManualReconLockedAsync(LockedBundle, CancellationToken.None))
            await repo.TryUpdatePostReconCsvSumAsync(LockedBundle, CancellationToken.None);

        Assert.Equal(1, repo.PostReconUpdateCount);
        Assert.Equal(0, repo.SyncFromSlitsCallCount);
    }

    [Fact]
    public async Task Closed_po_no_existing_bundle_flags_manual_review_without_batch()
    {
        var repo = new IncidentReplayBundleRepo(existingBundle: null);
        var record = new InputSlitRecord { PoNumber = Po, MillNo = Mill, SlitNo = "04", NdtPipes = 3 };

        var route = await ClosedPoTraceabilityBatchResolver.ResolveAsync(new StubCsvFill(repo), record, Mill, "2", CancellationToken.None);

        Assert.True(route.RequiresManualReview);
        Assert.Null(route.BatchNoFormatted);
    }

    [Fact]
    public async Task Traceability_lookup_never_selects_different_po_on_same_mill()
    {
        const string otherPo = "1000060363";
        const string otherBundle = "1226100003";
        var repo = new ScopingBundleRepo(
            (Po, Mill, "2", LockedBundle),
            (otherPo, Mill, "2", otherBundle));

        var route = await ClosedPoTraceabilityBatchResolver.ResolveAsync(
            new StubCsvFill(repo),
            new InputSlitRecord { PoNumber = Po, MillNo = Mill, SlitNo = "04", NdtPipes = 3 },
            Mill,
            "2",
            CancellationToken.None);

        Assert.Equal(LockedBundle, route.BatchNoFormatted);
        Assert.NotEqual(otherBundle, route.BatchNoFormatted);
    }

    [Fact]
    public async Task Traceability_lookup_respects_pipe_size_when_multiple_po_bundles_exist()
    {
        var repo = new ScopingBundleRepo(
            (Po, Mill, "2", LockedBundle),
            (Po, Mill, "4", "1226100099"));

        var route = await ClosedPoTraceabilityBatchResolver.ResolveAsync(
            new StubCsvFill(repo),
            new InputSlitRecord { PoNumber = Po, MillNo = Mill, SlitNo = "04", NdtPipes = 3 },
            Mill,
            "2",
            CancellationToken.None);

        Assert.Equal(LockedBundle, route.BatchNoFormatted);
    }

    [Fact]
    public async Task Awaiting_recon_empty_falls_back_to_po_mill_size_lookup_not_mill_latest()
    {
        // After fill cutover: incomplete fill / traceability lookup is PO+mill scoped via StubCsvFill.
        var repo = new ScopingBundleRepo(
            (Po, Mill, "2", LockedBundle),
            ("1000060363", Mill, "2", "1226100003"));

        var route = await ClosedPoTraceabilityBatchResolver.ResolveAsync(
            new StubCsvFill(repo),
            new InputSlitRecord { PoNumber = Po, MillNo = Mill, SlitNo = "04", NdtPipes = 3 },
            Mill,
            "2",
            CancellationToken.None);

        Assert.Equal(LockedBundle, route.BatchNoFormatted);
        Assert.NotEqual("1226100003", route.BatchNoFormatted);
    }

    [Theory]
    [InlineData(46, 49, 0, true)]
    [InlineData(46, 46, 0, false)]
    [InlineData(46, 48, 2, false)]
    [InlineData(46, 49, 2, true)]
    public void PostReconCsvSumGuard_warns_when_sum_exceeds_locked_total_by_margin(
        int locked,
        int postRecon,
        int margin,
        bool expectWarn)
    {
        Assert.Equal(expectWarn, PostReconCsvSumGuard.ShouldWarn(locked, postRecon, margin));
    }

    [Fact]
    public void PostReconCsvSum_is_monotonic_full_recompute_not_incremental()
    {
        // Documented behavior: each late attach triggers full SUM(Output_Slit_Row) recompute.
        // Unbounded growth is possible; PostReconCsvSumGuard surfaces drift vs locked total.
        var locked = 46;
        var afterAttach1 = 49;
        var afterAttach2 = 52;
        Assert.True(PostReconCsvSumGuard.ShouldWarn(locked, afterAttach1, 0));
        Assert.True(PostReconCsvSumGuard.ShouldWarn(locked, afterAttach2, 0));
    }

    [Fact]
    public void Orphan_sweep_skips_peek_stamped_orphan_without_open_partial_even_when_guard_passes()
    {
        var runtime = new OrphanProbeRuntime();
        runtime.SetLastActivityUtc(Po, Mill, DateTime.UtcNow.AddMinutes(-30));

        Assert.False(NdtBundleRuntimeStateLogic.HasOpenPartialBundle(
            runtime.GetRunningTotal(Po, Mill),
            runtime.GetSizeCounts(Po, Mill)));

        Assert.True(OrphanSweepGuard.ShouldSweepClosedPo(
            Mill,
            Po,
            PoLifecyclePhase.Closed,
            millRunningPo: "1000060363",
            runtime.GetLastActivityUtc(Po, Mill),
            DateTime.UtcNow,
            orphanQuiescenceMinutes: 15));
    }

    [Fact]
    public void Orphan_sweep_would_eventually_catch_processSlitRecord_orphan_after_quiescence()
    {
        var runtime = new OrphanProbeRuntime();
        runtime.SetOpenPartial(Po, Mill, pcs: 3);
        runtime.SetLastActivityUtc(Po, Mill, DateTime.UtcNow.AddMinutes(-20));

        Assert.True(NdtBundleRuntimeStateLogic.HasOpenPartialBundle(
            runtime.GetRunningTotal(Po, Mill),
            runtime.GetSizeCounts(Po, Mill)));

        Assert.True(OrphanSweepGuard.ShouldSweepClosedPo(
            Mill,
            Po,
            PoLifecyclePhase.Closed,
            millRunningPo: "1000060363",
            runtime.GetLastActivityUtc(Po, Mill),
            DateTime.UtcNow,
            orphanQuiescenceMinutes: 15));
    }

    private sealed class IncidentReplayBundleRepo : INdtBundleRepository
    {
        private readonly string? _existingBundle;

        public IncidentReplayBundleRepo(string? existingBundle) => _existingBundle = existingBundle;

        public int PostReconUpdateCount { get; private set; }
        public int SyncFromSlitsCallCount { get; private set; }

        public Task<string?> TryFindTraceabilityBundleForPoMillAsync(
            string poNumber,
            int millNo,
            string? pipeSize,
            CancellationToken cancellationToken) =>
            Task.FromResult(_existingBundle);

        public Task<bool> IsManualReconLockedAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult(batchNo == LockedBundle);

        public Task<int> TryUpdatePostReconCsvSumAsync(string batchNo, CancellationToken cancellationToken)
        {
            if (batchNo == LockedBundle)
                PostReconUpdateCount++;
            return Task.FromResult(49);
        }

        public Task<IReadOnlyList<PlcCsvReconAwaitingBundle>> ListAwaitingPlcReconBatchesAsync(
            string poNumber,
            int millNo,
            CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<PlcCsvReconAwaitingBundle>>([]);

        public Task RecordBundleAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RecordBundlePendingPrintAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task UpdateBundlePrintStatusAsync(string bundleNo, string printStatus, string? printError, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<IReadOnlyList<NdtBundleRecord>> GetStuckPrintsAsync(TimeSpan olderThan, CancellationToken cancellationToken) => Task.FromResult<IReadOnlyList<NdtBundleRecord>>([]);
        public Task<IReadOnlyList<NdtBundleRecord>> GetBundlesAsync(CancellationToken cancellationToken) => Task.FromResult<IReadOnlyList<NdtBundleRecord>>([]);
        public Task<NdtBundleRecord?> GetByBatchNoAsync(string batchNo, CancellationToken cancellationToken) => Task.FromResult<NdtBundleRecord?>(null);
        public Task UpdateBundlePipesAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<int> UpdateOutputCsvFilesForBundleAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<IReadOnlyList<(string SlitNo, int NdtPipes)>> GetSlitsForBatchAsync(string batchNo, CancellationToken cancellationToken) => Task.FromResult<IReadOnlyList<(string, int)>>([]);
        public Task<int> UpdateOutputCsvFilesForSlitAsync(string batchNo, string slitNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task UpdateBundleTotalInDatabaseAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<bool> UpdateBundleSummaryCsvAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.FromResult(false);
        public Task<int> TrySyncBundleTotalFromSlitsAsync(string batchNo, bool forceFromSlits, CancellationToken cancellationToken)
        {
            SyncFromSlitsCallCount++;
            return Task.FromResult(0);
        }
        public Task<(int RowsRemoved, IReadOnlyList<RemovedSlitRowTraceRef> TraceRefs)> DeletePerSlitOutputRowsForBatchSlitsAsync(string batchNo, IReadOnlyList<string> slitNos, CancellationToken cancellationToken) => Task.FromResult((0, (IReadOnlyList<RemovedSlitRowTraceRef>)[]));
        public Task<NdtBundleRecord?> GetLatestPrintedBundleForMillAsync(int millNo, CancellationToken cancellationToken) => Task.FromResult<NdtBundleRecord?>(null);
        public Task<bool> HasPrintedBundleForPoAsync(int millNo, string poNumber, CancellationToken cancellationToken) => Task.FromResult(_existingBundle is not null);
        public Task<int> MarkManualReviewAsync(string poNumber, int millNo, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task TrySetPlcCloseMetadataAsync(int engineBatchSequence, int millNo, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<(string BundleNo, int EngineSequence, int PlcTotal)?> TryGetAwaitingPlcReconBatchAsync(string poNumber, int millNo, CancellationToken cancellationToken) => Task.FromResult<(string, int, int)?>(null);
        public Task<PlcCsvReconResult?> TryReconcilePlcClosedBundleAsync(string poNumber, int millNo, int slitSum, CancellationToken cancellationToken) => Task.FromResult<PlcCsvReconResult?>(null);
        public Task<PlcCsvReconResult?> TryFinalizePlcReconBundleAsync(string bundleNo, int slitSum, int reconWindowMinutes, DateTime utcNow, bool force, CancellationToken cancellationToken) => Task.FromResult<PlcCsvReconResult?>(null);
        public Task<IReadOnlyList<PlcCsvReconResult>> TryFinalizeReadyPlcReconBundlesAsync(string poNumber, int millNo, int reconWindowMinutes, DateTime utcNow, CancellationToken cancellationToken) => Task.FromResult<IReadOnlyList<PlcCsvReconResult>>([]);
        public Task<PlcCsvReconResult?> TryForceFinalizeAwaitingReconOnReopenAsync(string poNumber, int millNo, CancellationToken cancellationToken) => Task.FromResult<PlcCsvReconResult?>(null);
        public Task<ManualBundleReconcileResult?> ManualReconcileBundleAsync(string batchNo, int correctedTotal, string reason, string reconciledBy, CancellationToken cancellationToken) => Task.FromResult<ManualBundleReconcileResult?>(null);
    }

    /// <summary>In-memory PO+mill+size scoped lookup mirroring production SQL rules.</summary>
    private sealed class ScopingBundleRepo : INdtBundleRepository
    {
        private readonly List<(string Po, int Mill, string Size, string Bundle)> _rows;

        public ScopingBundleRepo(params (string Po, int Mill, string Size, string Bundle)[] rows) =>
            _rows = rows.ToList();

        public Task<string?> TryFindTraceabilityBundleForPoMillAsync(
            string poNumber,
            int millNo,
            string? pipeSize,
            CancellationToken cancellationToken)
        {
            var sizeNorm = FormationChartLookup.NormalizePipeSizeKey(pipeSize);
            var match = _rows
                .Where(r => r.Mill == millNo && InputSlitCsvParsing.PoEquals(r.Po, poNumber))
                .Where(r => string.IsNullOrEmpty(sizeNorm)
                            || string.IsNullOrEmpty(r.Size)
                            || FormationChartLookup.NormalizePipeSizeKey(r.Size) == sizeNorm)
                .Select(r => r.Bundle)
                .FirstOrDefault();
            return Task.FromResult<string?>(match);
        }

        public Task<IReadOnlyList<PlcCsvReconAwaitingBundle>> ListAwaitingPlcReconBatchesAsync(
            string poNumber,
            int millNo,
            CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<PlcCsvReconAwaitingBundle>>([]);

        public Task RecordBundleAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RecordBundlePendingPrintAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task UpdateBundlePrintStatusAsync(string bundleNo, string printStatus, string? printError, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<IReadOnlyList<NdtBundleRecord>> GetStuckPrintsAsync(TimeSpan olderThan, CancellationToken cancellationToken) => Task.FromResult<IReadOnlyList<NdtBundleRecord>>([]);
        public Task<IReadOnlyList<NdtBundleRecord>> GetBundlesAsync(CancellationToken cancellationToken) => Task.FromResult<IReadOnlyList<NdtBundleRecord>>([]);
        public Task<NdtBundleRecord?> GetByBatchNoAsync(string batchNo, CancellationToken cancellationToken) => Task.FromResult<NdtBundleRecord?>(null);
        public Task UpdateBundlePipesAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<int> UpdateOutputCsvFilesForBundleAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<IReadOnlyList<(string SlitNo, int NdtPipes)>> GetSlitsForBatchAsync(string batchNo, CancellationToken cancellationToken) => Task.FromResult<IReadOnlyList<(string, int)>>([]);
        public Task<int> UpdateOutputCsvFilesForSlitAsync(string batchNo, string slitNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task UpdateBundleTotalInDatabaseAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<bool> UpdateBundleSummaryCsvAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.FromResult(false);
        public Task<int> TrySyncBundleTotalFromSlitsAsync(string batchNo, bool forceFromSlits, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<(int RowsRemoved, IReadOnlyList<RemovedSlitRowTraceRef> TraceRefs)> DeletePerSlitOutputRowsForBatchSlitsAsync(string batchNo, IReadOnlyList<string> slitNos, CancellationToken cancellationToken) => Task.FromResult((0, (IReadOnlyList<RemovedSlitRowTraceRef>)[]));
        public Task<NdtBundleRecord?> GetLatestPrintedBundleForMillAsync(int millNo, CancellationToken cancellationToken) => Task.FromResult<NdtBundleRecord?>(null);
        public Task<bool> HasPrintedBundleForPoAsync(int millNo, string poNumber, CancellationToken cancellationToken) => Task.FromResult(false);
        public Task<int> MarkManualReviewAsync(string poNumber, int millNo, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task TrySetPlcCloseMetadataAsync(int engineBatchSequence, int millNo, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<(string BundleNo, int EngineSequence, int PlcTotal)?> TryGetAwaitingPlcReconBatchAsync(string poNumber, int millNo, CancellationToken cancellationToken) => Task.FromResult<(string, int, int)?>(null);
        public Task<PlcCsvReconResult?> TryReconcilePlcClosedBundleAsync(string poNumber, int millNo, int slitSum, CancellationToken cancellationToken) => Task.FromResult<PlcCsvReconResult?>(null);
        public Task<PlcCsvReconResult?> TryFinalizePlcReconBundleAsync(string bundleNo, int slitSum, int reconWindowMinutes, DateTime utcNow, bool force, CancellationToken cancellationToken) => Task.FromResult<PlcCsvReconResult?>(null);
        public Task<IReadOnlyList<PlcCsvReconResult>> TryFinalizeReadyPlcReconBundlesAsync(string poNumber, int millNo, int reconWindowMinutes, DateTime utcNow, CancellationToken cancellationToken) => Task.FromResult<IReadOnlyList<PlcCsvReconResult>>([]);
        public Task<PlcCsvReconResult?> TryForceFinalizeAwaitingReconOnReopenAsync(string poNumber, int millNo, CancellationToken cancellationToken) => Task.FromResult<PlcCsvReconResult?>(null);
        public Task<bool> IsManualReconLockedAsync(string batchNo, CancellationToken cancellationToken) => Task.FromResult(false);
        public Task<int> TryUpdatePostReconCsvSumAsync(string batchNo, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<ManualBundleReconcileResult?> ManualReconcileBundleAsync(string batchNo, int correctedTotal, string reason, string reconciledBy, CancellationToken cancellationToken) => Task.FromResult<ManualBundleReconcileResult?>(null);
    }

    private sealed class OrphanProbeRuntime
    {
        private readonly Dictionary<(string Po, int Mill), Dictionary<string, int>> _sizeCounts = new();
        private readonly Dictionary<(string Po, int Mill), int> _running = new();
        private readonly Dictionary<(string Po, int Mill), DateTime> _lastActivity = new();

        public void SetOpenPartial(string po, int mill, int pcs)
        {
            _sizeCounts[(po, mill)] = new Dictionary<string, int> { ["Default"] = pcs };
            _running[(po, mill)] = pcs;
        }

        public void SetLastActivityUtc(string po, int mill, DateTime utc) => _lastActivity[(po, mill)] = utc;

        public IReadOnlyDictionary<string, int> GetSizeCounts(string po, int mill) =>
            _sizeCounts.GetValueOrDefault((po, mill), new Dictionary<string, int>());

        public int GetRunningTotal(string po, int mill) => _running.GetValueOrDefault((po, mill));

        public DateTime GetLastActivityUtc(string po, int mill) =>
            _lastActivity.GetValueOrDefault((po, mill));
    }

    private sealed class StubCsvFill : ICsvFillService
    {
        private readonly INdtBundleRepository _repo;
        public StubCsvFill(INdtBundleRepository repo) => _repo = repo;
        public Task TryInitializeFillTargetAsync(string bundleNo, int targetNdtPcs, string? closeSource, CancellationToken cancellationToken) => Task.CompletedTask;
        public async Task<CsvFillIncompleteBundle?> TryGetOldestIncompleteAsync(string poNumber, int millNo, string? pipeSize, CancellationToken cancellationToken)
        {
            var b = await _repo.TryFindTraceabilityBundleForPoMillAsync(poNumber, millNo, pipeSize, cancellationToken);
            return string.IsNullOrWhiteSpace(b) ? null : new CsvFillIncompleteBundle(b!, 1, 0, CsvFillState.PlcClosed, null, null);
        }
        public Task<CsvFillStampResult?> TryStampFileAsync(string poNumber, int millNo, string? pipeSize, int fileNdtPipes, CancellationToken cancellationToken) => Task.FromResult<CsvFillStampResult?>(null);
        public Task<int> AdvanceQuietShortAsync(string? poNumber, int? millNo, int quietMinutes, DateTime utcNow, bool forcePoEnd, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task UpsertHoldAsync(string sourceFileName, string poNumber, int millNo, string? pipeSize, string reasonCode, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<int> EscalateExpiredHoldsAsync(int quietMinutes, DateTime utcNow, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task ApplyCountRevisionAsync(string sourceFileName, string batchNo, int oldNdtPipes, int newNdtPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<Guid> ApplyBatchMoveAsync(string sourceFileName, string oldBatchNo, string newBatchNo, int ndtPipes, CancellationToken cancellationToken) => Task.FromResult(Guid.Empty);
        public Task<bool> HasAwaitingCsvReconRowsAsync(CancellationToken cancellationToken) => Task.FromResult(false);
        public Task<bool> HasBundlesMissingFillTargetAsync(CancellationToken cancellationToken) => Task.FromResult(false);
    }
}

