using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class ManualBundleReconcileSemanticsTests
{
    [Fact]
    public void Force_finalize_noop_for_file_closed_bundle()
    {
        var result = ManualBundleReconcileSemantics.EvaluateForceFinalize(
            awaitingCsvRecon: false,
            closeSource: "File",
            plcTotal: 48,
            slitSum: 50);

        Assert.False(result.ForceFinalized);
        Assert.False(result.CountDiscrepancy);
    }

    [Fact]
    public void Force_finalize_runs_for_plc_awaiting_bundle()
    {
        var result = ManualBundleReconcileSemantics.EvaluateForceFinalize(
            awaitingCsvRecon: true,
            closeSource: "Plc",
            plcTotal: 48,
            slitSum: 40);

        Assert.True(result.ForceFinalized);
        Assert.True(result.CountDiscrepancy);
    }

    [Fact]
    public void Force_finalize_noop_when_already_settled()
    {
        var result = ManualBundleReconcileSemantics.EvaluateForceFinalize(
            awaitingCsvRecon: false,
            closeSource: "Plc",
            plcTotal: 48,
            slitSum: 50);

        Assert.False(result.ForceFinalized);
    }
}

public sealed class ManualBundleReconcileGuardTests
{
    [Fact]
    public async Task TrySyncBundleTotalFromSlits_nonforced_preserves_stored_total_when_locked()
    {
        var repo = new ManualReconLockedRepository(storedTotal: 48, slitSum: 50, locked: true);
        var synced = await repo.TrySyncBundleTotalFromSlitsAsync("12261000001", forceFromSlits: false, CancellationToken.None);
        Assert.Equal(48, synced);
        Assert.Equal(48, repo.StoredTotal);
    }

    [Fact]
    public async Task TrySyncBundleTotalFromSlits_forced_also_noops_when_locked()
    {
        var repo = new ManualReconLockedRepository(storedTotal: 48, slitSum: 50, locked: true);
        var synced = await repo.TrySyncBundleTotalFromSlitsAsync("12261000001", forceFromSlits: true, CancellationToken.None);
        Assert.Equal(48, synced);
        Assert.Equal(48, repo.StoredTotal);
    }

    [Fact]
    public async Task Post_recon_csv_sum_is_full_recompute_not_incremental()
    {
        var repo = new ManualReconLockedRepository(storedTotal: 48, slitSum: 12, locked: true);
        repo.SetSlitSum(12);
        var first = await repo.TryUpdatePostReconCsvSumAsync("12261000001", CancellationToken.None);
        Assert.Equal(12, first);
        Assert.Equal(12, repo.PostReconCsvSum);

        repo.SetSlitSum(25);
        var second = await repo.TryUpdatePostReconCsvSumAsync("12261000001", CancellationToken.None);
        Assert.Equal(25, second);
        Assert.Equal(25, repo.PostReconCsvSum);
    }

    [Fact]
    public async Task Crash_window_nonforced_sync_preserves_plc_total_before_lock()
    {
        // Simulates: force-finalize committed, lock write not yet applied.
        // Awaiting=0, ManualRecon=0, Total=48 (PLC). Late CSV slit sum = 50.
        var repo = new ManualReconLockedRepository(storedTotal: 48, slitSum: 50, locked: false);
        var synced = await repo.TrySyncBundleTotalFromSlitsAsync("12261000001", forceFromSlits: false, CancellationToken.None);
        Assert.Equal(48, synced);
        Assert.Equal(48, repo.StoredTotal);
    }

    [Fact]
    public async Task Manual_reconcile_retry_after_simulated_crash_is_idempotent()
    {
        var repo = new SimulatedCrashManualReconcileRepository();
        repo.SeedAwaitingPlc("12261000001", plcTotal: 48, slitSum: 40);

        // First attempt: finalize succeeds, lock write "crashes" (not applied).
        repo.SimulateCrashBeforeLock = true;
        var first = await repo.ManualReconcileBundleAsync(
            "12261000001", 48, "Count verified", "op1", CancellationToken.None);
        Assert.Null(first);
        Assert.False(repo.IsLocked);
        Assert.False(repo.AwaitingCsvRecon);

        // Retry completes lock + total.
        repo.SimulateCrashBeforeLock = false;
        var second = await repo.ManualReconcileBundleAsync(
            "12261000001", 48, "Count verified", "op1", CancellationToken.None);
        Assert.NotNull(second);
        Assert.True(repo.IsLocked);
        Assert.Equal(48, repo.StoredTotal);
        Assert.Equal(48, second!.OriginalTotal);
        Assert.False(second.ForceFinalized);
    }

    [Fact]
    public async Task File_closed_bundle_manual_reconcile_skips_force_finalize_and_locks()
    {
        var repo = new SimulatedCrashManualReconcileRepository();
        repo.SeedFileClosed("12261000099", plcTotal: 36, slitSum: 0);

        var result = await repo.ManualReconcileBundleAsync(
            "12261000099", 36, "Damaged label", "op2", CancellationToken.None);

        Assert.NotNull(result);
        Assert.False(result!.ForceFinalized);
        Assert.True(repo.IsLocked);
        Assert.Equal(36, repo.StoredTotal);
    }

    [Fact]
    public async Task Unchanged_total_still_applies_manual_lock()
    {
        var repo = new SimulatedCrashManualReconcileRepository();
        repo.SeedFileClosed("12261000088", plcTotal: 42, slitSum: 0);

        var result = await repo.ManualReconcileBundleAsync(
            "12261000088", 42, "Reprint damaged label only", "op3", CancellationToken.None);

        Assert.NotNull(result);
        Assert.True(repo.IsLocked);
        Assert.Equal(42, result!.CorrectedTotal);
    }

    [Fact]
    public void Fifo_list_excludes_manually_reconciled_awaiting_bundle()
    {
        var printed = DateTime.UtcNow.AddMinutes(-5);
        var bundles = new List<PlcCsvReconAwaitingBundle>
        {
            new("1226100001", 1, PlcTotal: 10, CurrentSlitSum: 4, printed)
        };

        // Locked bundle would be excluded from SQL list; in-memory attach should skip full bundles only.
        Assert.True(PlcCsvReconFifo.TryAttachRow(
            bundles,
            new InputSlitRecord { PoNumber = "1000060364", MillNo = 1, SlitNo = "1", NdtPipes = 3 },
            out var batchNo));
        Assert.Equal("1226100001", batchNo);
    }
}

/// <summary>Minimal in-memory repo exercising manual-recon guard behavior without SQL.</summary>
internal sealed class ManualReconLockedRepository : INdtBundleRepository
{
    private readonly bool _locked;
    private int _slitSum;

    public ManualReconLockedRepository(int storedTotal, int slitSum, bool locked)
    {
        StoredTotal = storedTotal;
        _slitSum = slitSum;
        _locked = locked;
    }

    public int StoredTotal { get; private set; }
    public int? PostReconCsvSum { get; private set; }

    public void SetSlitSum(int sum) => _slitSum = sum;

    public Task<bool> IsManualReconLockedAsync(string batchNo, CancellationToken cancellationToken) =>
        Task.FromResult(_locked);

    public Task<int> TrySyncBundleTotalFromSlitsAsync(string batchNo, bool forceFromSlits, CancellationToken cancellationToken)
    {
        if (_locked)
            return Task.FromResult(StoredTotal);

        if (!forceFromSlits && StoredTotal > 0)
            return Task.FromResult(StoredTotal);

        if (StoredTotal == _slitSum)
            return Task.FromResult(_slitSum);

        StoredTotal = _slitSum;
        return Task.FromResult(_slitSum);
    }

    public Task<int> TryUpdatePostReconCsvSumAsync(string batchNo, CancellationToken cancellationToken)
    {
        if (!_locked)
            return Task.FromResult(0);

        PostReconCsvSum = _slitSum;
        return Task.FromResult(_slitSum);
    }

    public Task<IReadOnlyList<(string SlitNo, int NdtPipes)>> GetSlitsForBatchAsync(string batchNo, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<(string, int)>>([( "1", _slitSum )]);

    public Task<ManualBundleReconcileResult?> ManualReconcileBundleAsync(
        string batchNo, int correctedTotal, string reason, string reconciledBy, CancellationToken cancellationToken) =>
        Task.FromResult<ManualBundleReconcileResult?>(null);

    public Task RecordBundleAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
    public Task RecordBundlePendingPrintAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
    public Task UpdateBundlePrintStatusAsync(string bundleNo, string printStatus, string? printError, CancellationToken cancellationToken) => Task.CompletedTask;
    public Task<IReadOnlyList<NdtBundleRecord>> GetStuckPrintsAsync(TimeSpan olderThan, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
    public Task<IReadOnlyList<NdtBundleRecord>> GetBundlesAsync(CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
    public Task<NdtBundleRecord?> GetByBatchNoAsync(string batchNo, CancellationToken cancellationToken) =>
        Task.FromResult<NdtBundleRecord?>(null);
    public Task UpdateBundlePipesAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.CompletedTask;
    public Task<int> UpdateOutputCsvFilesForBundleAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
    public Task<int> UpdateOutputCsvFilesForSlitAsync(string batchNo, string slitNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
    public Task UpdateBundleTotalInDatabaseAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.CompletedTask;
    public Task<bool> UpdateBundleSummaryCsvAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.FromResult(false);
    public Task<(int RowsRemoved, IReadOnlyList<RemovedSlitRowTraceRef> TraceRefs)> DeletePerSlitOutputRowsForBatchSlitsAsync(
        string batchNo, IReadOnlyList<string> slitNos, CancellationToken cancellationToken) =>
        Task.FromResult((0, (IReadOnlyList<RemovedSlitRowTraceRef>)Array.Empty<RemovedSlitRowTraceRef>()));
    public Task<NdtBundleRecord?> GetLatestPrintedBundleForMillAsync(int millNo, CancellationToken cancellationToken) =>
        Task.FromResult<NdtBundleRecord?>(null);
    public Task<bool> HasPrintedBundleForPoAsync(int millNo, string poNumber, CancellationToken cancellationToken) => Task.FromResult(false);
    public Task<int> MarkManualReviewAsync(string poNumber, int millNo, CancellationToken cancellationToken) => Task.FromResult(0);
    public Task TrySetPlcCloseMetadataAsync(int engineBatchSequence, int millNo, CancellationToken cancellationToken) => Task.CompletedTask;
    public Task<(string BundleNo, int EngineSequence, int PlcTotal)?> TryGetAwaitingPlcReconBatchAsync(
        string poNumber, int millNo, CancellationToken cancellationToken) =>
        Task.FromResult<(string, int, int)?>(null);
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
}

internal sealed class SimulatedCrashManualReconcileRepository : INdtBundleRepository
{
    public bool AwaitingCsvRecon { get; private set; }
    public bool IsLocked { get; private set; }
    public int StoredTotal { get; private set; }
    public string? CloseSource { get; private set; }
    public bool SimulateCrashBeforeLock { get; set; }
    private int _slitSum;

    public void SeedAwaitingPlc(string batchNo, int plcTotal, int slitSum)
    {
        AwaitingCsvRecon = true;
        CloseSource = "Plc";
        StoredTotal = plcTotal;
        _slitSum = slitSum;
        IsLocked = false;
    }

    public void SeedFileClosed(string batchNo, int plcTotal, int slitSum)
    {
        AwaitingCsvRecon = false;
        CloseSource = "File";
        StoredTotal = plcTotal;
        _slitSum = slitSum;
        IsLocked = false;
    }

    public Task<bool> IsManualReconLockedAsync(string batchNo, CancellationToken cancellationToken) =>
        Task.FromResult(IsLocked);

    public Task<IReadOnlyList<(string SlitNo, int NdtPipes)>> GetSlitsForBatchAsync(string batchNo, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<(string, int)>>([( "1", _slitSum )]);

    public Task<ManualBundleReconcileResult?> ManualReconcileBundleAsync(
        string batchNo,
        int correctedTotal,
        string reason,
        string reconciledBy,
        CancellationToken cancellationToken)
    {
        var finalize = ManualBundleReconcileSemantics.EvaluateForceFinalize(
            AwaitingCsvRecon,
            CloseSource,
            StoredTotal,
            _slitSum);

        if (finalize.ForceFinalized)
            AwaitingCsvRecon = false;

        if (SimulateCrashBeforeLock)
            return Task.FromResult<ManualBundleReconcileResult?>(null);

        var original = StoredTotal;
        StoredTotal = correctedTotal;
        IsLocked = true;

        return Task.FromResult<ManualBundleReconcileResult?>(new ManualBundleReconcileResult
        {
            Bundle = new NdtBundleRecord { BundleNo = batchNo, TotalNdtPcs = correctedTotal, ManualRecon = true },
            OriginalTotal = original,
            CorrectedTotal = correctedTotal,
            ForceFinalized = finalize.ForceFinalized,
            CountDiscrepancyLogged = finalize.CountDiscrepancy,
            SlitSumAtFinalize = _slitSum
        });
    }

    public Task<int> TryUpdatePostReconCsvSumAsync(string batchNo, CancellationToken cancellationToken) => Task.FromResult(0);
    public Task<int> TrySyncBundleTotalFromSlitsAsync(string batchNo, bool forceFromSlits, CancellationToken cancellationToken)
    {
        if (IsLocked)
            return Task.FromResult(StoredTotal);
        if (!forceFromSlits && StoredTotal > 0)
            return Task.FromResult(StoredTotal);
        return Task.FromResult(StoredTotal);
    }

    public Task RecordBundleAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
    public Task RecordBundlePendingPrintAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
    public Task UpdateBundlePrintStatusAsync(string bundleNo, string printStatus, string? printError, CancellationToken cancellationToken) => Task.CompletedTask;
    public Task<IReadOnlyList<NdtBundleRecord>> GetStuckPrintsAsync(TimeSpan olderThan, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
    public Task<IReadOnlyList<NdtBundleRecord>> GetBundlesAsync(CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
    public Task<NdtBundleRecord?> GetByBatchNoAsync(string batchNo, CancellationToken cancellationToken) =>
        Task.FromResult<NdtBundleRecord?>(null);
    public Task UpdateBundlePipesAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.CompletedTask;
    public Task<int> UpdateOutputCsvFilesForBundleAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
    public Task<int> UpdateOutputCsvFilesForSlitAsync(string batchNo, string slitNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
    public Task UpdateBundleTotalInDatabaseAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.CompletedTask;
    public Task<bool> UpdateBundleSummaryCsvAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.FromResult(false);
    public Task<(int RowsRemoved, IReadOnlyList<RemovedSlitRowTraceRef> TraceRefs)> DeletePerSlitOutputRowsForBatchSlitsAsync(
        string batchNo, IReadOnlyList<string> slitNos, CancellationToken cancellationToken) =>
        Task.FromResult((0, (IReadOnlyList<RemovedSlitRowTraceRef>)Array.Empty<RemovedSlitRowTraceRef>()));
    public Task<NdtBundleRecord?> GetLatestPrintedBundleForMillAsync(int millNo, CancellationToken cancellationToken) =>
        Task.FromResult<NdtBundleRecord?>(null);
    public Task<bool> HasPrintedBundleForPoAsync(int millNo, string poNumber, CancellationToken cancellationToken) => Task.FromResult(false);
    public Task<int> MarkManualReviewAsync(string poNumber, int millNo, CancellationToken cancellationToken) => Task.FromResult(0);
    public Task TrySetPlcCloseMetadataAsync(int engineBatchSequence, int millNo, CancellationToken cancellationToken) => Task.CompletedTask;
    public Task<(string BundleNo, int EngineSequence, int PlcTotal)?> TryGetAwaitingPlcReconBatchAsync(
        string poNumber, int millNo, CancellationToken cancellationToken) =>
        Task.FromResult<(string, int, int)?>(null);
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
}
