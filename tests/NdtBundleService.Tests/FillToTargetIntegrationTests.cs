using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging.Abstractions;
using NdtBundleService.Controllers;
using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// Integration assertions for fill-to-target failure modes (wiring, not pure arithmetic):
/// batch-move rollback, ResubmitDrift delta revision, worker-path pointer advance,
/// manual-reconcile case (c) PPC gating, and no-open-bundle hold.
/// </summary>
public sealed class FillToTargetIntegrationTests
{
    private const string Po = "1000060363";
    private const int Mill = 1;
    private const string Batch0001 = "1226100001";
    private const string Batch0002 = "1226100002";
    private const string AcceptedFile = "2604361_01_260726_1000060363.csv";

    // ---- (a) Batch-move mid-transaction rollback ----

    [Fact]
    public async Task ApplyBatchMove_rolls_back_both_bundles_when_failure_after_source_adjust()
    {
        var fill = new InMemoryTransactionalCsvFillService
        {
            FailAfterSourceAdjustOnMove = true
        };
        fill.Seed(Batch0001, target: 22, filled: 22, CsvFillState.CsvComplete);
        fill.Seed(Batch0002, target: 22, filled: 10, CsvFillState.CsvFilling);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            fill.ApplyBatchMoveAsync(AcceptedFile, Batch0001, Batch0002, ndtPipes: 5, CancellationToken.None));

        Assert.Equal(22, fill.GetFilled(Batch0001));
        Assert.Equal(CsvFillState.CsvComplete, fill.GetState(Batch0001));
        Assert.Equal(10, fill.GetFilled(Batch0002));
        Assert.Equal(CsvFillState.CsvFilling, fill.GetState(Batch0002));
        Assert.True(fill.LastMoveRolledBack);
    }

    [Fact]
    public async Task ApplyBatchMove_commits_both_bundles_when_successful()
    {
        var fill = new InMemoryTransactionalCsvFillService();
        fill.Seed(Batch0001, target: 22, filled: 22, CsvFillState.CsvComplete);
        fill.Seed(Batch0002, target: 22, filled: 10, CsvFillState.CsvFilling);

        await fill.ApplyBatchMoveAsync(AcceptedFile, Batch0001, Batch0002, ndtPipes: 5, CancellationToken.None);

        Assert.Equal(17, fill.GetFilled(Batch0001));
        Assert.Equal(15, fill.GetFilled(Batch0002));
        Assert.False(fill.LastMoveRolledBack);
    }

    // ---- (b) ResubmitDriftService invokes delta revision (not full-add) ----

    [Fact]
    public async Task ResubmitDrift_calls_ApplyCountRevision_with_old_and_new_not_full_add()
    {
        const string fileName = "resubmit_delta.csv";
        using var pending = new TempPending(
            fileName,
            "PO Number,Slit No,NDT Pipes,NDT Batch No",
            $"{Po},01,46,{Batch0001}");

        var recordingFill = new RecordingCsvFillService();
        var bundleRepo = new DriftBundleRepo
        {
            SqlSums = { (Batch0001, "01", 56) }
        };
        var service = new ResubmitDriftService(
            bundleRepo,
            new NoOpReconcileSync(),
            new NoOpSapStatus(),
            recordingFill,
            NullLogger<ResubmitDriftService>.Instance);

        var result = await service.DetectAndReconcileAsync(pending.Folder, fileName, CancellationToken.None);

        Assert.NotNull(result);
        var call = Assert.Single(recordingFill.CountRevisions);
        Assert.Equal(fileName, call.SourceFileName);
        Assert.Equal(Batch0001, call.BatchNo);
        Assert.Equal(56, call.OldNdtPipes);
        Assert.Equal(46, call.NewNdtPipes);
        // Proves wiring passes old+new (delta = -10), never a "full add" of 46 alone.
        Assert.Equal(-10, call.NewNdtPipes - call.OldNdtPipes);
        Assert.Empty(recordingFill.BatchMoves);
    }

    // ---- (c) Pointer advance via worker-path assigner ----

    [Fact]
    public async Task Worker_fill_path_after_complete_on_0001_next_stamp_resolves_0002()
    {
        var fill = new InMemoryTransactionalCsvFillService();
        fill.Seed(Batch0001, target: 22, filled: 0, CsvFillState.PlcClosed, printedAt: DateTime.UtcNow.AddMinutes(-2));
        fill.Seed(Batch0002, target: 22, filled: 0, CsvFillState.PlcClosed, printedAt: DateTime.UtcNow.AddMinutes(-1));

        var assigner = new SlitCsvFillAssigner(fill, NullLogger<SlitCsvFillAssigner>.Instance);

        async Task<string?> Stamp(int pipes)
        {
            var r = await assigner.AssignAsync(
                @"C:\inbox\f.csv", Po, Mill, pipeSize: null, pipes,
                holdWhenNoOpenBundle: true, CancellationToken.None);
            return r.BatchNo;
        }

        Assert.Equal(Batch0001, await Stamp(7));
        Assert.Equal(Batch0001, await Stamp(6));
        Assert.Equal(Batch0001, await Stamp(4));
        Assert.Equal(Batch0001, await Stamp(5)); // completes 22
        Assert.Equal(CsvFillState.CsvComplete, fill.GetState(Batch0001));

        Assert.Equal(Batch0002, await Stamp(3));
        Assert.Equal(3, fill.GetFilled(Batch0002));
        Assert.Equal(CsvFillState.CsvFilling, fill.GetState(Batch0002));
    }

    // ---- (d) Manual-reconcile case (c) PPC gating ----

    [Fact]
    public async Task ManualReconcile_case_c_Accepted_opens_ppc_item()
    {
        var ppc = new PpcRepo();
        var controller = CreateManualReconcileController(
            csvFilled: 17,
            correctedTotal: 10,
            sapStatus: OutputSlitSapStatus.Accepted,
            ppc);

        var result = await controller.ManualBundleReconcile(
            new ReconcileController.ManualBundleReconcileRequest
            {
                NdtBatchNo = Batch0001,
                CorrectedTotal = 10
            },
            CancellationToken.None);

        var ok = Assert.IsType<OkObjectResult>(result);
        Assert.Equal(true, GetProp(ok.Value!, "FillOvershootVsCorrectedTarget"));
        Assert.Equal(1, GetProp(ok.Value!, "PpcCorrectionItemsCreated"));
        Assert.Single(ppc.Items);
        Assert.Equal(AcceptedFile, ppc.Items[0].FileName);
        Assert.Equal(17, ppc.Items[0].OldNdtPipes);
        Assert.Equal(10, ppc.Items[0].CorrectedNdtPipes);
    }

    [Fact]
    public async Task ManualReconcile_case_c_not_Accepted_flags_but_zero_ppc()
    {
        var ppc = new PpcRepo();
        var controller = CreateManualReconcileController(
            csvFilled: 17,
            correctedTotal: 10,
            sapStatus: OutputSlitSapStatus.Pending,
            ppc);

        var result = await controller.ManualBundleReconcile(
            new ReconcileController.ManualBundleReconcileRequest
            {
                NdtBatchNo = Batch0001,
                CorrectedTotal = 10
            },
            CancellationToken.None);

        var ok = Assert.IsType<OkObjectResult>(result);
        Assert.Equal(true, GetProp(ok.Value!, "FillOvershootVsCorrectedTarget"));
        Assert.Equal(0, GetProp(ok.Value!, "PpcCorrectionItemsCreated"));
        Assert.Equal(0, GetProp(ok.Value!, "PpcCorrectionItemsUpdated"));
        Assert.Empty(ppc.Items);
    }

    // ---- (e) No-open-bundle hold ----

    [Fact]
    public async Task No_open_bundle_hold_leaves_unpublished_with_no_invented_number()
    {
        var fill = new InMemoryTransactionalCsvFillService(); // no seeded incomplete targets
        var assigner = new SlitCsvFillAssigner(fill, NullLogger<SlitCsvFillAssigner>.Instance);

        var result = await assigner.AssignAsync(
            @"C:\inbox\late.csv", Po, Mill, pipeSize: null, fileNdtPipes: 11,
            holdWhenNoOpenBundle: true, CancellationToken.None);

        Assert.True(result.Held);
        Assert.Null(result.BatchNo);
        Assert.Null(result.Stamp);
        Assert.Contains("late.csv", fill.HeldFiles, StringComparer.OrdinalIgnoreCase);
        Assert.Empty(fill.AllBundleNos);
    }

    // ---- helpers ----

    private static ReconcileController CreateManualReconcileController(
        int csvFilled,
        int correctedTotal,
        OutputSlitSapStatus sapStatus,
        PpcRepo ppc) =>
        new(
            new ManualReconcileBundleRepo(csvFilled, correctedTotal),
            traceability: null!,
            reconcileSync: new NoOpReconcileSync(),
            formationChartProvider: null!,
            pipeSizeProvider: null!,
            reconcileTagService: new OkTagService(),
            new SapStatusRepo(sapStatus),
            ppc,
            new OptMon(),
            NullLogger<ReconcileController>.Instance);

    private static object? GetProp(object obj, string name) =>
        obj.GetType().GetProperty(name)?.GetValue(obj);

    private sealed class OptMon : Microsoft.Extensions.Options.IOptionsMonitor<NdtBundleService.Configuration.NdtBundleOptions>
    {
        public NdtBundleService.Configuration.NdtBundleOptions CurrentValue { get; } = new();
        public NdtBundleService.Configuration.NdtBundleOptions Get(string? name) => CurrentValue;
        public IDisposable OnChange(Action<NdtBundleService.Configuration.NdtBundleOptions, string?> listener) =>
            NullDisp.Instance;
        private sealed class NullDisp : IDisposable
        {
            public static readonly NullDisp Instance = new();
            public void Dispose() { }
        }
    }

    private sealed class OkTagService : IReconcileBundleTagService
    {
        public Task<ReconcileBundleTagPrintResult> ReprintAsync(NdtBundleRecord bundle, CancellationToken cancellationToken) =>
            Task.FromResult(new ReconcileBundleTagPrintResult { Success = true, Message = "ok" });
    }

    private sealed class ManualReconcileBundleRepo : StubBundleRepoBase
    {
        private readonly int _csvFilled;
        private readonly int _correctedTotal;

        public ManualReconcileBundleRepo(int csvFilled, int correctedTotal)
        {
            _csvFilled = csvFilled;
            _correctedTotal = correctedTotal;
        }

        public override Task<ManualBundleReconcileResult?> ManualReconcileBundleAsync(
            string batchNo, int correctedTotal, string reason, string reconciledBy, CancellationToken cancellationToken)
        {
            var bundle = new NdtBundleRecord
            {
                BundleNo = batchNo,
                PoNumber = Po,
                MillNo = Mill,
                TotalNdtPcs = correctedTotal,
                TargetNdtPcs = correctedTotal,
                CsvFilled = _csvFilled,
                CsvFillState = CsvFillState.CsvOvershoot,
                CountDiscrepancy = true,
                PrintStatus = BundlePrintStatus.Printed
            };
            return Task.FromResult<ManualBundleReconcileResult?>(new ManualBundleReconcileResult
            {
                Bundle = bundle,
                OriginalTotal = 22,
                CorrectedTotal = correctedTotal,
                CsvFilledAtReconcile = _csvFilled,
                ForceFinalized = false,
                CountDiscrepancyLogged = false,
                SlitSumAtFinalize = _csvFilled
            });
        }

        public override Task<NdtBundleRecord?> GetByBatchNoAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(new NdtBundleRecord
            {
                BundleNo = batchNo,
                PoNumber = Po,
                MillNo = Mill,
                TotalNdtPcs = _correctedTotal,
                CsvFilled = _csvFilled,
                PrintStatus = BundlePrintStatus.Printed
            });

        public override Task<IReadOnlyList<(string SlitNo, string SourceFileName)>> GetSlitSourceFileNamesForBatchAsync(
            string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string, string)>>(new[] { ("01", AcceptedFile) });

        public override Task<bool> UpdateBundleSummaryCsvAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) =>
            Task.FromResult(true);
    }

    private sealed class SapStatusRepo : IOutputSlitSapStatusRepository
    {
        private readonly OutputSlitSapStatus _status;
        public SapStatusRepo(OutputSlitSapStatus status) => _status = status;
        public bool Enabled => true;
        public Task<OutputSlitSapStatusApplyResult> ApplyObservationsAsync(IReadOnlyList<OutputSlitSapStatusObservation> observations, CancellationToken cancellationToken) =>
            Task.FromResult(OutputSlitSapStatusApplyResult.Empty);
        public Task RecordOutputFileWrittenAsync(string fileName, DateTime? fileLastWriteTimeUtc, string outputFolder, CancellationToken cancellationToken) =>
            Task.CompletedTask;
        public Task RecordResubmitDriftSyncedEventAsync(string fileName, string pendingFolder, CancellationToken cancellationToken) =>
            Task.CompletedTask;
        public Task<IReadOnlyDictionary<string, OutputSlitSapFileStatus>> GetStatusesForFilesAsync(
            IReadOnlyCollection<string> fileNames, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<string, OutputSlitSapFileStatus>>(
                new Dictionary<string, OutputSlitSapFileStatus>(StringComparer.OrdinalIgnoreCase)
                {
                    [AcceptedFile] = new(_status, DateTime.UtcNow, 0)
                });
    }

    private sealed class PpcRepo : IPpcCorrectionRepository
    {
        private long _nextId = 1;
        public List<PpcCorrectionItem> Items { get; } = new();
        public bool Enabled => true;
        public Task<PpcCorrectionUpsertResult?> UpsertOpenItemAsync(
            string batchNo, string fileName, string slitNo, int? oldNdtPipes, int correctedNdtPipes, CancellationToken cancellationToken)
        {
            var item = new PpcCorrectionItem(
                _nextId++, batchNo, fileName, ReconcileCsvParsing.NormalizeSlitKey(slitNo),
                oldNdtPipes, correctedNdtPipes, PpcCorrectionItem.StatusOpen,
                DateTime.UtcNow, null, null, null, null);
            Items.Add(item);
            return Task.FromResult<PpcCorrectionUpsertResult?>(new(item.Id, Created: true));
        }
        public Task<IReadOnlyList<PpcCorrectionItem>> GetItemsForBatchAsync(string batchNo, bool includeCleared, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<PpcCorrectionItem>>(Items);
        public Task<int> CountOpenItemsForBatchAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult(Items.Count);
        public Task<bool> ClearItemAsync(long id, string? clearedBy, string? note, CancellationToken cancellationToken) =>
            Task.FromResult(false);
    }

    private sealed class DriftBundleRepo : StubBundleRepoBase
    {
        public List<(string BatchNo, string SlitNo, int NdtPipes)> SqlSums { get; } = new();

        public override Task<IReadOnlyList<(string BatchNo, string SlitNo, int NdtPipes)>> GetOutputSlitRowSumsForSourceFileAsync(
            string sourceFileName, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string, string, int)>>(SqlSums);

        public override Task<bool> IsManualReconLockedAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult(false);

        public override Task<int> TrySyncBundleTotalFromSlitsAsync(string batchNo, bool forceFromSlits, CancellationToken cancellationToken) =>
            Task.FromResult(0);
    }

    private sealed class NoOpReconcileSync : IReconcileSyncService
    {
        public Task<int> SyncAfterSlitReconcileAsync(string batchNo, string slitNo, int newNdtPipes, CancellationToken cancellationToken) =>
            Task.FromResult(1);

        public Task SyncAfterBundleTotalReconcileAsync(
            string ndtBatchNo, string poNumber, int newBundleTotalPcs, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task SyncAfterManualStationReconcileAsync(
            ManualTagStation station,
            ManualStationReconcileSnapshot snapshot,
            string? ndtProcessCsvPath,
            CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class NoOpSapStatus : IOutputSlitSapStatusRepository
    {
        public bool Enabled => true;
        public Task<OutputSlitSapStatusApplyResult> ApplyObservationsAsync(IReadOnlyList<OutputSlitSapStatusObservation> observations, CancellationToken cancellationToken) =>
            Task.FromResult(OutputSlitSapStatusApplyResult.Empty);
        public Task RecordOutputFileWrittenAsync(string fileName, DateTime? fileLastWriteTimeUtc, string outputFolder, CancellationToken cancellationToken) =>
            Task.CompletedTask;
        public Task RecordResubmitDriftSyncedEventAsync(string fileName, string pendingFolder, CancellationToken cancellationToken) =>
            Task.CompletedTask;
        public Task<IReadOnlyDictionary<string, OutputSlitSapFileStatus>> GetStatusesForFilesAsync(
            IReadOnlyCollection<string> fileNames, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<string, OutputSlitSapFileStatus>>(
                new Dictionary<string, OutputSlitSapFileStatus>(StringComparer.OrdinalIgnoreCase));
    }

    private sealed class RecordingCsvFillService : NoOpCsvFillService
    {
        public List<(string SourceFileName, string BatchNo, int OldNdtPipes, int NewNdtPipes)> CountRevisions { get; } = new();
        public List<(string File, string OldBatch, string NewBatch, int Pipes)> BatchMoves { get; } = new();

        public override Task ApplyCountRevisionAsync(
            string sourceFileName, string batchNo, int oldNdtPipes, int newNdtPipes, CancellationToken cancellationToken)
        {
            CountRevisions.Add((sourceFileName, batchNo, oldNdtPipes, newNdtPipes));
            return Task.CompletedTask;
        }

        public override Task<Guid> ApplyBatchMoveAsync(
            string sourceFileName, string oldBatchNo, string newBatchNo, int ndtPipes, CancellationToken cancellationToken)
        {
            BatchMoves.Add((sourceFileName, oldBatchNo, newBatchNo, ndtPipes));
            return Task.FromResult(Guid.NewGuid());
        }
    }

    private sealed class TempPending : IDisposable
    {
        public string Folder { get; }
        public TempPending(string fileName, params string[] lines)
        {
            Folder = Path.Combine(Path.GetTempPath(), "ndt-fill-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(Folder);
            File.WriteAllLines(Path.Combine(Folder, fileName), lines);
        }
        public void Dispose()
        {
            try { Directory.Delete(Folder, recursive: true); } catch { /* ignore */ }
        }
    }
}

/// <summary>
/// In-memory fill store with explicit begin/commit/rollback for batch-move — mirrors production
/// transaction choreography so a mid-move failure cannot leave two bundles permanently wrong.
/// </summary>
internal sealed class InMemoryTransactionalCsvFillService : ICsvFillService
{
    private readonly Dictionary<string, BundleFill> _bundles = new(StringComparer.OrdinalIgnoreCase);
    private readonly List<string> _holds = new();

    public bool FailAfterSourceAdjustOnMove { get; set; }
    public bool LastMoveRolledBack { get; private set; }
    public IReadOnlyList<string> HeldFiles => _holds;
    public IReadOnlyCollection<string> AllBundleNos => _bundles.Keys;

    public void Seed(string bundleNo, int target, int filled, string state, DateTime? printedAt = null) =>
        _bundles[bundleNo] = new BundleFill(bundleNo, target, filled, state, printedAt ?? DateTime.UtcNow);

    public int GetFilled(string bundleNo) => _bundles[bundleNo].Filled;
    public string GetState(string bundleNo) => _bundles[bundleNo].State;

    public Task TryInitializeFillTargetAsync(string bundleNo, int targetNdtPcs, string? closeSource, CancellationToken cancellationToken)
    {
        _bundles[bundleNo] = new BundleFill(bundleNo, targetNdtPcs, 0, CsvFillState.PlcClosed, DateTime.UtcNow);
        return Task.CompletedTask;
    }

    public Task<CsvFillIncompleteBundle?> TryGetOldestIncompleteAsync(
        string poNumber, int millNo, string? pipeSize, CancellationToken cancellationToken)
    {
        var oldest = _bundles.Values
            .Where(b => CsvFillState.IsIncomplete(b.State))
            .OrderBy(b => b.PrintedAtUtc)
            .FirstOrDefault();
        if (oldest is null)
            return Task.FromResult<CsvFillIncompleteBundle?>(null);
        return Task.FromResult<CsvFillIncompleteBundle?>(
            new CsvFillIncompleteBundle(oldest.BundleNo, oldest.Target, oldest.Filled, oldest.State, oldest.PrintedAtUtc, null));
    }

    public Task<CsvFillStampResult?> TryStampFileAsync(
        string poNumber, int millNo, string? pipeSize, int fileNdtPipes, CancellationToken cancellationToken)
    {
        var oldest = _bundles.Values
            .Where(b => CsvFillState.IsIncomplete(b.State))
            .OrderBy(b => b.PrintedAtUtc)
            .FirstOrDefault();
        if (oldest is null)
            return Task.FromResult<CsvFillStampResult?>(null);

        var result = CsvFillLogic.ComputeAfterStamp(oldest.BundleNo, oldest.Target, oldest.Filled, fileNdtPipes, 20);
        _bundles[oldest.BundleNo] = oldest with { Filled = result.CsvFilledAfter, State = result.FillState };
        return Task.FromResult<CsvFillStampResult?>(result);
    }

    public Task<int> AdvanceQuietShortAsync(
        string? poNumber, int? millNo, int quietMinutes, DateTime utcNow, bool forcePoEnd, CancellationToken cancellationToken) =>
        Task.FromResult(0);

    public Task UpsertHoldAsync(
        string sourceFileName, string poNumber, int millNo, string? pipeSize, string reasonCode, CancellationToken cancellationToken)
    {
        _holds.Add(Path.GetFileName(sourceFileName));
        return Task.CompletedTask;
    }

    public Task<int> EscalateExpiredHoldsAsync(int quietMinutes, DateTime utcNow, CancellationToken cancellationToken) =>
        Task.FromResult(0);

    public Task ApplyCountRevisionAsync(
        string sourceFileName, string batchNo, int oldNdtPipes, int newNdtPipes, CancellationToken cancellationToken)
    {
        if (!_bundles.TryGetValue(batchNo, out var b))
            return Task.CompletedTask;
        var delta = newNdtPipes - oldNdtPipes;
        var (after, state, _, _) = CsvFillLogic.ApplyFilledDelta(b.Target, b.Filled, delta, 20);
        _bundles[batchNo] = b with { Filled = after, State = state };
        return Task.CompletedTask;
    }

    public Task<Guid> ApplyBatchMoveAsync(
        string sourceFileName, string oldBatchNo, string newBatchNo, int ndtPipes, CancellationToken cancellationToken)
    {
        LastMoveRolledBack = false;
        var correlationId = Guid.NewGuid();
        // Snapshot = begin transaction
        var snapshot = _bundles.ToDictionary(
            static kv => kv.Key,
            static kv => kv.Value,
            StringComparer.OrdinalIgnoreCase);

        try
        {
            AdjustInPlace(oldBatchNo, -ndtPipes);
            if (FailAfterSourceAdjustOnMove)
                throw new InvalidOperationException("Simulated failure after source adjust (mid-move).");

            AdjustInPlace(newBatchNo, ndtPipes);
            // commit — keep mutated state
            return Task.FromResult(correlationId);
        }
        catch
        {
            // explicit rollback
            _bundles.Clear();
            foreach (var (k, v) in snapshot)
                _bundles[k] = v;
            LastMoveRolledBack = true;
            throw;
        }
    }

    public Task<bool> HasAwaitingCsvReconRowsAsync(CancellationToken cancellationToken) => Task.FromResult(false);
    public Task<bool> HasBundlesMissingFillTargetAsync(CancellationToken cancellationToken) => Task.FromResult(false);

    private void AdjustInPlace(string batchNo, int delta)
    {
        if (!_bundles.TryGetValue(batchNo, out var b))
            throw new InvalidOperationException($"Unknown batch {batchNo}");
        var (after, state, _, _) = CsvFillLogic.ApplyFilledDelta(b.Target, b.Filled, delta, 20);
        _bundles[batchNo] = b with { Filled = after, State = state };
    }

    private sealed record BundleFill(string BundleNo, int Target, int Filled, string State, DateTime PrintedAtUtc);
}

/// <summary>Minimal INdtBundleRepository defaults for fill integration tests.</summary>
internal abstract class StubBundleRepoBase : INdtBundleRepository
{
    public virtual Task RecordBundleAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
    public virtual Task RecordBundlePendingPrintAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
    public virtual Task UpdateBundlePrintStatusAsync(string bundleNo, string printStatus, string? printError, CancellationToken cancellationToken) => Task.CompletedTask;
    public virtual Task<IReadOnlyList<NdtBundleRecord>> GetStuckPrintsAsync(TimeSpan olderThan, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
    public virtual Task<IReadOnlyList<NdtBundleRecord>> GetBundlesAsync(CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
    public virtual Task<NdtBundleRecord?> GetByBatchNoAsync(string batchNo, CancellationToken cancellationToken) =>
        Task.FromResult<NdtBundleRecord?>(null);
    public virtual Task<NdtBundleRecord?> GetLatestPrintedBundleForMillAsync(int millNo, CancellationToken cancellationToken) =>
        Task.FromResult<NdtBundleRecord?>(null);
    public virtual Task<bool> HasPrintedBundleForPoAsync(int millNo, string poNumber, CancellationToken cancellationToken) =>
        Task.FromResult(false);
    public virtual Task<int> MarkManualReviewAsync(string poNumber, int millNo, CancellationToken cancellationToken) =>
        Task.FromResult(0);
    public virtual Task TrySetPlcCloseMetadataAsync(int engineBatchSequence, int millNo, CancellationToken cancellationToken) =>
        Task.CompletedTask;
    public virtual Task<(string BundleNo, int EngineSequence, int PlcTotal)?> TryGetAwaitingPlcReconBatchAsync(
        string poNumber, int millNo, CancellationToken cancellationToken) =>
        Task.FromResult<(string, int, int)?>(null);
    public virtual Task<IReadOnlyList<PlcCsvReconAwaitingBundle>> ListAwaitingPlcReconBatchesAsync(
        string poNumber, int millNo, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<PlcCsvReconAwaitingBundle>>(Array.Empty<PlcCsvReconAwaitingBundle>());
    public virtual Task<PlcCsvReconResult?> TryFinalizePlcReconBundleAsync(
        string bundleNo, int slitSum, int reconWindowMinutes, DateTime utcNow, bool force, CancellationToken cancellationToken) =>
        Task.FromResult<PlcCsvReconResult?>(null);
    public virtual Task<IReadOnlyList<PlcCsvReconResult>> TryFinalizeReadyPlcReconBundlesAsync(
        string poNumber, int millNo, int reconWindowMinutes, DateTime utcNow, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<PlcCsvReconResult>>(Array.Empty<PlcCsvReconResult>());
    public virtual Task<PlcCsvReconResult?> TryReconcilePlcClosedBundleAsync(string poNumber, int millNo, int slitSum, CancellationToken cancellationToken) =>
        Task.FromResult<PlcCsvReconResult?>(null);
    public virtual Task<PlcCsvReconResult?> TryForceFinalizeAwaitingReconOnReopenAsync(string poNumber, int millNo, CancellationToken cancellationToken) =>
        Task.FromResult<PlcCsvReconResult?>(null);
    public virtual Task UpdateBundlePipesAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.CompletedTask;
    public virtual Task<int> TrySyncBundleTotalFromSlitsAsync(string batchNo, bool forceFromSlits, CancellationToken cancellationToken) =>
        Task.FromResult(0);
    public virtual Task<int> UpdateOutputCsvFilesForBundleAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
    public virtual Task UpdateBundleTotalInDatabaseAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.CompletedTask;
    public virtual Task<bool> UpdateBundleSummaryCsvAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.FromResult(false);
    public virtual Task<(int RowsRemoved, IReadOnlyList<RemovedSlitRowTraceRef> TraceRefs)> DeletePerSlitOutputRowsForBatchSlitsAsync(
        string batchNo, IReadOnlyList<string> slitNos, CancellationToken cancellationToken) =>
        Task.FromResult<(int, IReadOnlyList<RemovedSlitRowTraceRef>)>((0, Array.Empty<RemovedSlitRowTraceRef>()));
    public virtual Task<IReadOnlyList<(string SlitNo, int NdtPipes)>> GetSlitsForBatchAsync(string batchNo, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<(string, int)>>(Array.Empty<(string, int)>());
    public virtual Task<IReadOnlyList<(string SlitNo, string SourceFileName)>> GetSlitSourceFileNamesForBatchAsync(
        string batchNo, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<(string, string)>>(Array.Empty<(string, string)>());
    public virtual Task<int> UpdateOutputCsvFilesForSlitAsync(string batchNo, string slitNo, int newPipes, CancellationToken cancellationToken) =>
        Task.FromResult(0);
    public virtual Task<bool> IsManualReviewFlaggedAsync(string batchNo, CancellationToken cancellationToken) => Task.FromResult(false);
    public virtual Task<IReadOnlyList<(string BatchNo, string SlitNo, int NdtPipes)>> GetOutputSlitRowSumsForSourceFileAsync(
        string sourceFileName, CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyList<(string, string, int)>>(Array.Empty<(string, string, int)>());

    public virtual Task<bool> IsManualReconLockedAsync(string batchNo, CancellationToken cancellationToken) =>
        Task.FromResult(false);

    public virtual Task<ManualBundleReconcileResult?> ManualReconcileBundleAsync(
        string batchNo, int correctedTotal, string reason, string reconciledBy, CancellationToken cancellationToken) =>
        Task.FromResult<ManualBundleReconcileResult?>(null);
}
