using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.PlcHandshake;
using NdtBundleService.Services.PoLifecycle;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// Immediate PLC PO-end prints live remainder (partial / exact / overshoot) without waiting for Input Slit.
/// </summary>
public sealed class PoEndImmediateRemainderTests
{
    [Theory]
    [InlineData(4)]
    [InlineData(10)]
    [InlineData(15)]
    public async Task Immediate_prints_live_remainder_without_csv(int remainder)
    {
        var opts = CreateImmediateOpts();
        var runtime = new InMemoryRuntime();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        runtime.SetSizeCounts("1000060163", 1, new Dictionary<string, int> { ["Default"] = remainder });

        var closed = new List<(int Batch, int Pcs)>();
        var metadata = new List<int>();
        var engine = TestEngineFactory.Create(new FormationStub(10), new PipeSizeStub(), runtime);
        var wip = new RecordingWip();
        var workflow = CreateWorkflow(engine, closed, metadata, runtime, wip, opts);

        var result = await workflow.ExecuteAsync(
            "1000060163",
            1,
            advancePoPlanFile: false,
            CancellationToken.None,
            correlationId: Guid.NewGuid(),
            plcNdtCountFinal: 0);

        Assert.False(result.FlushDeferred);
        Assert.Equal(1, result.BundlesClosed);
        Assert.Equal(remainder, result.TotalNdtPcsClosed);
        Assert.Single(closed);
        Assert.Equal(remainder, closed[0].Pcs);
        Assert.Single(metadata);
        Assert.True(wip.Waiting);
        Assert.Equal(0, runtime.GetSizeCounts("1000060163", 1).GetValueOrDefault("Default"));
    }

    [Fact]
    public async Task Immediate_falls_back_to_plc_ndt_when_sizeCounts_empty()
    {
        var opts = CreateImmediateOpts();
        var runtime = new InMemoryRuntime();
        await runtime.EnsureInitializedAsync(CancellationToken.None);

        var closed = new List<(int Batch, int Pcs)>();
        var metadata = new List<int>();
        var engine = TestEngineFactory.Create(new FormationStub(10), new PipeSizeStub(), runtime);
        var workflow = CreateWorkflow(engine, closed, metadata, runtime, new RecordingWip(), opts);

        var result = await workflow.ExecuteAsync(
            "1000060163",
            1,
            false,
            CancellationToken.None,
            Guid.NewGuid(),
            plcNdtCountFinal: 7);

        Assert.Equal(7, result.TotalNdtPcsClosed);
        Assert.Single(closed);
        Assert.Equal(7, closed[0].Pcs);
    }

    [Fact]
    public void RemainderResolver_prefers_sizeCounts_over_plc()
    {
        var runtime = new InMemoryRuntime();
        runtime.SetSizeCounts("1000060163", 1, new Dictionary<string, int> { ["Default"] = 4 });
        var n = PoEndRemainderResolver.Resolve(
            "1000060163",
            1,
            pipeSize: null,
            runtime,
            handshakeStatus: null,
            plcNdtCountFinal: 99);
        Assert.Equal(4, n);
    }

    private static NdtBundleOptions CreateImmediateOpts() =>
        new()
        {
            PoEndFlushMode = "Immediate",
            WaitForWipBundleAfterPoEnd = true,
            PlcHandshake = new PlcHandshakeOptions
            {
                Enabled = true,
                Mills = [new MillConfig { MillNo = 1, Name = "Mill-1", PoEndSource = "Plc" }]
            }
        };

    private static PoEndWorkflowService CreateWorkflow(
        IBundleEngine engine,
        List<(int Batch, int Pcs)> closed,
        List<int> metadataBatches,
        INdtBundleRuntimeStateStore runtime,
        IWipBundleRunningPoProvider wip,
        NdtBundleOptions opts)
    {
        return new PoEndWorkflowService(
            engine,
            new CapturingWriter(closed),
            new NdtBatchStateService(new FormationStub(10), new PipeSizeStub(), runtime),
            runtime,
            new NoopLiveNdt(),
            wip,
            new MillBundleStateLock(),
            new PoLifecycleService(new TestOptionsMonitor<NdtBundleOptions>(opts)),
            new PipeSizeStub(),
            new CapturingRepo(metadataBatches),
            new PlcHandshakeStatusRegistry(),
            new TestOptionsMonitor<NdtBundleOptions>(opts),
            NullLogger<PoEndWorkflowService>.Instance);
    }

    private sealed class TestOptionsMonitor<T> : IOptionsMonitor<T>
    {
        public TestOptionsMonitor(T value) => CurrentValue = value;
        public T CurrentValue { get; }
        public T Get(string? name) => CurrentValue;
        public IDisposable OnChange(Action<T, string?> listener) => NullDisposable.Instance;
    }

    private sealed class NullDisposable : IDisposable
    {
        public static readonly NullDisposable Instance = new();
        public void Dispose() { }
    }

    private sealed class CapturingWriter : IBundleOutputWriter
    {
        private readonly List<(int Batch, int Pcs)> _closed;
        public CapturingWriter(List<(int Batch, int Pcs)> closed) => _closed = closed;
        public Task WriteBundleAsync(InputSlitRecord contextRecord, int ndtBatchNo, int totalNdtPcs, CancellationToken cancellationToken, Guid? correlationId = null)
        {
            _closed.Add((ndtBatchNo, totalNdtPcs));
            return Task.CompletedTask;
        }
    }

    private sealed class CapturingRepo : INdtBundleRepository
    {
        private readonly List<int> _batches;
        public CapturingRepo(List<int> batches) => _batches = batches;
        public Task TrySetPlcCloseMetadataAsync(int engineBatchSequence, int millNo, CancellationToken cancellationToken)
        {
            _batches.Add(engineBatchSequence);
            return Task.CompletedTask;
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
        public Task<IReadOnlyList<(string SlitNo, int NdtPipes)>> GetSlitsForBatchAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string SlitNo, int NdtPipes)>>(Array.Empty<(string, int)>());
        public Task<int> UpdateOutputCsvFilesForSlitAsync(string batchNo, string slitNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task UpdateBundleTotalInDatabaseAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<bool> UpdateBundleSummaryCsvAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.FromResult(false);
        public Task<int> TrySyncBundleTotalFromSlitsAsync(string batchNo, bool forceFromSlits, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<(int RowsRemoved, IReadOnlyList<RemovedSlitRowTraceRef> TraceRefs)> DeletePerSlitOutputRowsForBatchSlitsAsync(
            string batchNo, IReadOnlyList<string> slitNos, CancellationToken cancellationToken) =>
            Task.FromResult((0, (IReadOnlyList<RemovedSlitRowTraceRef>)Array.Empty<RemovedSlitRowTraceRef>()));
        public Task<NdtBundleRecord?> GetLatestPrintedBundleForMillAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(null);
        public Task<bool> HasPrintedBundleForPoAsync(int millNo, string poNumber, CancellationToken cancellationToken) => Task.FromResult(false);
        public Task<int> MarkManualReviewAsync(string poNumber, int millNo, CancellationToken cancellationToken) => Task.FromResult(0);
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
        public Task<PlcCsvReconResult?> TryReconcilePlcClosedBundleAsync(
            string poNumber, int millNo, int slitSum, CancellationToken cancellationToken) =>
            Task.FromResult<PlcCsvReconResult?>(null);
        public Task<PlcCsvReconResult?> TryForceFinalizeAwaitingReconOnReopenAsync(
            string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<PlcCsvReconResult?>(null);
    }

    private sealed class RecordingWip : IWipBundleRunningPoProvider
    {
        public bool Waiting { get; private set; }
        public Task<string?> TryGetRunningPoForMillAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<string?>("1000060163");
        public void NotifyPoEndForMill(int millNo, string endedPo) => Waiting = true;
        public bool IsWaitingForNewWipAfterPoEnd(int millNo) => Waiting;
        public bool TryGetPoEndWaitContext(int millNo, out bool waitingForNewWip, out string? endedPo)
        {
            waitingForNewWip = Waiting;
            endedPo = null;
            return true;
        }
        public bool ResumeRunningWipForMill(int millNo) => false;
        public bool TrySetRunningPoFromWipFile(int millNo, string newPo, DateTime wipStampUtc, string wipFileName) => false;
    }

    private sealed class NoopLiveNdt : IMillSlitLiveNdtAccumulator
    {
        public IReadOnlyList<int>? TryConsumeRawForBundleIncrements(string normalizedPoNumber, int millNo, int plcRawNdt) => null;
        public void OnPoEndForMill(string normalizedPoNumber, int millNo) { }
    }

    private sealed class FormationStub : IFormationChartProvider
    {
        private readonly int _threshold;
        public FormationStub(int threshold) => _threshold = threshold;
        public Task<IReadOnlyDictionary<string, FormationChartEntry>> GetFormationChartAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<string, FormationChartEntry>>(
                new Dictionary<string, FormationChartEntry>
                {
                    ["Default"] = new FormationChartEntry { PipeSize = "Default", RequiredNdtPcs = _threshold }
                });
        public void InvalidateCache() { }
    }

    private sealed class PipeSizeStub : IPipeSizeProvider
    {
        public Task<IReadOnlyDictionary<string, string>> GetPipeSizeByPoAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<string, string>>(new Dictionary<string, string>());
        public IReadOnlyDictionary<string, string>? TryGetCachedPipeSizes() => null;
        public Task<string?> TryGetPipeSizeForPoAsync(string poNumber, CancellationToken cancellationToken) =>
            Task.FromResult<string?>(null);
    }

    private sealed class InMemoryRuntime : INdtBundleRuntimeStateStore
    {
        private readonly Dictionary<string, Dictionary<string, int>> _sizes = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, int> _engine = new(StringComparer.OrdinalIgnoreCase);
        private static string Key(string po, int mill) => $"{InputSlitCsvParsing.NormalizePo(po)}|{mill}";

        public Task EnsureInitializedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task SaveAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task SyncBatchSequencesFromBundlesAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Dictionary<string, int> GetSizeCounts(string poNumber, int millNo)
        {
            var k = Key(poNumber, millNo);
            if (!_sizes.TryGetValue(k, out var d))
            {
                d = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                _sizes[k] = d;
            }
            return new Dictionary<string, int>(d, StringComparer.OrdinalIgnoreCase);
        }
        public void SetSizeCounts(string poNumber, int millNo, IReadOnlyDictionary<string, int> counts) =>
            _sizes[Key(poNumber, millNo)] = new Dictionary<string, int>(counts, StringComparer.OrdinalIgnoreCase);
        public int GetRunningTotal(string poNumber, int millNo) => 0;
        public void ClearRunningTotal(string poNumber, int millNo) { }
        public void ClearOpenAccumulation(string poNumber, int millNo) => ClearRunningTotal(poNumber, millNo);
        public DateTime GetLastActivityUtc(string poNumber, int millNo) => DateTime.UtcNow;
        public void AdvanceOnPoEnd(string poNumber, int millNo, int threshold) { }
        public BundleCloseAllocation CloseBundle(string poNumber, int millNo, int closedTotalPcs, int threshold)
        {
            var k = Key(poNumber, millNo);
            _engine.TryGetValue(k, out var prev);
            var next = prev + 1;
            _engine[k] = next;
            return new BundleCloseAllocation(next, 0);
        }
        public void ApplySlitContribution(string poNumber, int millNo, int ndtPipes, int threshold, out int batchNumberForRow, out int totalSoFar)
        {
            batchNumberForRow = 1;
            totalSoFar = ndtPipes;
        }
        public int GetEngineBatchNo(string poNumber, int millNo) =>
            _engine.TryGetValue(Key(poNumber, millNo), out var n) ? n : 0;
        public void SetEngineBatchNo(string poNumber, int millNo, int batchNo) =>
            _engine[Key(poNumber, millNo)] = batchNo;
        public int GetBatchOffset(string poNumber, int millNo) => GetEngineBatchNo(poNumber, millNo);
        public InputSlitRecord? GetLastRecord(string poNumber, int millNo) => null;
        public void SetLastRecord(string poNumber, int millNo, InputSlitRecord? record) { }
    }
}
