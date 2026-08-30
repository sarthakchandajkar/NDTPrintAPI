using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.PlcHandshake;
using NdtBundleService.Services.PlcHandshake.S7;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// Replay: same physical slits fed via Input Slit CSV and PlcSlitEndBundleCloser must not double-count
/// open-bundle remainder (sizeCounts / MW56).
/// </summary>
public sealed class PlcOpenCsvDoubleCountReplayTests
{
    /// <summary>
    /// Seven slits Ã— 6 pcs (42 physical). CSV + PLC replay must yield 42, not 84.
    /// </summary>
    [Fact]
    public async Task Seven_slits_csv_and_plc_replay_counts_once_not_sum()
    {
        const string po = "1000060288";
        const int mill = 1;
        const int pcsPerSlit = 6;
        const int slitCount = 7;

        var runtime = new ReplayRuntime();
        await runtime.EnsureInitializedAsync(CancellationToken.None);

        var engine = TestEngineFactory.Create(
            new FormationStub(threshold: 80),
            new PipeSizeStub(),
            runtime,
            closeTrigger: "PlcWithFileFallback",
            s7Registry: new FixedRegistry(new AlwaysHealthyS7Provider()));

        var closed = new List<int>();
        for (var slit = 1; slit <= slitCount; slit++)
        {
            await engine.ProcessSlitRecordAsync(
                new InputSlitRecord
                {
                    PoNumber = po,
                    MillNo = mill,
                    SlitNo = slit.ToString(),
                    NdtPipes = pcsPerSlit
                },
                (_, _, total) =>
                {
                    closed.Add(total);
                    return Task.CompletedTask;
                },
                CancellationToken.None);
        }

        Assert.Empty(closed);
        Assert.Equal(0, runtime.GetSizeCounts(po, mill).GetValueOrDefault("Default"));

        var closer = CreateCloser(runtime, closed);
        var millCfg = new MillConfig { Name = "Mill-1", MillNo = mill };
        var s7 = new AlwaysHealthyNoOpS7();

        await closer.TryCloseOnSlitEndAsync(mill, millCfg, s7, liveNdtCount: 0, liveSlitId: 1, CancellationToken.None);

        for (var slit = 1; slit <= slitCount; slit++)
        {
            await closer.TryCloseOnSlitEndAsync(mill, millCfg, s7, liveNdtCount: pcsPerSlit, liveSlitId: slit, CancellationToken.None);
            await closer.TryCloseOnSlitEndAsync(mill, millCfg, s7, liveNdtCount: 0, liveSlitId: slit + 1, CancellationToken.None);
        }

        Assert.Empty(closed);
        var expectedPhysical = slitCount * pcsPerSlit;
        Assert.Equal(expectedPhysical, runtime.GetSizeCounts(po, mill)["Default"]);
        Assert.Equal(0, runtime.GetRunningTotal(po, mill));
    }

    [Fact]
    public void PlcOpenCsvIngestPolicy_traceability_only_when_plc_healthy()
    {
        Assert.True(PlcOpenCsvIngestPolicy.ShouldIngestTraceabilityOnly(
            BundleCloseTrigger.Plc,
            plcPathHealthy: true));
        Assert.True(PlcOpenCsvIngestPolicy.ShouldIngestTraceabilityOnly(
            BundleCloseTrigger.PlcWithFileFallback,
            plcPathHealthy: true));
        Assert.False(PlcOpenCsvIngestPolicy.ShouldIngestTraceabilityOnly(
            BundleCloseTrigger.PlcWithFileFallback,
            plcPathHealthy: false));
        Assert.False(PlcOpenCsvIngestPolicy.ShouldIngestTraceabilityOnly(
            BundleCloseTrigger.File,
            plcPathHealthy: true));
    }

    /// <summary>
    /// Mill-1 2026-07-26 bundle 1226100001: 7 CSV slits (38 pcs) + 5 PLC slit-ends (18 pcs).
    /// Pre-fix hybrid was 56; corrected open remainder is PLC-only 18 at threshold 80.
    /// </summary>
    [Fact]
    public async Task Mill1_20260726_incident_exact_values_plc_only_18_not_hybrid_56()
    {
        const string po = "1000060288";
        const int mill = 1;
        const int threshold = 80;
        int[] csvPcs = [17, 6, 2, 3, 4, 4, 2];
        int[] plcPcs = [6, 4, 2, 2, 4];

        var runtime = new ReplayRuntime();
        await runtime.EnsureInitializedAsync(CancellationToken.None);

        var engine = TestEngineFactory.Create(
            new FormationStub(threshold),
            new PipeSizeStub(),
            runtime,
            closeTrigger: "PlcWithFileFallback",
            s7Registry: new FixedRegistry(new AlwaysHealthyS7Provider()));

        var closed = new List<int>();
        for (var i = 0; i < csvPcs.Length; i++)
        {
            await engine.ProcessSlitRecordAsync(
                new InputSlitRecord
                {
                    PoNumber = po,
                    MillNo = mill,
                    SlitNo = (i + 1).ToString(),
                    NdtPipes = csvPcs[i]
                },
                (_, _, total) =>
                {
                    closed.Add(total);
                    return Task.CompletedTask;
                },
                CancellationToken.None);
        }

        Assert.Empty(closed);
        Assert.Equal(0, runtime.GetSizeCounts(po, mill).GetValueOrDefault("Default"));
        Assert.Equal(0, runtime.GetRunningTotal(po, mill));

        var closer = CreateCloser(runtime, closed, threshold);
        await SimulatePlcSlitEndsAsync(closer, mill, plcPcs, CancellationToken.None);

        Assert.Empty(closed);
        const int plcOnly = 18;
        const int preFixHybrid = 56;
        Assert.Equal(plcOnly, runtime.GetSizeCounts(po, mill)["Default"]);
        Assert.NotEqual(preFixHybrid, runtime.GetSizeCounts(po, mill)["Default"]);
    }

    private static async Task SimulatePlcSlitEndsAsync(
        PlcSlitEndBundleCloser closer,
        int mill,
        IReadOnlyList<int> slitPcs,
        CancellationToken cancellationToken)
    {
        var millCfg = new MillConfig { Name = "Mill-1", MillNo = mill };
        var s7 = new AlwaysHealthyNoOpS7();

        await closer.TryCloseOnSlitEndAsync(mill, millCfg, s7, liveNdtCount: 0, liveSlitId: 1, cancellationToken);

        for (var i = 0; i < slitPcs.Count; i++)
        {
            var slitId = i + 1;
            await closer.TryCloseOnSlitEndAsync(mill, millCfg, s7, liveNdtCount: slitPcs[i], liveSlitId: slitId, cancellationToken);
            await closer.TryCloseOnSlitEndAsync(mill, millCfg, s7, liveNdtCount: 0, liveSlitId: slitId + 1, cancellationToken);
        }
    }

    private static PlcSlitEndBundleCloser CreateCloser(ReplayRuntime runtime, List<int> closed, int threshold = 80) =>
        new(
            Options.Create(new NdtBundleOptions
            {
                CloseTrigger = "PlcWithFileFallback",
                PlcHandshake = new PlcHandshakeOptions { SlitEndTriggerByte = -1 }
            }),
            new FakePlcCloseEngine(closed, runtime),
            new NoOpOutputWriter(),
            new FixedActivePo("1000060288"),
            new PipeSizeStub(),
            new FormationStub(threshold),
            runtime,
            new MillBundleStateLock(),
            new NoOpPlcCloseRepo(),
            NullLogger<PlcSlitEndBundleCloser>.Instance);

    private sealed class FakePlcCloseEngine : IBundleEngine
    {
        private readonly List<int> _closed;
        private readonly ReplayRuntime _runtime;

        public FakePlcCloseEngine(List<int> closed, ReplayRuntime runtime)
        {
            _closed = closed;
            _runtime = runtime;
        }

        public Task ProcessSlitRecordAsync(
            InputSlitRecord record,
            Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
            CancellationToken cancellationToken,
            string? knownPipeSize = null) =>
            throw new NotSupportedException();

        public Task HandlePoEndAsync(
            string poNumber,
            int millNo,
            Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
            CancellationToken cancellationToken,
            Guid? correlationId = null) =>
            throw new NotSupportedException();

        public async Task CloseBundleFromPlcAsync(
            string poNumber,
            int millNo,
            string? pipeSize,
            int plcCount,
            Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
            CancellationToken cancellationToken,
            bool allowPartial = false)
        {
            var sizeKey = "Default";
            var sizeCounts = _runtime.GetSizeCounts(poNumber, millNo);
            sizeCounts[sizeKey] = 0;
            _runtime.SetSizeCounts(poNumber, millNo, sizeCounts);
            _closed.Add(plcCount);
            await onBundleClosedAsync(
                new InputSlitRecord { PoNumber = poNumber, MillNo = millNo, NdtPipes = plcCount },
                1,
                plcCount).ConfigureAwait(false);
        }
    }

    private sealed class ReplayRuntime : INdtBundleRuntimeStateStore
    {
        private readonly Dictionary<string, Dictionary<string, int>> _sizes = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, int> _running = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, InputSlitRecord?> _last = new(StringComparer.OrdinalIgnoreCase);

        private static string Key(string po, int mill) => $"{InputSlitCsvParsing.NormalizePo(po)}|{mill}";

        public Task EnsureInitializedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public int GetBatchOffset(string poNumber, int millNo) => 0;
        public int GetRunningTotal(string poNumber, int millNo) => _running.GetValueOrDefault(Key(poNumber, millNo));
        public void ClearRunningTotal(string poNumber, int millNo) => _running[Key(poNumber, millNo)] = 0;
        public void ClearOpenAccumulation(string poNumber, int millNo) => ClearRunningTotal(poNumber, millNo);
        public DateTime GetLastActivityUtc(string poNumber, int millNo) => DateTime.UtcNow;
        public Task SyncBatchSequencesFromBundlesAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public void ApplySlitContribution(string poNumber, int millNo, int ndtPipes, int threshold, out int totalSoFar)
        {
            var k = Key(poNumber, millNo);
            _running.TryGetValue(k, out var run);
            if (ndtPipes > 0)
                run += ndtPipes;
            _running[k] = run;
            totalSoFar = run;
        }

        public BundleCloseAllocation CloseBundle(string poNumber, int millNo, int closedTotalPcs, int threshold) =>
            new(1, 1);

        public void AdvanceOnPoEnd(string poNumber, int millNo, int threshold) { }
        public int GetEngineBatchNo(string poNumber, int millNo) => 0;
        public void SetEngineBatchNo(string poNumber, int millNo, int batchNo) { }

        public Dictionary<string, int> GetSizeCounts(string poNumber, int millNo)
        {
            var k = Key(poNumber, millNo);
            return _sizes.TryGetValue(k, out var d)
                ? new Dictionary<string, int>(d, StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        }

        public void SetSizeCounts(string poNumber, int millNo, IReadOnlyDictionary<string, int> counts) =>
            _sizes[Key(poNumber, millNo)] = new Dictionary<string, int>(counts, StringComparer.OrdinalIgnoreCase);

        public InputSlitRecord? GetLastRecord(string poNumber, int millNo) =>
            _last.GetValueOrDefault(Key(poNumber, millNo));

        public void SetLastRecord(string poNumber, int millNo, InputSlitRecord? record) =>
            _last[Key(poNumber, millNo)] = record;

        public Task SaveAsync(CancellationToken cancellationToken) => Task.CompletedTask;
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

    private sealed class FixedActivePo : IActivePoPerMillService
    {
        private readonly string _po;
        public FixedActivePo(string po) => _po = po;
        public IReadOnlyList<string> GetInputSlitReadFolderPaths() => Array.Empty<string>();
        public Task<IReadOnlyDictionary<int, string>> GetLatestPoByMillAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<int, string>>(new Dictionary<int, string> { [1] = _po });
    }

    private sealed class NoOpOutputWriter : IBundleOutputWriter
    {
        public Task<int> WriteBundleAsync(InputSlitRecord contextRecord, int ndtBatchNo, int totalNdtPcs, CancellationToken cancellationToken, Guid? correlationId = null) =>
            Task.FromResult(0);
    }

    private sealed class NoOpPlcCloseRepo : INdtBundleRepository
    {
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
        public Task<NdtBundleRecord?> GetLatestPrintedBundleForMillAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(null);
        public Task RecordBundleAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RecordBundlePendingPrintAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task UpdateBundlePrintStatusAsync(string bundleNo, string printStatus, string? printError, CancellationToken cancellationToken) =>
            Task.CompletedTask;
        public Task<IReadOnlyList<NdtBundleRecord>> GetStuckPrintsAsync(TimeSpan olderThan, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
        public Task<IReadOnlyList<NdtBundleRecord>> GetBundlesAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
        public Task<NdtBundleRecord?> GetByBatchNoAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(null);
        public Task UpdateBundlePipesAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<int> UpdateOutputCsvFilesForBundleAsync(string batchNo, int newPipes, CancellationToken cancellationToken) =>
            Task.FromResult(0);
        public Task<IReadOnlyList<(string SlitNo, int NdtPipes)>> GetSlitsForBatchAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string SlitNo, int NdtPipes)>>(Array.Empty<(string, int)>());
        public Task<int> UpdateOutputCsvFilesForSlitAsync(string batchNo, string slitNo, int newPipes, CancellationToken cancellationToken) =>
            Task.FromResult(0);
        public Task UpdateBundleTotalInDatabaseAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) =>
            Task.CompletedTask;
        public Task<bool> UpdateBundleSummaryCsvAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) =>
            Task.FromResult(false);
        public Task<int> TrySyncBundleTotalFromSlitsAsync(string batchNo, bool forceFromSlits, CancellationToken cancellationToken) =>
            Task.FromResult(0);
        public Task<(int RowsRemoved, IReadOnlyList<RemovedSlitRowTraceRef> TraceRefs)> DeletePerSlitOutputRowsForBatchSlitsAsync(
            string batchNo,
            IReadOnlyList<string> slitNos,
            CancellationToken cancellationToken) =>
            Task.FromResult((0, (IReadOnlyList<RemovedSlitRowTraceRef>)Array.Empty<RemovedSlitRowTraceRef>()));
    }

    private sealed class FixedRegistry : IS7ConnectionProviderRegistry
    {
        private readonly IS7ConnectionProvider _provider;
        public FixedRegistry(IS7ConnectionProvider provider) => _provider = provider;
        public IS7ConnectionProvider GetOrCreate(MillConfig mill, PlcHandshakeOptions options) => _provider;
        public IS7ConnectionProvider? TryGet(int millNo) => millNo == 1 ? _provider : null;
    }

    private sealed class AlwaysHealthyS7Provider : IS7ConnectionProvider
    {
        public int MillNo => 1;
        public string MillName => "Mill-1";
        public bool IsConnected => true;
        public bool IsHealthy => true;
#pragma warning disable CS0067
        public event Action<bool>? HealthChanged;
#pragma warning restore CS0067
        public Task<bool> EnsureConnectedAsync(CancellationToken cancellationToken) => Task.FromResult(true);
        public void Disconnect() { }
        public T Read<T>(Func<IS7PlcOperations, T> operation) => throw new NotSupportedException();
        public void Write(Action<IS7PlcOperations> operation) => throw new NotSupportedException();
        public Task<T> ReadAsync<T>(Func<IS7PlcOperations, T> operation, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
        public Task WriteAsync(Action<IS7PlcOperations> operation, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
        public int TakeReconnectDelayMs() => 1000;
        public void ResetReconnectBackoff() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class AlwaysHealthyNoOpS7 : IS7ConnectionProvider
    {
        public int MillNo => 1;
        public string MillName => "Mill-1";
        public bool IsConnected => true;
        public bool IsHealthy => true;
#pragma warning disable CS0067
        public event Action<bool>? HealthChanged;
#pragma warning restore CS0067
        public Task<bool> EnsureConnectedAsync(CancellationToken cancellationToken) => Task.FromResult(true);
        public void Disconnect() { }
        public T Read<T>(Func<IS7PlcOperations, T> operation) => default!;
        public void Write(Action<IS7PlcOperations> operation) { }
        public Task<T> ReadAsync<T>(Func<IS7PlcOperations, T> operation, CancellationToken cancellationToken = default) =>
            Task.FromResult(default(T)!);
        public Task WriteAsync(Action<IS7PlcOperations> operation, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
        public int TakeReconnectDelayMs() => 1000;
        public void ResetReconnectBackoff() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
