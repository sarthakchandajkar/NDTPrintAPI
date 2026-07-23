using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.PlcHandshake;
using NdtBundleService.Services.PlcHandshake.S7;
using S7.Net;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// Slit end defaults to Mills PLC Slit ID change (DB251.DBW10), not NDT count reset.
/// Sub-threshold slits accumulate into sizeCounts; close when remainder ≥ threshold.
/// </summary>
public sealed class PlcSlitEndSlitIdChangeTests
{
    [Fact]
    public void SlitId_first_poll_primes_without_detecting()
    {
        var sut = CreateCloser();
        var s7 = new AlwaysHealthyNoOpS7();
        var handshake = new PlcHandshakeOptions { SlitEndTriggerByte = -1 };

        Assert.False(sut.TryDetectSlitEnd(1, handshake, s7, liveNdtCount: 11, liveSlitId: 3, out _, out _));
    }

    [Fact]
    public void Same_SlitId_does_not_detect_even_when_NDT_drops()
    {
        var sut = CreateCloser();
        var s7 = new AlwaysHealthyNoOpS7();
        var handshake = new PlcHandshakeOptions { SlitEndTriggerByte = -1 };

        Assert.False(sut.TryDetectSlitEnd(1, handshake, s7, 11, 3, out _, out _));
        Assert.False(sut.TryDetectSlitEnd(1, handshake, s7, 0, 3, out _, out _));
    }

    [Fact]
    public void SlitId_change_detects_and_uses_previous_NDT_count()
    {
        var sut = CreateCloser();
        var s7 = new AlwaysHealthyNoOpS7();
        var handshake = new PlcHandshakeOptions { SlitEndTriggerByte = -1 };

        Assert.False(sut.TryDetectSlitEnd(1, handshake, s7, 11, 3, out _, out _));

        Assert.True(sut.TryDetectSlitEnd(1, handshake, s7, 0, 4, out var reason, out var plcCount));
        Assert.Equal(11, plcCount);
        Assert.Contains("Slit ID change (3→4)", reason);
    }

    [Fact]
    public void SlitId_change_updates_baseline_for_next_slit()
    {
        var sut = CreateCloser();
        var s7 = new AlwaysHealthyNoOpS7();
        var handshake = new PlcHandshakeOptions { SlitEndTriggerByte = -1 };

        Assert.False(sut.TryDetectSlitEnd(1, handshake, s7, 11, 3, out _, out _));
        Assert.True(sut.TryDetectSlitEnd(1, handshake, s7, 2, 4, out _, out var firstCloseCount));
        Assert.Equal(11, firstCloseCount);

        Assert.False(sut.TryDetectSlitEnd(1, handshake, s7, 10, 4, out _, out _));

        Assert.True(sut.TryDetectSlitEnd(1, handshake, s7, 1, 5, out _, out var secondCloseCount));
        Assert.Equal(10, secondCloseCount);
    }

    [Fact]
    public async Task TryCloseOnSlitEnd_closes_when_SlitId_changes_and_count_meets_threshold()
    {
        var closed = new List<(int Batch, int Pcs)>();
        var runtime = new MiniRuntime();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        var sut = CreateCloserWithRuntime(closed, runtime);

        var mill = new MillConfig { Name = "Mill-1", MillNo = 1 };
        var s7 = new AlwaysHealthyNoOpS7();

        await sut.TryCloseOnSlitEndAsync(1, mill, s7, liveNdtCount: 11, liveSlitId: 3, CancellationToken.None);
        Assert.Empty(closed);

        await sut.TryCloseOnSlitEndAsync(1, mill, s7, liveNdtCount: 0, liveSlitId: 4, CancellationToken.None);
        Assert.Single(closed);
        Assert.Equal(11, closed[0].Pcs);
        Assert.Equal(0, runtime.GetSizeCounts("1000060163", 1).GetValueOrDefault("Default"));
    }

    [Fact]
    public async Task TryCloseOnSlitEnd_accumulates_sub_threshold_then_closes_on_overshoot()
    {
        var closed = new List<(int Batch, int Pcs)>();
        var runtime = new MiniRuntime();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        var sut = CreateCloserWithRuntime(closed, runtime);

        var mill = new MillConfig { Name = "Mill-1", MillNo = 1 };
        var s7 = new AlwaysHealthyNoOpS7();

        // Slit 1: 9 pcs (< 10) → accumulate only
        await sut.TryCloseOnSlitEndAsync(1, mill, s7, 9, 1, CancellationToken.None);
        await sut.TryCloseOnSlitEndAsync(1, mill, s7, 0, 2, CancellationToken.None);
        Assert.Empty(closed);
        Assert.Equal(9, runtime.GetSizeCounts("1000060163", 1)["Default"]);

        // Slit 2: 3 pcs → 9+3=12 ≥ 10 → close
        await sut.TryCloseOnSlitEndAsync(1, mill, s7, 3, 2, CancellationToken.None);
        await sut.TryCloseOnSlitEndAsync(1, mill, s7, 0, 3, CancellationToken.None);
        Assert.Single(closed);
        Assert.Equal(12, closed[0].Pcs);
        Assert.Equal(0, runtime.GetSizeCounts("1000060163", 1).GetValueOrDefault("Default"));
    }

    [Fact]
    public async Task TryCloseOnSlitEnd_sub_threshold_does_not_close()
    {
        var closed = new List<(int Batch, int Pcs)>();
        var runtime = new MiniRuntime();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        var sut = CreateCloserWithRuntime(closed, runtime);

        var mill = new MillConfig { Name = "Mill-1", MillNo = 1 };
        var s7 = new AlwaysHealthyNoOpS7();

        await sut.TryCloseOnSlitEndAsync(1, mill, s7, 5, 1, CancellationToken.None);
        await sut.TryCloseOnSlitEndAsync(1, mill, s7, 0, 2, CancellationToken.None);
        Assert.Empty(closed);
        Assert.Equal(5, runtime.GetSizeCounts("1000060163", 1)["Default"]);
    }

    private static PlcSlitEndBundleCloser CreateCloser() =>
        CreateCloserWithRuntime(new List<(int, int)>(), new MiniRuntime());

    private static PlcSlitEndBundleCloser CreateCloserWithRuntime(
        List<(int Batch, int Pcs)> closed,
        INdtBundleRuntimeStateStore runtime) =>
        new(
            Options.Create(new NdtBundleOptions
            {
                CloseTrigger = "Plc",
                PlcHandshake = new PlcHandshakeOptions { SlitEndTriggerByte = -1 }
            }),
            new FakePlcCloseEngine(closed, runtime),
            new NoOpOutputWriter(),
            new FixedActivePo("1000060163"),
            new PipeSizeStub(),
            new FormationStub(10),
            runtime,
            new MillBundleStateLock(),
            new NoOpPlcCloseRepo(),
            NullLogger<PlcSlitEndBundleCloser>.Instance);

    private sealed class FakePlcCloseEngine : IBundleEngine
    {
        private readonly List<(int Batch, int Pcs)> _closed;
        private readonly INdtBundleRuntimeStateStore _runtime;

        public FakePlcCloseEngine(List<(int Batch, int Pcs)> closed, INdtBundleRuntimeStateStore runtime)
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

            var record = new InputSlitRecord { PoNumber = poNumber, MillNo = millNo, NdtPipes = plcCount };
            _closed.Add((1, plcCount));
            await onBundleClosedAsync(record, 1, plcCount).ConfigureAwait(false);
        }
    }

    private sealed class MiniRuntime : INdtBundleRuntimeStateStore
    {
        private readonly Dictionary<string, Dictionary<string, int>> _sizes = new(StringComparer.OrdinalIgnoreCase);
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

        public void SetSizeCounts(string poNumber, int millNo, IReadOnlyDictionary<string, int> counts)
        {
            _sizes[Key(poNumber, millNo)] = new Dictionary<string, int>(counts, StringComparer.OrdinalIgnoreCase);
        }

        public int GetRunningTotal(string poNumber, int millNo) => 0;
        public void ClearRunningTotal(string poNumber, int millNo) { }
        public void ClearOpenAccumulation(string poNumber, int millNo) => ClearRunningTotal(poNumber, millNo);
        public DateTime GetLastActivityUtc(string poNumber, int millNo) => DateTime.UtcNow;
        public void AdvanceOnPoEnd(string poNumber, int millNo, int threshold) { }
        public BundleCloseAllocation CloseBundle(string poNumber, int millNo, int closedTotalPcs, int threshold) =>
            new(1, 0);
        public void ApplySlitContribution(string poNumber, int millNo, int ndtPipes, int threshold, out int batchNumberForRow, out int totalSoFar)
        {
            batchNumberForRow = 1;
            totalSoFar = ndtPipes;
        }
        public int GetEngineBatchNo(string poNumber, int millNo) => 0;
        public void SetEngineBatchNo(string poNumber, int millNo, int batchNo) { }
        public int GetBatchOffset(string poNumber, int millNo) => 0;
        public InputSlitRecord? GetLastRecord(string poNumber, int millNo) => null;
        public void SetLastRecord(string poNumber, int millNo, InputSlitRecord? record) { }
    }

    private sealed class FixedActivePo : IActivePoPerMillService
    {
        private readonly string _po;
        public FixedActivePo(string po) => _po = po;
        public Task<IReadOnlyDictionary<int, string>> GetLatestPoByMillAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<int, string>>(new Dictionary<int, string> { [1] = _po });
        public IReadOnlyList<string> GetInputSlitReadFolderPaths() => Array.Empty<string>();
    }

    private sealed class NoOpOutputWriter : IBundleOutputWriter
    {
        public Task WriteBundleAsync(
            InputSlitRecord contextRecord,
            int ndtBatchNo,
            int totalNdtPcs,
            CancellationToken cancellationToken,
            Guid? correlationId = null) =>
            Task.CompletedTask;
    }

    private sealed class NoOpPlcCloseRepo : INdtBundleRepository
    {
        public Task TrySetPlcCloseMetadataAsync(int engineBatchSequence, int millNo, CancellationToken cancellationToken) =>
            Task.CompletedTask;
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
        public T Read<T>(Func<IS7PlcOperations, T> operation) => operation(new NoOpOps());
        public void Write(Action<IS7PlcOperations> operation) { }
        public Task<T> ReadAsync<T>(Func<IS7PlcOperations, T> operation, CancellationToken cancellationToken = default) =>
            Task.FromResult(Read(operation));
        public Task WriteAsync(Action<IS7PlcOperations> operation, CancellationToken cancellationToken = default) =>
            Task.CompletedTask;
        public int TakeReconnectDelayMs() => 1000;
        public void ResetReconnectBackoff() { }
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private sealed class NoOpOps : IS7PlcOperations
        {
            public object? Read(DataType dataType, int db, int startByteAdr, VarType varType, int varCount, byte bitAdr = 0) =>
                false;
            public void Write(DataType dataType, int db, int startByteAdr, object value, int bitAdr = -1) { }
        }
    }
}
