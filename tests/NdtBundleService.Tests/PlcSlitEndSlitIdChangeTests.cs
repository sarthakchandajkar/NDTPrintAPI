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
        var sut = new PlcSlitEndBundleCloser(
            Options.Create(new NdtBundleOptions
            {
                CloseTrigger = "Plc",
                PlcHandshake = new PlcHandshakeOptions { SlitEndTriggerByte = -1 }
            }),
            new FakePlcCloseEngine(closed),
            new NoOpOutputWriter(),
            new FixedActivePo("1000060163"),
            new PipeSizeStub(),
            new FormationStub(10),
            new MillBundleStateLock(),
            new NoOpPlcCloseRepo(),
            NullLogger<PlcSlitEndBundleCloser>.Instance);

        var mill = new MillConfig { Name = "Mill-1", MillNo = 1 };
        var s7 = new AlwaysHealthyNoOpS7();

        await sut.TryCloseOnSlitEndAsync(1, mill, s7, liveNdtCount: 11, liveSlitId: 3, CancellationToken.None);
        Assert.Empty(closed);

        await sut.TryCloseOnSlitEndAsync(1, mill, s7, liveNdtCount: 0, liveSlitId: 4, CancellationToken.None);
        Assert.Single(closed);
        Assert.Equal(11, closed[0].Pcs);
    }

    [Fact]
    public async Task TryCloseOnSlitEnd_skips_when_previous_NDT_below_threshold()
    {
        var closed = new List<(int Batch, int Pcs)>();
        var sut = new PlcSlitEndBundleCloser(
            Options.Create(new NdtBundleOptions
            {
                CloseTrigger = "Plc",
                PlcHandshake = new PlcHandshakeOptions { SlitEndTriggerByte = -1 }
            }),
            new FakePlcCloseEngine(closed),
            new NoOpOutputWriter(),
            new FixedActivePo("1000060163"),
            new PipeSizeStub(),
            new FormationStub(10),
            new MillBundleStateLock(),
            new NoOpPlcCloseRepo(),
            NullLogger<PlcSlitEndBundleCloser>.Instance);

        var mill = new MillConfig { Name = "Mill-1", MillNo = 1 };
        var s7 = new AlwaysHealthyNoOpS7();

        await sut.TryCloseOnSlitEndAsync(1, mill, s7, 5, 1, CancellationToken.None);
        await sut.TryCloseOnSlitEndAsync(1, mill, s7, 0, 2, CancellationToken.None);
        Assert.Empty(closed);
    }

    private static PlcSlitEndBundleCloser CreateCloser() =>
        new(
            Options.Create(new NdtBundleOptions
            {
                CloseTrigger = "Plc",
                PlcHandshake = new PlcHandshakeOptions { SlitEndTriggerByte = -1 }
            }),
            new FakePlcCloseEngine(new List<(int, int)>()),
            new NoOpOutputWriter(),
            new FixedActivePo("1000060163"),
            new PipeSizeStub(),
            new FormationStub(10),
            new MillBundleStateLock(),
            new NoOpPlcCloseRepo(),
            NullLogger<PlcSlitEndBundleCloser>.Instance);

    private sealed class FakePlcCloseEngine : IBundleEngine
    {
        private readonly List<(int Batch, int Pcs)> _closed;
        public FakePlcCloseEngine(List<(int Batch, int Pcs)> closed) => _closed = closed;

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
            CancellationToken cancellationToken)
        {
            var record = new InputSlitRecord { PoNumber = poNumber, MillNo = millNo, NdtPipes = plcCount };
            _closed.Add((1, plcCount));
            await onBundleClosedAsync(record, 1, plcCount).ConfigureAwait(false);
        }
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

    /// <summary>Only <see cref="INdtBundleRepository.TrySetPlcCloseMetadataAsync"/> is used on the close path.</summary>
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
            Task.FromResult<(string, int, int)?>(null);
        public Task<PlcCsvReconResult?> TryReconcilePlcClosedBundleAsync(
            string poNumber, int millNo, int slitSum, CancellationToken cancellationToken) =>
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
