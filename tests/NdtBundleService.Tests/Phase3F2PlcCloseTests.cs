using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.PlcHandshake.S7;
using S7.Net;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class Phase3F2PlcCloseTests
{
    [Fact]
    public void File_trigger_always_allows_file_close()
    {
        Assert.True(BundleClosePolicy.AllowFileThresholdClose(BundleCloseTrigger.File, plcPathHealthy: true));
        Assert.True(BundleClosePolicy.AllowFileThresholdClose(BundleCloseTrigger.File, plcPathHealthy: false));
        Assert.False(BundleClosePolicy.AllowPlcClose(BundleCloseTrigger.File, plcPathHealthy: true));
    }

    [Fact]
    public void PlcWithFileFallback_uses_file_when_unhealthy_and_plc_when_healthy()
    {
        Assert.True(BundleClosePolicy.AllowFileThresholdClose(BundleCloseTrigger.PlcWithFileFallback, plcPathHealthy: false));
        Assert.False(BundleClosePolicy.AllowFileThresholdClose(BundleCloseTrigger.PlcWithFileFallback, plcPathHealthy: true));
        Assert.True(BundleClosePolicy.AllowPlcClose(BundleCloseTrigger.PlcWithFileFallback, plcPathHealthy: true));
        Assert.False(BundleClosePolicy.AllowPlcClose(BundleCloseTrigger.PlcWithFileFallback, plcPathHealthy: false));
    }

    [Fact]
    public async Task CloseBundleFromPlc_closes_and_zeros_size_count()
    {
        var runtime = new MiniRuntime();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        runtime.SetSizeCounts("1000060163", 1, new Dictionary<string, int> { ["Default"] = 7 });

        var engine = TestEngineFactory.Create(
            new FormationStub(10),
            new PipeSizeStub(),
            runtime,
            closeTrigger: "Plc");

        var closed = new List<(int Batch, int Pcs)>();
        await engine.CloseBundleFromPlcAsync(
            "1000060163",
            1,
            pipeSize: null,
            plcCount: 11,
            (record, batchNo, total) =>
            {
                closed.Add((batchNo, total));
                return Task.CompletedTask;
            },
            CancellationToken.None);

        Assert.Single(closed);
        Assert.Equal(11, closed[0].Pcs);
        Assert.Equal(0, runtime.GetSizeCounts("1000060163", 1)["Default"]);
    }

    [Fact]
    public async Task ProcessSlitRecord_file_close_identical_when_CloseTrigger_File()
    {
        var runtime = new MiniRuntime();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        var engine = TestEngineFactory.Create(
            new FormationStub(10),
            new PipeSizeStub(),
            runtime,
            closeTrigger: "File");

        var closed = new List<int>();
        var record = new InputSlitRecord
        {
            PoNumber = "1000060163",
            MillNo = 1,
            SlitNo = "1",
            NdtPipes = 11
        };

        await engine.ProcessSlitRecordAsync(
            record,
            (_, batchNo, total) =>
            {
                closed.Add(total);
                return Task.CompletedTask;
            },
            CancellationToken.None);

        Assert.Single(closed);
        Assert.Equal(11, closed[0]);
    }

    [Fact]
    public async Task ProcessSlitRecord_skips_file_close_when_PlcWithFileFallback_and_healthy()
    {
        var runtime = new MiniRuntime();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        var healthy = new AlwaysHealthyS7Provider();
        var engine = TestEngineFactory.Create(
            new FormationStub(10),
            new PipeSizeStub(),
            runtime,
            closeTrigger: "PlcWithFileFallback",
            s7Registry: new FixedRegistry(healthy));

        var closed = new List<int>();
        var record = new InputSlitRecord
        {
            PoNumber = "1000060163",
            MillNo = 1,
            SlitNo = "1",
            NdtPipes = 11
        };

        await engine.ProcessSlitRecordAsync(
            record,
            (_, batchNo, total) =>
            {
                closed.Add(total);
                return Task.CompletedTask;
            },
            CancellationToken.None);

        Assert.Empty(closed);
        Assert.Equal(11, runtime.GetSizeCounts("1000060163", 1)["Default"]);
    }

    [Fact]
    public void PlcCsvReconResult_flags_discrepancy_when_sums_differ()
    {
        var match = new PlcCsvReconResult { BundleNo = "1226100001", PlcTotal = 11, SlitSum = 11 };
        Assert.False(match.CountDiscrepancy);
        var mismatch = new PlcCsvReconResult { BundleNo = "1226100001", PlcTotal = 11, SlitSum = 10 };
        Assert.True(mismatch.CountDiscrepancy);
    }

    [Fact]
    public async Task ProcessSlitRecord_file_close_when_PlcWithFileFallback_and_unhealthy()
    {
        var runtime = new MiniRuntime();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        var engine = TestEngineFactory.Create(
            new FormationStub(10),
            new PipeSizeStub(),
            runtime,
            closeTrigger: "PlcWithFileFallback",
            s7Registry: new EmptyS7Registry());

        var closed = new List<int>();
        await engine.ProcessSlitRecordAsync(
            new InputSlitRecord { PoNumber = "1000060163", MillNo = 1, SlitNo = "1", NdtPipes = 11 },
            (_, _, total) =>
            {
                closed.Add(total);
                return Task.CompletedTask;
            },
            CancellationToken.None);

        Assert.Single(closed);
        Assert.Equal(11, closed[0]);
    }

    private sealed class EmptyS7Registry : IS7ConnectionProviderRegistry
    {
        public IS7ConnectionProvider GetOrCreate(MillConfig mill, PlcHandshakeOptions options) =>
            throw new NotSupportedException();
        public IS7ConnectionProvider? TryGet(int millNo) => null;
    }

    private sealed class MiniRuntime : INdtBundleRuntimeStateStore
    {
        private readonly Dictionary<string, Dictionary<string, int>> _sizes = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, int> _engine = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, InputSlitRecord?> _last = new(StringComparer.OrdinalIgnoreCase);

        private static string Key(string po, int mill) => $"{InputSlitCsvParsing.NormalizePo(po)}|{mill}";

        public Task EnsureInitializedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public int GetBatchOffset(string poNumber, int millNo) => 0;
        public int GetRunningTotal(string poNumber, int millNo) => 0;
        public void ClearRunningTotal(string poNumber, int millNo) { }
        public Task SyncBatchSequencesFromBundlesAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public void ApplySlitContribution(string poNumber, int millNo, int ndtPipes, int threshold, out int batchNumberForRow, out int totalSoFar)
        {
            batchNumberForRow = 1;
            totalSoFar = ndtPipes;
        }
        public BundleCloseAllocation CloseBundle(string poNumber, int millNo, int closedTotalPcs, int threshold)
        {
            var k = Key(poNumber, millNo);
            _engine.TryGetValue(k, out var n);
            var provisional = n + 1;
            n += 1;
            _engine[k] = n;
            return new BundleCloseAllocation(n, provisional);
        }
        public void AdvanceOnPoEnd(string poNumber, int millNo, int threshold) { }
        public int GetEngineBatchNo(string poNumber, int millNo) => _engine.GetValueOrDefault(Key(poNumber, millNo));
        public void SetEngineBatchNo(string poNumber, int millNo, int batchNo) => _engine[Key(poNumber, millNo)] = batchNo;
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
}
