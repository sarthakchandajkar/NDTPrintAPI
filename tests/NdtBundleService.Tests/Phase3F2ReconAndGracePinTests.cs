using Microsoft.Extensions.Logging;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.PlcHandshake.S7;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>F-2 recon attach / surplus-deficit / PlcCloseGraceSeconds safety-net pins.</summary>
public sealed class Phase3F2ReconAndGracePinTests
{
    [Fact]
    public async Task Awaiting_recon_attach_leaves_sizeCounts_and_RunningTotal_untouched_and_opens_no_new_sequence()
    {
        var runtime = new TrackingRuntime();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        var engine = TestEngineFactory.Create(new FormationStub(10), new PipeSizeStub(), runtime, closeTrigger: "Plc");

        await engine.CloseBundleFromPlcAsync(
            "1000060163",
            1,
            pipeSize: null,
            plcCount: 11,
            (_, _, _) => Task.CompletedTask,
            CancellationToken.None);

        Assert.Equal(1, runtime.GetEngineBatchNo("1000060163", 1));
        Assert.Equal(0, runtime.GetSizeCounts("1000060163", 1).GetValueOrDefault("Default"));
        Assert.Equal(0, runtime.GetRunningTotal("1000060163", 1));

        var awaiting = ("1226100001", 1, 11);
        var slitSums = new Dictionary<(string Po, int Mill), int>();
        var attached = PlcCsvReconAttach.TryAttach(
            awaiting,
            new InputSlitRecord { PoNumber = "1000060163", MillNo = 1, SlitNo = "1", NdtPipes = 11 },
            slitSums,
            out var batchNo);

        Assert.True(attached);
        Assert.Equal("1226100001", batchNo);
        Assert.Equal(11, slitSums[("1000060163", 1)]);

        // Attach must not call ProcessSlitRecord / ApplySlitContribution — runtime stays frozen.
        Assert.Equal(0, runtime.GetSizeCounts("1000060163", 1).GetValueOrDefault("Default"));
        Assert.Equal(0, runtime.GetRunningTotal("1000060163", 1));
        Assert.Equal(1, runtime.GetEngineBatchNo("1000060163", 1));
    }

    [Fact]
    public void Surplus_and_deficit_recon_set_discrepancy_without_updating_stored_total()
    {
        var deficit = PlcCsvReconSemantics.Evaluate("1226100001", plcTotal: 11, slitSum: 10);
        Assert.True(deficit.CountDiscrepancy);
        Assert.True(deficit.ClearsAwaitingCsvRecon);
        Assert.False(deficit.UpdatesStoredTotal);
        Assert.Equal(11, deficit.PlcTotal);
        Assert.Equal(10, deficit.SlitSum);

        var surplus = PlcCsvReconSemantics.Evaluate("1226100001", plcTotal: 11, slitSum: 14);
        Assert.True(surplus.CountDiscrepancy);
        Assert.True(surplus.ClearsAwaitingCsvRecon);
        Assert.False(surplus.UpdatesStoredTotal);
        Assert.Equal(11, surplus.PlcTotal);
        Assert.Equal(14, surplus.SlitSum);

        var match = PlcCsvReconSemantics.Evaluate("1226100001", plcTotal: 11, slitSum: 11);
        Assert.False(match.CountDiscrepancy);
        Assert.False(match.UpdatesStoredTotal);
    }

    [Fact]
    public async Task After_plc_close_and_attach_next_bundle_starts_from_zero_not_surplus()
    {
        var runtime = new TrackingRuntime();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        var engine = TestEngineFactory.Create(new FormationStub(10), new PipeSizeStub(), runtime, closeTrigger: "Plc");

        await engine.CloseBundleFromPlcAsync(
            "1000060163",
            1,
            null,
            11,
            (_, _, _) => Task.CompletedTask,
            CancellationToken.None);

        // Late CSV surplus attached without ProcessSlitRecord (14 pcs on closed bundle only).
        var slitSums = new Dictionary<(string Po, int Mill), int>();
        Assert.True(PlcCsvReconAttach.TryAttach(
            ("1226100001", 1, 11),
            new InputSlitRecord { PoNumber = "1000060163", MillNo = 1, SlitNo = "2", NdtPipes = 14 },
            slitSums,
            out _));
        Assert.Equal(14, slitSums[("1000060163", 1)]);
        Assert.Equal(0, runtime.GetSizeCounts("1000060163", 1).GetValueOrDefault("Default"));

        // Next physical slit uses file path (unhealthy / File) — must close fresh 11, not 11+14.
        var fileEngine = TestEngineFactory.Create(
            new FormationStub(10),
            new PipeSizeStub(),
            runtime,
            closeTrigger: "File");

        var closed = new List<(int Batch, int Pcs)>();
        await fileEngine.ProcessSlitRecordAsync(
            new InputSlitRecord { PoNumber = "1000060163", MillNo = 1, SlitNo = "3", NdtPipes = 11 },
            (_, batch, pcs) =>
            {
                closed.Add((batch, pcs));
                return Task.CompletedTask;
            },
            CancellationToken.None);

        Assert.Single(closed);
        Assert.Equal(11, closed[0].Pcs);
        Assert.Equal(2, closed[0].Batch);
        Assert.Equal(0, runtime.GetSizeCounts("1000060163", 1).GetValueOrDefault("Default"));
    }

    [Fact]
    public async Task PlcCloseGrace_file_close_after_grace_when_healthy_and_no_plc_close()
    {
        var runtime = new TrackingRuntime();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 7, 19, 12, 0, 0, TimeSpan.Zero));
        var logger = new ListLogger();
        var engine = TestEngineFactory.Create(
            new FormationStub(10),
            new PipeSizeStub(),
            runtime,
            closeTrigger: "PlcWithFileFallback",
            s7Registry: new FixedRegistry(new AlwaysHealthyS7Provider()),
            plcCloseGraceSeconds: 60,
            timeProvider: clock,
            logger: logger);

        var closed = new List<int>();
        Task OnClose(InputSlitRecord _, int __, int total)
        {
            closed.Add(total);
            return Task.CompletedTask;
        }

        // Threshold reached — grace starts; no close yet.
        await engine.ProcessSlitRecordAsync(
            new InputSlitRecord { PoNumber = "1000060163", MillNo = 1, SlitNo = "1", NdtPipes = 11 },
            OnClose,
            CancellationToken.None);
        Assert.Empty(closed);
        Assert.Equal(11, runtime.GetSizeCounts("1000060163", 1)["Default"]);

        clock.Advance(TimeSpan.FromSeconds(59));
        await engine.ProcessSlitRecordAsync(
            new InputSlitRecord { PoNumber = "1000060163", MillNo = 1, SlitNo = "2", NdtPipes = 1 },
            OnClose,
            CancellationToken.None);
        Assert.Empty(closed);

        clock.Advance(TimeSpan.FromSeconds(2));
        await engine.ProcessSlitRecordAsync(
            new InputSlitRecord { PoNumber = "1000060163", MillNo = 1, SlitNo = "3", NdtPipes = 1 },
            OnClose,
            CancellationToken.None);

        Assert.Single(closed);
        Assert.Equal(13, closed[0]);
        Assert.Contains(logger.Messages, m => m.Contains("Missed PLC close", StringComparison.Ordinal));
        Assert.Equal(0, runtime.GetSizeCounts("1000060163", 1).GetValueOrDefault("Default"));
    }

    private sealed class ManualTimeProvider : TimeProvider
    {
        private DateTimeOffset _utc;
        public ManualTimeProvider(DateTimeOffset utc) => _utc = utc;
        public void Advance(TimeSpan delta) => _utc += delta;
        public override DateTimeOffset GetUtcNow() => _utc;
    }

    private sealed class ListLogger : ILogger<NdtBundleEngine>
    {
        public List<string> Messages { get; } = new();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Messages.Add(formatter(state, exception));
    }

    private sealed class TrackingRuntime : INdtBundleRuntimeStateStore
    {
        private readonly Dictionary<string, Dictionary<string, int>> _sizes = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, int> _engine = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, int> _running = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, InputSlitRecord?> _last = new(StringComparer.OrdinalIgnoreCase);

        private static string Key(string po, int mill) => $"{InputSlitCsvParsing.NormalizePo(po)}|{mill}";

        public Task EnsureInitializedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public int GetBatchOffset(string poNumber, int millNo) => GetEngineBatchNo(poNumber, millNo);
        public int GetRunningTotal(string poNumber, int millNo) => _running.GetValueOrDefault(Key(poNumber, millNo));
        public void ClearRunningTotal(string poNumber, int millNo) => _running[Key(poNumber, millNo)] = 0;
        public Task SyncBatchSequencesFromBundlesAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public void ApplySlitContribution(string poNumber, int millNo, int ndtPipes, int threshold, out int batchNumberForRow, out int totalSoFar)
        {
            var k = Key(poNumber, millNo);
            _running.TryGetValue(k, out var run);
            run += ndtPipes;
            _running[k] = run;
            totalSoFar = run;
            batchNumberForRow = GetEngineBatchNo(poNumber, millNo) + 1;
            if (run >= threshold)
                _running[k] = 0;
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

        public void AdvanceOnPoEnd(string poNumber, int millNo, int threshold) =>
            _running[Key(poNumber, millNo)] = 0;

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
