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
    public async Task Plc_close_leaves_sizeCounts_and_RunningTotal_untouched_for_csv_fill()
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

        // CSV fill stamps against the closed target without ApplySlitContribution — runtime stays frozen.
        var stamp = CsvFillLogic.ComputeAfterStamp("1226100001", targetNdtPcs: 11, csvFilledBefore: 0, fileNdtPipes: 11, 20);
        Assert.Equal(CsvFillState.CsvComplete, stamp.FillState);
        Assert.Equal(0, runtime.GetSizeCounts("1000060163", 1).GetValueOrDefault("Default"));
        Assert.Equal(0, runtime.GetRunningTotal("1000060163", 1));
        Assert.Equal(1, runtime.GetEngineBatchNo("1000060163", 1));
    }

    [Fact]
    public void Surplus_and_deficit_fill_set_discrepancy_without_updating_printed_total()
    {
        var shortFill = CsvFillLogic.ComputeQuietShort(targetNdtPcs: 11, csvFilled: 10, 20);
        Assert.True(shortFill.CountDiscrepancy);
        Assert.Equal(CsvFillState.CsvShort, shortFill.State);

        var overshoot = CsvFillLogic.ComputeAfterStamp("1226100001", 11, 0, 14, 20);
        Assert.True(overshoot.CountDiscrepancy);
        Assert.Equal(CsvFillState.CsvOvershoot, overshoot.FillState);
        Assert.Equal(14, overshoot.CsvFilledAfter);

        var match = CsvFillLogic.ComputeAfterStamp("1226100001", 11, 0, 11, 20);
        Assert.False(match.CountDiscrepancy);
        Assert.Equal(CsvFillState.CsvComplete, match.FillState);
    }

    [Fact]
    public async Task After_plc_close_and_csv_overshoot_next_bundle_starts_from_zero_not_surplus()
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

        // Late CSV surplus fills the closed target only (overshoot) — does not touch runtime sizeCounts.
        var overshoot = CsvFillLogic.ComputeAfterStamp("1226100001", 11, 0, 14, 20);
        Assert.Equal(CsvFillState.CsvOvershoot, overshoot.FillState);
        Assert.Equal(0, runtime.GetSizeCounts("1000060163", 1).GetValueOrDefault("Default"));

        // Next physical slit uses file path — must close fresh 11, not 11+14.
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

        // Threshold reached â€” grace starts; no close yet.
        await engine.ProcessSlitRecordAsync(
            new InputSlitRecord { PoNumber = "1000060163", MillNo = 1, SlitNo = "1", NdtPipes = 11 },
            OnClose,
            CancellationToken.None);
        Assert.Empty(closed);
        Assert.Equal(0, runtime.GetSizeCounts("1000060163", 1).GetValueOrDefault("Default"));

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

    [Fact]
    public async Task PlcCloseGrace_close_total_is_isolated_file_sum_plc_sizeCounts_never_merged()
    {
        const string po = "1000060163";
        const int mill = 1;
        const int livePlcRemainder = 25;
        const int threshold = 80;

        var runtime = new TrackingRuntime();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        runtime.SetSizeCounts(po, mill, new Dictionary<string, int> { ["Default"] = livePlcRemainder });

        var clock = new ManualTimeProvider(new DateTimeOffset(2026, 7, 26, 12, 0, 0, TimeSpan.Zero));
        var engine = TestEngineFactory.Create(
            new FormationStub(threshold),
            new PipeSizeStub(),
            runtime,
            closeTrigger: "PlcWithFileFallback",
            s7Registry: new FixedRegistry(new AlwaysHealthyS7Provider()),
            plcCloseGraceSeconds: 60,
            timeProvider: clock);

        var closed = new List<int>();
        Task OnClose(InputSlitRecord _, int __, int total)
        {
            closed.Add(total);
            return Task.CompletedTask;
        }

        await engine.ProcessSlitRecordAsync(
            new InputSlitRecord { PoNumber = po, MillNo = mill, SlitNo = "1", NdtPipes = threshold },
            OnClose,
            CancellationToken.None);
        Assert.Empty(closed);
        Assert.Equal(livePlcRemainder, runtime.GetSizeCounts(po, mill)["Default"]);

        clock.Advance(TimeSpan.FromSeconds(61));
        await engine.ProcessSlitRecordAsync(
            new InputSlitRecord { PoNumber = po, MillNo = mill, SlitNo = "2", NdtPipes = 3 },
            OnClose,
            CancellationToken.None);

        Assert.Single(closed);
        Assert.Equal(threshold + 3, closed[0]);
        Assert.NotEqual(threshold + 3 + livePlcRemainder, closed[0]);
    }

    [Fact]
    public void ApplySlitContribution_zero_ndt_is_peek_only_no_running_total_or_sequence_burn()
    {
        var runtime = new TrackingRuntime();
        runtime.ApplySlitContribution("1000060288", 1, ndtPipes: 17, threshold: 80, out var afterFirst);
        Assert.Equal(17, afterFirst);
        Assert.Equal(0, runtime.GetEngineBatchNo("1000060288", 1));

        runtime.ApplySlitContribution("1000060288", 1, ndtPipes: 0, threshold: 80, out var peekTotal);
        Assert.Equal(17, peekTotal);
        Assert.Equal(17, runtime.GetRunningTotal("1000060288", 1));
        Assert.Equal(0, runtime.GetEngineBatchNo("1000060288", 1));
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
        public void ClearOpenAccumulation(string poNumber, int millNo) => ClearRunningTotal(poNumber, millNo);
        public DateTime GetLastActivityUtc(string poNumber, int millNo) => DateTime.UtcNow;
        public Task SyncBatchSequencesFromBundlesAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public void ApplySlitContribution(string poNumber, int millNo, int ndtPipes, int threshold, out int totalSoFar)
        {
            var k = Key(poNumber, millNo);
            _running.TryGetValue(k, out var run);
            run += ndtPipes;
            _running[k] = run;
            totalSoFar = run;
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
            return new BundleCloseAllocation(n);
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
