using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.PlcHandshake;
using NdtBundleService.Services.PlcHandshake.PlcPoEnd;
using NdtBundleService.Services.PoLifecycle;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class Phase1PoLifecycleTests
{
    [Fact]
    public void SlitWipGate_plc_with_file_po_does_not_skip()
    {
        Assert.False(SlitWipBundlingGate.ShouldSkipBundling(
            waitingForWip: true,
            runningPoFromWip: null,
            filePo: "1000060163",
            poEndSource: MillPoEndSource.Plc,
            bundleSlitRowsWithFilePoDuringWipWait: true));
    }

    [Fact]
    public void SlitWipGate_file_mill_still_skips_during_wip_wait()
    {
        Assert.True(SlitWipBundlingGate.ShouldSkipBundling(
            waitingForWip: true,
            runningPoFromWip: null,
            filePo: "1000060163",
            poEndSource: MillPoEndSource.File,
            bundleSlitRowsWithFilePoDuringWipWait: true));
    }

    [Fact]
    public void FileRetryTracker_parks_with_backoff_and_logs_once_per_step()
    {
        var tracker = new InputSlitFileRetryTracker();
        var t0 = new DateTime(2026, 7, 14, 20, 36, 0, DateTimeKind.Utc);
        var path = @"\\share\input\2510117_06.csv";

        var (d1, log1, step1) = tracker.Park(path, t0, [5, 30, 120]);
        Assert.Equal(TimeSpan.FromSeconds(5), d1);
        Assert.True(log1);
        Assert.Equal(0, step1);
        Assert.True(tracker.ShouldSkip(path, t0.AddSeconds(2)));
        Assert.False(tracker.ShouldSkip(path, t0.AddSeconds(6)));

        var (_, log2, step2) = tracker.Park(path, t0.AddSeconds(6), [5, 30, 120]);
        Assert.True(log2);
        Assert.Equal(1, step2);

        // Advance to final cap step — logs once
        var (_, log3, step3) = tracker.Park(path, t0.AddSeconds(40), [5, 30, 120]);
        Assert.True(log3);
        Assert.Equal(2, step3);

        // Same cap step again — no duplicate log
        var (_, log3b, _) = tracker.Park(path, t0.AddSeconds(41), [5, 30, 120]);
        Assert.False(log3b);

        tracker.Clear(path);
        Assert.False(tracker.ShouldSkip(path, t0.AddHours(1)));
    }

    [Fact]
    public async Task ResolvePo_uses_running_po_before_NotifyPoEnd_clears_it()
    {
        var wip = new RecordingWipProvider(runningPo: "1000060163");
        var activePo = new StubActivePo(new Dictionary<int, string> { [1] = "1000059986" }); // mismatched slit PO
        var options = Options.Create(CreatePlcOptions());
        var worker = new PlcPoEndQueueWorker(
            new PlcPoEndQueue(),
            new StubPoEndWorkflow(),
            activePo,
            wip,
            new PoLifecycleService(Monitor(CreatePlcOptions())),
            new PlcHandshakeCoordinator(),
            new PlcHandshakeStatusRegistry(),
            options,
            NullLogger<PlcPoEndQueueWorker>.Instance);

        var request = new PlcPoEndRequest
        {
            MillNo = 1,
            PoId = 6,
            NdtCountFinal = 5,
            CorrelationId = Guid.NewGuid()
        };

        Assert.Equal(0, wip.NotifyCount);
        var resolved = await worker.ResolvePoNumberAsync(request, CancellationToken.None);
        Assert.Equal("1000060163", resolved);
        Assert.Equal(0, wip.NotifyCount); // resolve must not clear RunningPo

        wip.NotifyPoEndForMill(1, "1000060163");
        Assert.Equal(1, wip.NotifyCount);
        Assert.Null(await wip.TryGetRunningPoForMillAsync(1, CancellationToken.None));
    }

    [Fact]
    public async Task PoEndWorkflow_AfterDrain_defers_flush_for_plc_only()
    {
        var runtime = new InMemoryRuntimeStateStore();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        runtime.SetSizeCounts("1000060163", 1, new Dictionary<string, int> { ["Default"] = 8 });

        var lifecycle = new PoLifecycleService(Monitor(CreatePlcOptions()));
        var wip = new RecordingWipProvider("1000060163");
        var engine = TestEngineFactory.Create(new FormationStub(10), new PipeSizeStub(), runtime);
        var closed = new List<int>();
        var output = new CapturingOutputWriter(closed);
        var workflow = CreateWorkflow(engine, output, runtime, wip, lifecycle, CreatePlcOptions(flushMode: "AfterDrain"));

        var result = await workflow.ExecuteAsync("1000060163", 1, advancePoPlanFile: false, CancellationToken.None);
        Assert.True(result.FlushDeferred);
        Assert.Equal(0, result.BundlesClosed);
        Assert.Empty(closed);
        Assert.Equal(8, runtime.GetSizeCounts("1000060163", 1)["Default"]);
        Assert.Equal(PoLifecyclePhase.Draining, lifecycle.GetPhase(1, "1000060163"));
        Assert.True(wip.IsWaitingForNewWipAfterPoEnd(1));
    }

    [Fact]
    public async Task PoEndWorkflow_File_mill_still_flushes_immediately()
    {
        var runtime = new InMemoryRuntimeStateStore();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        runtime.SetSizeCounts("1000060163", 4, new Dictionary<string, int> { ["Default"] = 8 });

        var opts = CreateFileMillOptions();
        var lifecycle = new PoLifecycleService(Monitor(opts));
        var wip = new RecordingWipProvider("1000060163");
        var engine = TestEngineFactory.Create(new FormationStub(10), new PipeSizeStub(), runtime);
        var closed = new List<int>();
        var workflow = CreateWorkflow(engine, new CapturingOutputWriter(closed), runtime, wip, lifecycle, opts);

        var result = await workflow.ExecuteAsync("1000060163", 4, advancePoPlanFile: false, CancellationToken.None);
        Assert.False(result.FlushDeferred);
        Assert.Equal(1, result.BundlesClosed);
        Assert.Equal(8, closed.Sum());
        Assert.Equal(PoLifecyclePhase.Running, lifecycle.GetPhase(4, "1000060163")); // File never enters lifecycle
    }

    [Fact]
    public async Task IncidentReplay_AfterDrain_late_rows_merge_into_one_tail_bundle()
    {
        // E2/E5 replay: 8 pcs open at PO end + 6 late → one 14-pc tail after drain (threshold 20 so late file does not auto-close).
        var opts = CreatePlcOptions(flushMode: "AfterDrain", drainMinutes: 1);
        var runtime = new InMemoryRuntimeStateStore();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        runtime.SetSizeCounts("1000060163", 1, new Dictionary<string, int> { ["Default"] = 8 });
        runtime.SetLastRecord("1000060163", 1, new InputSlitRecord
        {
            PoNumber = "1000060163",
            MillNo = 1,
            SlitNo = "03",
            NdtPipes = 8
        });

        var lifecycle = new PoLifecycleService(Monitor(opts));
        var wip = new RecordingWipProvider("1000060163");
        var engine = TestEngineFactory.Create(new FormationStub(20), new PipeSizeStub(), runtime);
        var closed = new List<(int Batch, int Pcs)>();
        var output = new CapturingOutputWriter(pcs => { }, closed);
        var workflow = CreateWorkflow(engine, output, runtime, wip, lifecycle, opts, threshold: 20);

        var poEnd = await workflow.ExecuteAsync("1000060163", 1, false, CancellationToken.None);
        Assert.True(poEnd.FlushDeferred);

        // F-3: late file 2510117_06 at ~20:36 while waiting for WIP — still bundles (no 85-min gate).
        Assert.False(SlitWipBundlingGate.ShouldSkipBundling(
            waitingForWip: true,
            runningPoFromWip: null,
            filePo: "1000060163",
            poEndSource: MillPoEndSource.Plc,
            bundleSlitRowsWithFilePoDuringWipWait: true));

        await engine.ProcessSlitRecordAsync(
            new InputSlitRecord { PoNumber = "1000060163", MillNo = 1, SlitNo = "06", NdtPipes = 6 },
            (_, _, _) => Task.CompletedTask,
            CancellationToken.None);
        Assert.Equal(14, runtime.GetSizeCounts("1000060163", 1)["Default"]);
        Assert.Empty(closed); // still below threshold — waiting for drain flush

        // Force drain expiry
        Assert.True(lifecycle.TryMarkDraining(1, "1000060163", DateTime.UtcNow.AddMinutes(-2)));
        var sweep = new PoLifecycleSweepWorker(
            lifecycle,
            workflow,
            new NdtBatchStateService(new FormationStub(20), new PipeSizeStub(), runtime),
            runtime,
            engine,
            output,
            new MillBundleStateLock(),
            Monitor(opts),
            NullLogger<PoLifecycleSweepWorker>.Instance);

        await sweep.SweepOnceAsync(CancellationToken.None);

        Assert.Equal(PoLifecyclePhase.Closed, lifecycle.GetPhase(1, "1000060163"));
        Assert.Single(closed);
        Assert.Equal(14, closed[0].Pcs);
        Assert.Equal(0, runtime.GetSizeCounts("1000060163", 1).GetValueOrDefault("Default"));
        Assert.False(NdtBundleRuntimeStateLogic.HasOpenPartialBundle(
            runtime.GetRunningTotal("1000060163", 1),
            runtime.GetSizeCounts("1000060163", 1)));
    }

    [Fact]
    public async Task IncidentReplay_Immediate_plus_orphan_sweep_closes_reopened_bundle()
    {
        var opts = CreatePlcOptions(flushMode: "Immediate", drainMinutes: 1);
        var runtime = new InMemoryRuntimeStateStore();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        runtime.SetSizeCounts("1000060163", 1, new Dictionary<string, int> { ["Default"] = 8 });

        var lifecycle = new PoLifecycleService(Monitor(opts));
        var wip = new RecordingWipProvider("1000060163");
        var engine = TestEngineFactory.Create(new FormationStub(10), new PipeSizeStub(), runtime);
        var closed = new List<(int Batch, int Pcs)>();
        var output = new CapturingOutputWriter(_ => { }, closed);
        var workflow = CreateWorkflow(engine, output, runtime, wip, lifecycle, opts);

        var result = await workflow.ExecuteAsync("1000060163", 1, false, CancellationToken.None);
        Assert.False(result.FlushDeferred);
        Assert.Equal(8, closed.Sum(c => c.Pcs));

        // Late 6 pcs reopen below-threshold orphan (E5 class)
        await engine.ProcessSlitRecordAsync(
            new InputSlitRecord { PoNumber = "1000060163", MillNo = 1, NdtPipes = 6, SlitNo = "06" },
            (_, _, _) => Task.CompletedTask,
            CancellationToken.None);
        Assert.Equal(6, runtime.GetSizeCounts("1000060163", 1)["Default"]);

        Assert.True(lifecycle.TryMarkDraining(1, "1000060163", DateTime.UtcNow.AddMinutes(-2)));
        var sweep = new PoLifecycleSweepWorker(
            lifecycle,
            workflow,
            new NdtBatchStateService(new FormationStub(10), new PipeSizeStub(), runtime),
            runtime,
            engine,
            output,
            new MillBundleStateLock(),
            Monitor(opts),
            NullLogger<PoLifecycleSweepWorker>.Instance);
        await sweep.SweepOnceAsync(CancellationToken.None);

        Assert.Equal(PoLifecyclePhase.Closed, lifecycle.GetPhase(1, "1000060163"));
        Assert.Equal(14, closed.Sum(c => c.Pcs)); // 8 + 6
        Assert.False(NdtBundleRuntimeStateLogic.HasOpenPartialBundle(
            runtime.GetRunningTotal("1000060163", 1),
            runtime.GetSizeCounts("1000060163", 1)));
    }

    [Fact]
    public async Task OrphanSweep_does_not_run_for_file_mills()
    {
        var opts = CreateFileMillOptions();
        opts.AutoCloseOrphanBundles = true;
        var runtime = new InMemoryRuntimeStateStore();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        runtime.SetSizeCounts("PO-FILE", 4, new Dictionary<string, int> { ["Default"] = 6 });

        var lifecycle = new PoLifecycleService(Monitor(opts));
        // Even if someone marks draining, File mill TryMarkDraining returns false
        Assert.False(lifecycle.TryMarkDraining(4, "PO-FILE", DateTime.UtcNow.AddHours(-3)));

        var closed = new List<(int, int)>();
        var engine = TestEngineFactory.Create(new FormationStub(10), new PipeSizeStub(), runtime);
        var workflow = CreateWorkflow(
            engine,
            new CapturingOutputWriter(_ => { }, closed),
            runtime,
            new RecordingWipProvider("PO-FILE"),
            lifecycle,
            opts);
        var sweep = new PoLifecycleSweepWorker(
            lifecycle,
            workflow,
            new NdtBatchStateService(new FormationStub(10), new PipeSizeStub(), runtime),
            runtime,
            engine,
            new CapturingOutputWriter(_ => { }, closed),
            new MillBundleStateLock(),
            Monitor(opts),
            NullLogger<PoLifecycleSweepWorker>.Instance);

        await sweep.SweepOnceAsync(CancellationToken.None);
        Assert.Empty(closed);
        Assert.Equal(6, runtime.GetSizeCounts("PO-FILE", 4)["Default"]);
    }

    private static PoEndWorkflowService CreateWorkflow(
        IBundleEngine engine,
        IBundleOutputWriter output,
        INdtBundleRuntimeStateStore runtime,
        IWipBundleRunningPoProvider wip,
        IPoLifecycleService lifecycle,
        NdtBundleOptions opts,
        int threshold = 10)
    {
        return new PoEndWorkflowService(
            engine,
            output,
            new NdtBatchStateService(new FormationStub(threshold), new PipeSizeStub(), runtime),
            runtime,
            new NoopLiveNdt(),
            wip,
            new MillBundleStateLock(),
            lifecycle,
            new TestOptionsMonitor<NdtBundleOptions>(opts),
            NullLogger<PoEndWorkflowService>.Instance);
    }

    private static TestOptionsMonitor<NdtBundleOptions> Monitor(NdtBundleOptions opts) => new(opts);

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

    private static NdtBundleOptions CreatePlcOptions(string flushMode = "AfterDrain", int drainMinutes = 120) =>
        new()
        {
            WaitForWipBundleAfterPoEnd = true,
            BundleSlitRowsWithFilePoDuringWipWait = true,
            PoEndFlushMode = flushMode,
            PoEndDrainMinutes = drainMinutes,
            AutoCloseOrphanBundles = true,
            PlcHandshake = new PlcHandshakeOptions
            {
                Enabled = true,
                Mills = [new MillConfig { MillNo = 1, Name = "Mill-1", PoEndSource = "Plc" }]
            }
        };

    private static NdtBundleOptions CreateFileMillOptions() =>
        new()
        {
            WaitForWipBundleAfterPoEnd = true,
            BundleSlitRowsWithFilePoDuringWipWait = true,
            PoEndFlushMode = "AfterDrain", // ignored for File
            AutoCloseOrphanBundles = true,
            PlcHandshake = new PlcHandshakeOptions
            {
                Enabled = true,
                Mills = [new MillConfig { MillNo = 4, Name = "Mill-4", PoEndSource = "File" }]
            }
        };

    private sealed class RecordingWipProvider : IWipBundleRunningPoProvider
    {
        private string? _running;
        private bool _waiting;

        public int NotifyCount { get; private set; }

        public RecordingWipProvider(string? runningPo)
        {
            _running = runningPo;
        }

        public Task<string?> TryGetRunningPoForMillAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult(_waiting ? null : _running);

        public void NotifyPoEndForMill(int millNo, string endedPo)
        {
            NotifyCount++;
            _waiting = true;
            _running = null;
        }

        public bool IsWaitingForNewWipAfterPoEnd(int millNo) => _waiting;

        public bool ResumeRunningWipForMill(int millNo)
        {
            _waiting = false;
            return true;
        }

        public bool TrySetRunningPoFromWipFile(int millNo, string newPo, DateTime wipStampUtc, string wipFileName)
        {
            _running = newPo;
            _waiting = false;
            return true;
        }
    }

    private sealed class StubActivePo : IActivePoPerMillService
    {
        private readonly IReadOnlyDictionary<int, string> _map;
        public StubActivePo(IReadOnlyDictionary<int, string> map) => _map = map;
        public Task<IReadOnlyDictionary<int, string>> GetLatestPoByMillAsync(CancellationToken cancellationToken) =>
            Task.FromResult(_map);
        public IReadOnlyList<string> GetInputSlitReadFolderPaths() => [];
    }

    private sealed class StubPoEndWorkflow : IPoEndWorkflowService
    {
        public Task<PoEndWorkflowResult> ExecuteAsync(string poNumber, int millNo, bool advancePoPlanFile, CancellationToken cancellationToken, Guid? correlationId = null) =>
            Task.FromResult(new PoEndWorkflowResult { PoNumber = poNumber, MillNo = millNo });
    }

    private sealed class CapturingOutputWriter : IBundleOutputWriter
    {
        private readonly Action<int>? _onPcs;
        private readonly List<(int Batch, int Pcs)>? _closed;

        public CapturingOutputWriter(List<int> pcsList)
        {
            _onPcs = pcsList.Add;
        }

        public CapturingOutputWriter(Action<int> onPcs, List<(int Batch, int Pcs)> closed)
        {
            _onPcs = onPcs;
            _closed = closed;
        }

        public Task WriteBundleAsync(InputSlitRecord contextRecord, int batchNo, int totalNdtPcs, CancellationToken cancellationToken, Guid? correlationId = null)
        {
            _onPcs?.Invoke(totalNdtPcs);
            _closed?.Add((batchNo, totalNdtPcs));
            return Task.CompletedTask;
        }
    }

    private sealed class NoopLiveNdt : IMillSlitLiveNdtAccumulator
    {
        public IReadOnlyList<int>? TryConsumeRawForBundleIncrements(string normalizedPoNumber, int millNo, int plcRawNdt) => null;
        public void OnPoEndForMill(string normalizedPoNumber, int millNo) { }
    }

    private sealed class FormationStub : IFormationChartProvider
    {
        private readonly IReadOnlyDictionary<string, FormationChartEntry> _chart;
        public FormationStub(int threshold) =>
            _chart = new Dictionary<string, FormationChartEntry>(StringComparer.OrdinalIgnoreCase)
            {
                ["Default"] = new() { PipeSize = "Default", RequiredNdtPcs = threshold }
            };
        public Task<IReadOnlyDictionary<string, FormationChartEntry>> GetFormationChartAsync(CancellationToken cancellationToken) =>
            Task.FromResult(_chart);
        public void InvalidateCache() { }
    }

    private sealed class PipeSizeStub : IPipeSizeProvider
    {
        public IReadOnlyDictionary<string, string>? TryGetCachedPipeSizes() => new Dictionary<string, string>();
        public Task<string?> TryGetPipeSizeForPoAsync(string poNumber, CancellationToken cancellationToken) =>
            Task.FromResult<string?>(null);
        public Task<IReadOnlyDictionary<string, string>> GetPipeSizeByPoAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<string, string>>(new Dictionary<string, string>());
    }

    private sealed class InMemoryRuntimeStateStore : INdtBundleRuntimeStateStore
    {
        private readonly Dictionary<string, PersistedSlot> _slots = new(StringComparer.OrdinalIgnoreCase);

        private sealed class PersistedSlot
        {
            public int BatchOffset;
            public int RunningTotal;
            public int EngineBatchNo;
            public Dictionary<string, int> SizeCounts = new(StringComparer.OrdinalIgnoreCase);
            public InputSlitRecord? LastRecord;
        }

        public Task EnsureInitializedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public int GetBatchOffset(string poNumber, int millNo) => Slot(poNumber, millNo).BatchOffset;
        public int GetRunningTotal(string poNumber, int millNo) => Slot(poNumber, millNo).RunningTotal;
        public void ClearRunningTotal(string poNumber, int millNo) => Slot(poNumber, millNo).RunningTotal = 0;
        public Task SyncBatchSequencesFromBundlesAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public void ApplySlitContribution(string poNumber, int millNo, int ndtPipes, int threshold, out int batchNumberForRow, out int totalSoFar)
        {
            var slot = Slot(poNumber, millNo);
            if (ndtPipes > 0)
                slot.RunningTotal += ndtPipes;
            totalSoFar = slot.RunningTotal;
            batchNumberForRow = slot.BatchOffset + 1;
            if (slot.RunningTotal >= threshold)
            {
                slot.BatchOffset += 1;
                slot.RunningTotal = 0;
            }
        }

        public BundleCloseAllocation CloseBundle(string poNumber, int millNo, int closedTotalPcs, int threshold)
        {
            var slot = Slot(poNumber, millNo);
            if (closedTotalPcs <= 0)
                return new BundleCloseAllocation(slot.EngineBatchNo, slot.EngineBatchNo + 1);
            var provisional = slot.EngineBatchNo + 1;
            slot.EngineBatchNo += 1;
            if (slot.BatchOffset < slot.EngineBatchNo)
                slot.BatchOffset = slot.EngineBatchNo;
            return new BundleCloseAllocation(slot.EngineBatchNo, provisional);
        }

        public void AdvanceOnPoEnd(string poNumber, int millNo, int threshold)
        {
            var slot = Slot(poNumber, millNo);
            slot.RunningTotal = 0;
            if (slot.BatchOffset < slot.EngineBatchNo)
                slot.BatchOffset = slot.EngineBatchNo;
        }

        public int GetEngineBatchNo(string poNumber, int millNo) => Slot(poNumber, millNo).EngineBatchNo;
        public void SetEngineBatchNo(string poNumber, int millNo, int batchNo) => Slot(poNumber, millNo).EngineBatchNo = batchNo;
        public Dictionary<string, int> GetSizeCounts(string poNumber, int millNo) =>
            new(Slot(poNumber, millNo).SizeCounts, StringComparer.OrdinalIgnoreCase);
        public void SetSizeCounts(string poNumber, int millNo, IReadOnlyDictionary<string, int> counts) =>
            Slot(poNumber, millNo).SizeCounts = new Dictionary<string, int>(counts, StringComparer.OrdinalIgnoreCase);
        public InputSlitRecord? GetLastRecord(string poNumber, int millNo) => Slot(poNumber, millNo).LastRecord;
        public void SetLastRecord(string poNumber, int millNo, InputSlitRecord? record) => Slot(poNumber, millNo).LastRecord = record;
        public Task SaveAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        private PersistedSlot Slot(string poNumber, int millNo)
        {
            var key = $"{InputSlitCsvParsing.NormalizePo(poNumber)}|{millNo}";
            if (!_slots.TryGetValue(key, out var slot))
            {
                slot = new PersistedSlot();
                _slots[key] = slot;
            }

            return slot;
        }
    }
}
