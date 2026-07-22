using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.PlcHandshake;
using NdtBundleService.Services.PoLifecycle;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>PO reopen (Closed→Running) for hold/resume cycles and orphan sweep guards.</summary>
public sealed class PoReopenLifecycleTests
{
    private const string PoA = "1000060100";
    private const string PoB = "1000060200";
    private const string PoC = "1000060300";
    private const int Mill = 1;
    private const int Threshold = 10;

    [Fact]
    public async Task HoldResumeReplay_reopened_PO_A_zero_orphan_prints_next_close_is_21()
    {
        var opts = CreatePlcOptions();
        var lifecycle = new PoLifecycleService(Monitor(opts));
        var runtime = new TestRuntimeStore();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        runtime.SetEngineBatchNo(PoA, Mill, 9);

        var engine = TestEngineFactory.Create(new FormationStub(Threshold), new PipeSizeStub(), runtime);
        var closed = new List<(string Po, int Batch, int Pcs)>();
        var output = new CapturingOutputWriter((po, batch, pcs) => closed.Add((po, batch, pcs)));
        var wip = new MutableWipProvider(PoA);
        var repo = new NoOpPlcCloseRepo();
        var poReopen = CreatePoReopen(lifecycle, runtime, repo, opts);
        var workflow = CreateWorkflow(engine, output, runtime, wip, lifecycle, opts);

        // PO-A: five full bundles → sequences 10–14
        for (var i = 0; i < 5; i++)
            await CloseFullBundleAsync(engine, output, PoA, Mill, Threshold);

        Assert.Equal([10, 11, 12, 13, 14], closed.Where(c => c.Po == PoA).Select(c => c.Batch).ToList());

        // PO change trigger for PO-A — remainder batch 15
        runtime.SetSizeCounts(PoA, Mill, new Dictionary<string, int> { ["Default"] = 7 });
        await workflow.ExecuteAsync(PoA, Mill, false, CancellationToken.None);
        await DrainAndCloseAsync(lifecycle, workflow, runtime, engine, output, opts, PoA);
        Assert.Contains(closed, c => c.Po == PoA && c.Batch == 15 && c.Pcs == 7);

        // PO-B: bundles 16–20
        wip.SetRunningPo(PoB);
        runtime.SetEngineBatchNo(PoB, Mill, 15);
        for (var i = 0; i < 5; i++)
            await CloseFullBundleAsync(engine, output, PoB, Mill, Threshold);

        Assert.Equal([16, 17, 18, 19, 20], closed.Where(c => c.Po == PoB).Select(c => c.Batch).ToList());

        await workflow.ExecuteAsync(PoB, Mill, false, CancellationToken.None);
        await DrainAndCloseAsync(lifecycle, workflow, runtime, engine, output, opts, PoB);
        Assert.Equal(PoLifecyclePhase.Closed, lifecycle.GetPhase(Mill, PoB));
        Assert.Equal(PoLifecyclePhase.Closed, lifecycle.GetPhase(Mill, PoA));

        var orphanCountBefore = closed.Count;

        // Slit activity marks resume candidate only — reopen requires WIP confirmation
        wip.SetWaitingAfterPoEnd(PoB);
        runtime.SetSizeCounts(PoA, Mill, new Dictionary<string, int> { ["Default"] = 6 });
        runtime.SetLastActivityUtc(PoA, Mill, DateTime.UtcNow);
        var closedEntries = lifecycle.GetClosedEntries();
        Assert.True(PoPlanValidation.IsKnownPo(CreatePlanSnapshot(PoA, PoB), PoA));
        Assert.True(PoResumeCandidateSelector.IsEligibleForResumeCandidate(Mill, PoA, PoB, closedEntries));
        Assert.True(lifecycle.TryMarkResumeCandidate(Mill, PoA));
        Assert.False(poReopen.TryReopenIfClosed(Mill, PoA, wipConfirmedRunningPo: ""));
        wip.AcceptNewPo(PoA);
        Assert.True(TryReopenFromWip(poReopen, wip, Mill, PoA));
        Assert.Equal(PoLifecyclePhase.Running, lifecycle.GetPhase(Mill, PoA));
        Assert.False(NdtBundleRuntimeStateLogic.HasOpenPartialBundle(
            runtime.GetRunningTotal(PoA, Mill),
            runtime.GetSizeCounts(PoA, Mill)));

        poReopen.TryReopenIfClosed(Mill, PoA, PoA);

        // Orphan sweep must not print while PO-A is running / is mill running PO
        var sweep = CreateSweep(lifecycle, workflow, runtime, engine, output, wip, opts);
        await sweep.SweepOnceAsync(CancellationToken.None);
        Assert.Equal(orphanCountBefore, closed.Count);

        // Resumed PO-A production closes as mill-wide 21
        runtime.SetSizeCounts(PoA, Mill, new Dictionary<string, int> { ["Default"] = Threshold });
        await engine.HandlePoEndAsync(
            PoA,
            Mill,
            async (ctx, batch, pcs) =>
            {
                closed.Add((PoA, batch, pcs));
                await output.WriteBundleAsync(ctx, batch, pcs, CancellationToken.None);
            },
            CancellationToken.None);

        Assert.Contains(closed, c => c.Po == PoA && c.Batch == 21 && c.Pcs == Threshold);
    }

    [Fact]
    public async Task DoubleHoldResumeCycle_reopens_twice_and_allocates_fresh_sequences()
    {
        var opts = CreatePlcOptions();
        var lifecycle = new PoLifecycleService(Monitor(opts));
        var runtime = new TestRuntimeStore();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        var engine = TestEngineFactory.Create(new FormationStub(Threshold), new PipeSizeStub(), runtime);
        var closed = new List<(string Po, int Batch, int Pcs)>();
        var output = new CapturingOutputWriter((po, batch, pcs) => closed.Add((po, batch, pcs)));
        var wip = new MutableWipProvider(PoA);
        var repo = new NoOpPlcCloseRepo();
        var poReopen = CreatePoReopen(lifecycle, runtime, repo, opts);
        var workflow = CreateWorkflow(engine, output, runtime, wip, lifecycle, opts);

        var reopenCount = 0;
        for (var cycle = 0; cycle < 2; cycle++)
        {
            var po = cycle == 0 ? PoA : PoB;
            wip.SetRunningPo(po);
            await CloseFullBundleAsync(engine, output, po, Mill, Threshold);
            await workflow.ExecuteAsync(po, Mill, false, CancellationToken.None);
            await DrainAndCloseAsync(lifecycle, workflow, runtime, engine, output, opts, po);
            Assert.Equal(PoLifecyclePhase.Closed, lifecycle.GetPhase(Mill, po));

            runtime.SetSizeCounts(po, Mill, new Dictionary<string, int> { ["Default"] = 4 });
            wip.SetRunningPo(po);
            if (TryReopenFromWip(poReopen, wip, Mill, po))
                reopenCount++;
            Assert.Equal(PoLifecyclePhase.Running, lifecycle.GetPhase(Mill, po));
            Assert.Equal(0, runtime.GetSizeCounts(po, Mill).GetValueOrDefault("Default"));

            runtime.SetSizeCounts(po, Mill, new Dictionary<string, int> { ["Default"] = Threshold });
            await engine.HandlePoEndAsync(
                po,
                Mill,
                async (ctx, batch, pcs) =>
                {
                    closed.Add((po, batch, pcs));
                    await output.WriteBundleAsync(ctx, batch, pcs, CancellationToken.None);
                },
                CancellationToken.None);
        }

        Assert.Equal(2, reopenCount);
        Assert.Equal(2, closed.Select(c => c.Po).Distinct().Count());
    }

    [Fact]
    public async Task ClosedPo_trickle_rows_without_readoption_still_orphan_swept()
    {
        var opts = CreatePlcOptions();
        var lifecycle = new PoLifecycleService(Monitor(opts));
        var runtime = new TestRuntimeStore();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        var engine = TestEngineFactory.Create(new FormationStub(Threshold), new PipeSizeStub(), runtime);
        var closed = new List<(string Po, int Batch, int Pcs)>();
        var output = new CapturingOutputWriter((po, batch, pcs) => closed.Add((po, batch, pcs)));
        var wip = new MutableWipProvider(PoB);
        var workflow = CreateWorkflow(engine, output, runtime, wip, lifecycle, opts);

        await workflow.ExecuteAsync(PoA, Mill, false, CancellationToken.None);
        lifecycle.TryMarkDraining(Mill, PoA, DateTime.UtcNow.AddHours(-1));
        await DrainAndCloseAsync(lifecycle, workflow, runtime, engine, output, opts, PoA);

        // Late trickle for Closed PO-A while mill runs PO-B — not WIP-confirmed
        runtime.SetSizeCounts(PoA, Mill, new Dictionary<string, int> { ["Default"] = 3 });
        runtime.SetLastActivityUtc(PoA, Mill, DateTime.UtcNow.AddHours(-2));
        Assert.False(PoRunningAdoption.IsWipConfirmedRunning(PoA, PoB));

        var sweep = CreateSweep(lifecycle, workflow, runtime, engine, output, wip, opts);
        await sweep.SweepOnceAsync(CancellationToken.None);

        Assert.Single(closed);
        Assert.Equal(PoA, closed[0].Po);
        Assert.Equal(3, closed[0].Pcs);
    }

    [Fact]
    public void OrphanSweepGuard_skips_running_phase_mill_running_po_and_row_quiescence()
    {
        var recentRow = DateTime.UtcNow.AddMinutes(-2);
        var staleRow = DateTime.UtcNow.AddMinutes(-30);

        Assert.False(OrphanSweepGuard.ShouldSweepClosedPo(
            Mill, PoA, PoLifecyclePhase.Running, PoA, recentRow, DateTime.UtcNow, orphanQuiescenceMinutes: 0));
        Assert.False(OrphanSweepGuard.ShouldSweepClosedPo(
            Mill, PoA, PoLifecyclePhase.Closed, PoA, recentRow, DateTime.UtcNow, orphanQuiescenceMinutes: 0));
        Assert.False(OrphanSweepGuard.ShouldSweepClosedPo(
            Mill, PoA, PoLifecyclePhase.Closed, PoB, recentRow, DateTime.UtcNow, orphanQuiescenceMinutes: 15));
        Assert.True(OrphanSweepGuard.ShouldSweepClosedPo(
            Mill, PoA, PoLifecyclePhase.Closed, PoB, staleRow, DateTime.UtcNow, orphanQuiescenceMinutes: 15));
        Assert.True(OrphanSweepGuard.ShouldSweepClosedPo(
            Mill, PoA, PoLifecyclePhase.Closed, PoB, staleRow, DateTime.UtcNow, orphanQuiescenceMinutes: 0));
    }

    [Fact]
    public async Task Reopen_force_finalizes_awaiting_recon_and_resumed_rows_use_new_bundle()
    {
        var opts = CreatePlcOptions();
        var lifecycle = new PoLifecycleService(Monitor(opts));
        var runtime = new TestRuntimeStore();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        var repo = new ReconRecordingRepository();
        const int remainderPlcTotal = 7;
        const string remainderBatch = "1226100015";
        repo.SetAwaiting(PoA, Mill, remainderBatch, remainderPlcTotal, slitRows: [( "03", 4 ), ("04", 3)]);

        var wip = new MutableWipProvider(null);
        var engine = TestEngineFactory.Create(new FormationStub(Threshold), new PipeSizeStub(), runtime, closeTrigger: "Plc");
        var closed = new List<(string Po, int Batch, int Pcs)>();
        var output = new CapturingOutputWriter((po, batch, pcs) => closed.Add((po, batch, pcs)));
        var poReopen = CreatePoReopen(lifecycle, runtime, repo, opts);

        lifecycle.TryMarkDraining(Mill, PoA, DateTime.UtcNow.AddHours(-1));
        lifecycle.TryMarkClosed(Mill, PoA);

        var awaiting = await repo.TryGetAwaitingPlcReconBatchAsync(PoA, Mill, CancellationToken.None);
        Assert.NotNull(awaiting);
        var slitSums = new Dictionary<(string Po, int Mill), int>();
        Assert.True(PlcCsvReconAttach.TryAttach(
            awaiting,
            new InputSlitRecord { PoNumber = PoA, MillNo = Mill, SlitNo = "03", NdtPipes = 4 },
            slitSums,
            out _));

        wip.AcceptNewPo(PoA);
        Assert.True(poReopen.TryReopenIfClosed(Mill, PoA, PoA));
        Assert.Null(await repo.TryGetAwaitingPlcReconBatchAsync(PoA, Mill, CancellationToken.None));
        Assert.Equal(remainderPlcTotal, repo.GetStoredTotal(remainderBatch));
        Assert.Equal(1, repo.ForceFinalizeCount);

        runtime.SetEngineBatchNo(PoA, Mill, 15);
        runtime.SetSizeCounts(PoA, Mill, new Dictionary<string, int> { ["Default"] = Threshold });
        await engine.HandlePoEndAsync(
            PoA,
            Mill,
            async (ctx, batch, pcs) =>
            {
                closed.Add((PoA, batch, pcs));
                await output.WriteBundleAsync(ctx, batch, pcs, CancellationToken.None);
            },
            CancellationToken.None);

        Assert.Contains(closed, c => c.Po == PoA && c.Batch == 16 && c.Pcs == Threshold);
        Assert.DoesNotContain(closed, c => c.Batch == 15 && c.Pcs != remainderPlcTotal);
    }

    [Fact]
    public async Task Stale_trickle_two_transitions_ago_no_candidate_no_reopen_orphan_after_quiescence()
    {
        var opts = CreatePlcOptions();
        opts.OrphanQuiescenceMinutes = 15;
        var lifecycle = new PoLifecycleService(Monitor(opts));
        var runtime = new TestRuntimeStore();
        await runtime.EnsureInitializedAsync(CancellationToken.None);

        lifecycle.TryMarkDraining(Mill, PoA, DateTime.UtcNow.AddDays(-3));
        lifecycle.TryMarkClosed(Mill, PoA);
        lifecycle.TryMarkDraining(Mill, PoB, DateTime.UtcNow.AddDays(-2));
        lifecycle.TryMarkClosed(Mill, PoB);
        lifecycle.TryMarkDraining(Mill, PoC, DateTime.UtcNow.AddHours(-1));
        lifecycle.TryMarkClosed(Mill, PoC);

        var wip = new MutableWipProvider(null);
        wip.SetWaitingAfterPoEnd(PoC);
        var closedEntries = lifecycle.GetClosedEntries();

        Assert.False(PoResumeCandidateSelector.IsEligibleForResumeCandidate(Mill, PoA, PoC, closedEntries));

        var repo = new NoOpPlcCloseRepo();
        var poReopen = CreatePoReopen(lifecycle, runtime, repo, opts);
        Assert.False(poReopen.TryReopenIfClosed(Mill, PoA, wipConfirmedRunningPo: ""));

        runtime.SetSizeCounts(PoA, Mill, new Dictionary<string, int> { ["Default"] = 2 });
        runtime.SetLastActivityUtc(PoA, Mill, DateTime.UtcNow.AddMinutes(-30));

        Assert.True(OrphanSweepGuard.ShouldSweepClosedPo(
            Mill,
            PoA,
            PoLifecyclePhase.Closed,
            millRunningPo: null,
            runtime.GetLastActivityUtc(PoA, Mill),
            DateTime.UtcNow,
            opts.OrphanQuiescenceMinutes));

        var engine = TestEngineFactory.Create(new FormationStub(Threshold), new PipeSizeStub(), runtime);
        var closed = new List<(string Po, int Batch, int Pcs)>();
        var output = new CapturingOutputWriter((po, batch, pcs) => closed.Add((po, batch, pcs)));
        var workflow = CreateWorkflow(engine, output, runtime, wip, lifecycle, opts);
        var sweep = CreateSweep(lifecycle, workflow, runtime, engine, output, wip, opts);
        await sweep.SweepOnceAsync(CancellationToken.None);

        Assert.Single(closed);
        Assert.Equal(PoA, closed[0].Po);
        Assert.Equal(2, closed[0].Pcs);
    }

    private static PoPlanWipEnrichmentSnapshot CreatePlanSnapshot(params string[] pos)
    {
        var byPo = pos.ToDictionary(
            p => p,
            p => new PoPlanWipRow { PoNumber = p },
            StringComparer.OrdinalIgnoreCase);
        return new PoPlanWipEnrichmentSnapshot(new Dictionary<int, PoPlanWipRow>(), byPo, "test");
    }

    private static PoReopenService CreatePoReopen(
        IPoLifecycleService lifecycle,
        TestRuntimeStore runtime,
        INdtBundleRepository repo,
        NdtBundleOptions opts) =>
        new(
            lifecycle,
            runtime,
            repo,
            Monitor(opts),
            NullLogger<PoReopenService>.Instance);

    private static bool TryReopenFromWip(PoReopenService poReopen, MutableWipProvider wip, int mill, string po)
    {
        var running = wip.RunningPo;
        return running is not null && poReopen.TryReopenIfClosed(mill, po, running);
    }

    private static async Task CloseFullBundleAsync(
        IBundleEngine engine,
        CapturingOutputWriter output,
        string po,
        int mill,
        int threshold)
    {
        var record = new InputSlitRecord { PoNumber = po, MillNo = mill, SlitNo = "01", NdtPipes = threshold };
        await engine.ProcessSlitRecordAsync(
            record,
            async (ctx, batch, pcs) => await output.WriteBundleAsync(ctx, batch, pcs, CancellationToken.None),
            CancellationToken.None);
    }

    private static async Task DrainAndCloseAsync(
        IPoLifecycleService lifecycle,
        PoEndWorkflowService workflow,
        TestRuntimeStore runtime,
        IBundleEngine engine,
        CapturingOutputWriter output,
        NdtBundleOptions opts,
        string po)
    {
        lifecycle.TryMarkDraining(Mill, po, DateTime.UtcNow.AddHours(-2));
        var sweep = new PoLifecycleSweepWorker(
            lifecycle,
            workflow,
            new NdtBatchStateService(new FormationStub(Threshold), new PipeSizeStub(), runtime),
            runtime,
            engine,
            output,
            new MillBundleStateLock(),
            new MutableWipProvider(null),
            Monitor(opts),
            NullLogger<PoLifecycleSweepWorker>.Instance);
        await sweep.SweepOnceAsync(CancellationToken.None);
    }

    private static PoLifecycleSweepWorker CreateSweep(
        IPoLifecycleService lifecycle,
        PoEndWorkflowService workflow,
        TestRuntimeStore runtime,
        IBundleEngine engine,
        CapturingOutputWriter output,
        MutableWipProvider wip,
        NdtBundleOptions opts) =>
        new(
            lifecycle,
            workflow,
            new NdtBatchStateService(new FormationStub(Threshold), new PipeSizeStub(), runtime),
            runtime,
            engine,
            output,
            new MillBundleStateLock(),
            wip,
            Monitor(opts),
            NullLogger<PoLifecycleSweepWorker>.Instance);

    private static PoEndWorkflowService CreateWorkflow(
        IBundleEngine engine,
        CapturingOutputWriter output,
        TestRuntimeStore runtime,
        MutableWipProvider wip,
        IPoLifecycleService lifecycle,
        NdtBundleOptions opts) =>
        new(
            engine,
            output,
            new NdtBatchStateService(new FormationStub(Threshold), new PipeSizeStub(), runtime),
            runtime,
            new NoopLiveNdt(),
            wip,
            new MillBundleStateLock(),
            lifecycle,
            new PipeSizeStub(),
            new NoOpPlcCloseRepo(),
            new PlcHandshakeStatusRegistry(),
            Monitor(opts),
            NullLogger<PoEndWorkflowService>.Instance);

    private static NdtBundleOptions CreatePlcOptions() =>
        new()
        {
            WaitForWipBundleAfterPoEnd = true,
            PoEndFlushMode = "Immediate",
            PoEndDrainMinutes = 1,
            AutoCloseOrphanBundles = true,
            OrphanQuiescenceMinutes = 0,
            PlcHandshake = new PlcHandshakeOptions
            {
                Enabled = true,
                Mills = [new MillConfig { MillNo = 1, Name = "Mill-1", PoEndSource = "Plc" }]
            }
        };

    private static TestOptionsMonitor<NdtBundleOptions> Monitor(NdtBundleOptions opts) => new(opts);

    private sealed class TestOptionsMonitor<T>(T value) : IOptionsMonitor<T>
    {
        public T CurrentValue { get; } = value;
        public T Get(string? name) => CurrentValue;
        public IDisposable OnChange(Action<T, string?> listener) => NullDisposable.Instance;
    }

    private sealed class NullDisposable : IDisposable
    {
        public static readonly NullDisposable Instance = new();
        public void Dispose() { }
    }

    private sealed class MutableWipProvider : IWipBundleRunningPoProvider
    {
        private string? _running;
        private bool _waiting;
        private string? _ended;

        public MutableWipProvider(string? runningPo) => _running = runningPo;

        public string? RunningPo => _waiting ? null : _running;
        public bool Waiting => _waiting;
        public string? EndedPo => _ended;

        public void SetRunningPo(string? po)
        {
            _running = po;
            _waiting = false;
            _ended = null;
        }

        public void SetWaitingAfterPoEnd(string endedPo)
        {
            _waiting = true;
            _ended = endedPo;
            _running = null;
        }

        public void AcceptNewPo(string po)
        {
            _running = po;
            _waiting = false;
            _ended = null;
        }

        public Task<string?> TryGetRunningPoForMillAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult(RunningPo);

        public void NotifyPoEndForMill(int millNo, string endedPo) => SetWaitingAfterPoEnd(endedPo);

        public bool IsWaitingForNewWipAfterPoEnd(int millNo) => _waiting;

        public bool TryGetPoEndWaitContext(int millNo, out bool waitingForNewWip, out string? endedPo)
        {
            waitingForNewWip = _waiting;
            endedPo = _ended;
            return true;
        }

        public bool ResumeRunningWipForMill(int millNo)
        {
            _waiting = false;
            return true;
        }

        public bool TrySetRunningPoFromWipFile(int millNo, string newPo, DateTime wipStampUtc, string wipFileName)
        {
            AcceptNewPo(newPo);
            return true;
        }
    }

    private sealed class CapturingOutputWriter : IBundleOutputWriter
    {
        private readonly Action<string, int, int> _onClose;

        public CapturingOutputWriter(Action<string, int, int> onClose) => _onClose = onClose;

        public Task WriteBundleAsync(
            InputSlitRecord contextRecord,
            int batchNo,
            int totalNdtPcs,
            CancellationToken cancellationToken,
            Guid? correlationId = null)
        {
            _onClose(
                InputSlitCsvParsing.NormalizePo(contextRecord.PoNumber),
                batchNo,
                totalNdtPcs);
            return Task.CompletedTask;
        }
    }

    private sealed class NoopLiveNdt : IMillSlitLiveNdtAccumulator
    {
        public IReadOnlyList<int>? TryConsumeRawForBundleIncrements(string normalizedPoNumber, int millNo, int plcRawNdt) => null;
        public void OnPoEndForMill(string normalizedPoNumber, int millNo) { }
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
            Task.FromResult<(string, int, int)?>(null);
        public Task<PlcCsvReconResult?> TryReconcilePlcClosedBundleAsync(
            string poNumber, int millNo, int slitSum, CancellationToken cancellationToken) =>
            Task.FromResult<PlcCsvReconResult?>(null);
        public Task<PlcCsvReconResult?> TryForceFinalizeAwaitingReconOnReopenAsync(
            string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<PlcCsvReconResult?>(null);
    }

    private sealed class ReconRecordingRepository : INdtBundleRepository
    {
        private (string Po, int Mill, string BundleNo, int PlcTotal)? _awaiting;
        private List<(string SlitNo, int NdtPipes)> _slits = [];
        private readonly Dictionary<string, int> _totals = new(StringComparer.OrdinalIgnoreCase);

        public int ForceFinalizeCount { get; private set; }

        public void SetAwaiting(
            string po,
            int mill,
            string bundleNo,
            int plcTotal,
            IReadOnlyList<(string SlitNo, int NdtPipes)> slitRows)
        {
            _awaiting = (po, mill, bundleNo, plcTotal);
            _slits = slitRows.ToList();
            _totals[bundleNo] = plcTotal;
        }

        public int GetStoredTotal(string bundleNo) => _totals.GetValueOrDefault(bundleNo);

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
            Task.FromResult<IReadOnlyList<(string SlitNo, int NdtPipes)>>(_slits);
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
            string poNumber, int millNo, CancellationToken cancellationToken)
        {
            if (_awaiting is null)
                return Task.FromResult<(string, int, int)?>(null);
            var a = _awaiting.Value;
            if (a.Mill != millNo || !InputSlitCsvParsing.PoEquals(a.Po, poNumber))
                return Task.FromResult<(string, int, int)?>(null);
            return Task.FromResult<(string, int, int)?>((a.BundleNo, 15, a.PlcTotal));
        }

        public Task<PlcCsvReconResult?> TryReconcilePlcClosedBundleAsync(
            string poNumber, int millNo, int slitSum, CancellationToken cancellationToken)
        {
            if (_awaiting is null)
                return Task.FromResult<PlcCsvReconResult?>(null);
            var a = _awaiting.Value;
            if (a.Mill != millNo || !InputSlitCsvParsing.PoEquals(a.Po, poNumber))
                return Task.FromResult<PlcCsvReconResult?>(null);

            _awaiting = null;
            var result = new PlcCsvReconResult
            {
                BundleNo = a.BundleNo,
                PlcTotal = a.PlcTotal,
                SlitSum = slitSum
            };
            return Task.FromResult<PlcCsvReconResult?>(result);
        }

        public async Task<PlcCsvReconResult?> TryForceFinalizeAwaitingReconOnReopenAsync(
            string poNumber, int millNo, CancellationToken cancellationToken)
        {
            ForceFinalizeCount++;
            var awaiting = await TryGetAwaitingPlcReconBatchAsync(poNumber, millNo, cancellationToken).ConfigureAwait(false);
            if (awaiting is null)
                return null;
            var slitSum = _slits.Sum(s => s.NdtPipes);
            return await TryReconcilePlcClosedBundleAsync(poNumber, millNo, slitSum, cancellationToken).ConfigureAwait(false);
        }
    }

    private sealed class FormationStub : IFormationChartProvider
    {
        public FormationStub(int threshold) =>
            Chart = new Dictionary<string, FormationChartEntry>(StringComparer.OrdinalIgnoreCase)
            {
                ["Default"] = new() { PipeSize = "Default", RequiredNdtPcs = threshold }
            };

        private IReadOnlyDictionary<string, FormationChartEntry> Chart { get; }

        public Task<IReadOnlyDictionary<string, FormationChartEntry>> GetFormationChartAsync(CancellationToken cancellationToken) =>
            Task.FromResult(Chart);
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

    /// <summary>Runtime store with mill-wide <see cref="NdtBundleRuntimeStateStore.CloseBundle"/> semantics.</summary>
    private sealed class TestRuntimeStore : INdtBundleRuntimeStateStore
    {
        private readonly Dictionary<string, Slot> _slots = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<int, int> _millMax = new();

        private sealed class Slot
        {
            public int BatchOffset;
            public int RunningTotal;
            public int EngineBatchNo;
            public int ProvisionalBatchNo;
            public Dictionary<string, int> SizeCounts = new(StringComparer.OrdinalIgnoreCase);
            public DateTime LastActivityUtc;
        }

        public Task EnsureInitializedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public int GetBatchOffset(string poNumber, int millNo) => S(poNumber, millNo).BatchOffset;
        public int GetRunningTotal(string poNumber, int millNo) => S(poNumber, millNo).RunningTotal;
        public void ClearRunningTotal(string poNumber, int millNo) => S(poNumber, millNo).RunningTotal = 0;

        public void ClearOpenAccumulation(string poNumber, int millNo)
        {
            var slot = S(poNumber, millNo);
            slot.RunningTotal = 0;
            slot.ProvisionalBatchNo = 0;
            slot.SizeCounts.Clear();
        }

        public DateTime GetLastActivityUtc(string poNumber, int millNo) => S(poNumber, millNo).LastActivityUtc;

        public void SetLastActivityUtc(string poNumber, int millNo, DateTime utc) =>
            S(poNumber, millNo).LastActivityUtc = utc;

        public void ApplySlitContribution(string poNumber, int millNo, int ndtPipes, int threshold, out int batchNumberForRow, out int totalSoFar)
        {
            var slot = S(poNumber, millNo);
            slot.LastActivityUtc = DateTime.UtcNow;
            var floor = _millMax.GetValueOrDefault(millNo);
            if (slot.ProvisionalBatchNo <= 0)
                slot.ProvisionalBatchNo = Math.Max(floor, slot.EngineBatchNo) + 1;
            batchNumberForRow = slot.ProvisionalBatchNo;
            if (ndtPipes > 0)
                slot.RunningTotal += ndtPipes;
            totalSoFar = slot.RunningTotal;
        }

        public BundleCloseAllocation CloseBundle(string poNumber, int millNo, int closedTotalPcs, int threshold)
        {
            var slot = S(poNumber, millNo);
            var floor = _millMax.GetValueOrDefault(millNo);
            var final = Math.Max(floor, slot.EngineBatchNo) + 1;
            _millMax[millNo] = final;
            slot.EngineBatchNo = final;
            slot.BatchOffset = final;
            slot.ProvisionalBatchNo = 0;
            slot.RunningTotal = 0;
            return new BundleCloseAllocation(final, final);
        }

        public void AdvanceOnPoEnd(string poNumber, int millNo, int threshold)
        {
            var slot = S(poNumber, millNo);
            slot.RunningTotal = 0;
            slot.ProvisionalBatchNo = 0;
        }

        public Task SyncBatchSequencesFromBundlesAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public int GetEngineBatchNo(string poNumber, int millNo) => S(poNumber, millNo).EngineBatchNo;

        public void SetEngineBatchNo(string poNumber, int millNo, int batchNo)
        {
            var slot = S(poNumber, millNo);
            slot.EngineBatchNo = batchNo;
            slot.BatchOffset = batchNo;
            _millMax[millNo] = Math.Max(_millMax.GetValueOrDefault(millNo), batchNo);
        }

        public Dictionary<string, int> GetSizeCounts(string poNumber, int millNo) =>
            new(S(poNumber, millNo).SizeCounts, StringComparer.OrdinalIgnoreCase);

        public void SetSizeCounts(string poNumber, int millNo, IReadOnlyDictionary<string, int> counts) =>
            S(poNumber, millNo).SizeCounts = new Dictionary<string, int>(counts, StringComparer.OrdinalIgnoreCase);

        public InputSlitRecord? GetLastRecord(string poNumber, int millNo) => null;
        public void SetLastRecord(string poNumber, int millNo, InputSlitRecord? record) { }
        public Task SaveAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        private Slot S(string poNumber, int millNo)
        {
            var key = $"{InputSlitCsvParsing.NormalizePo(poNumber)}|{millNo}";
            if (!_slots.TryGetValue(key, out var slot))
            {
                slot = new Slot();
                _slots[key] = slot;
            }

            return slot;
        }
    }
}
