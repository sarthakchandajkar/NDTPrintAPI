using System.Reflection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.FileBasedPoChange;
using NdtBundleService.Services.PlcHandshake;
using NdtBundleService.Services.PlcHandshake.PlcPoEnd;
using NdtBundleService.Services.PoLifecycle;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// Pins production-time WIP ordering (SortKey primary, write-stamp secondary) against the
/// 2026-07-27 Mill-1 incidents: 06:16 cross-PO bounce and 04:15 same-PO out-of-order replay.
/// </summary>
public sealed class WipBundleProductionTimeOrderingTests : IDisposable
{
    private readonly string _wipFolder;

    public WipBundleProductionTimeOrderingTests()
    {
        _wipFolder = Path.Combine(Path.GetTempPath(), "wip-sortkey-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_wipFolder);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_wipFolder))
                Directory.Delete(_wipFolder, recursive: true);
        }
        catch
        {
            /* ignore */
        }
    }

    // T1 â€” 06:16 cross-PO bounce: after 546 ends, 522 backlog with fresher mtimes must not re-accept.
    [Fact]
    public async Task T1_AfterPoEnd_cross_po_backlog_replay_rejected_then_newer_po_accepted()
    {
        var notifier = new RecordingWipConfirmedNotifier();
        // Seed 522 chain then 546 so LastApplied / EndedPo floor track the live sequence.
        Write("WIP_01_1000060522_2601020504_260727_042012.csv", Utc(2026, 7, 27, 0, 20, 12));
        Write("WIP_01_1000060522_2601020509_260727_050214.csv", Utc(2026, 7, 27, 1, 2, 14));
        Write("WIP_01_1000060546_2601020510_260727_054023.csv", Utc(2026, 7, 27, 1, 40, 23));
        Write("WIP_01_1000060546_2601020511_260727_055211.csv", Utc(2026, 7, 27, 1, 52, 11));

        var provider = CreateProvider(notifier);
        Assert.Equal("1000060546", await provider.TryGetRunningPoForMillAsync(1, CancellationToken.None));
        Assert.Equal("260727_055211", GetLastAppliedSortKey(provider, 1));

        provider.NotifyPoEndForMill(1, "1000060546");
        Assert.True(provider.IsWaitingForNewWipAfterPoEnd(1));
        Assert.Equal("260727_055211", GetEndedPoLastWipSortKey(provider, 1));

        // Observed 06:16 arrival order: 522 backlog then 546 trailers, each with refreshed mtime.
        var drip = DateTime.UtcNow;
        foreach (var (name, po) in new[]
                 {
                     ("WIP_01_1000060522_2601020504_260727_042012.csv", "1000060522"),
                     ("WIP_01_1000060522_2601020505_260727_043027.csv", "1000060522"),
                     ("WIP_01_1000060522_2601020506_260727_043733.csv", "1000060522"),
                     ("WIP_01_1000060522_2601020507_260727_044518.csv", "1000060522"),
                     ("WIP_01_1000060522_2601020508_260727_045605.csv", "1000060522"),
                     ("WIP_01_1000060522_2601020509_260727_050214.csv", "1000060522"),
                     ("WIP_01_1000060546_2601020510_260727_054023.csv", "1000060546"),
                     ("WIP_01_1000060546_2601020511_260727_055211.csv", "1000060546"),
                     ("WIP_01_1000060546_2601020512_260727_060206.csv", "1000060546"),
                 })
        {
            drip = drip.AddSeconds(10);
            Write(name, drip);
            Assert.False(provider.TrySetRunningPoFromWipFile(1, po, drip, name));
        }

        Assert.True(provider.IsWaitingForNewWipAfterPoEnd(1));
        Assert.Null(await provider.TryGetRunningPoForMillAsync(1, CancellationToken.None));
        var confirmsBefore = notifier.Confirmations.Count;

        var nextStamp = drip.AddMinutes(1);
        Write("WIP_01_1000060549_2601020513_260727_074500.csv", nextStamp);
        Assert.True(provider.TrySetRunningPoFromWipFile(
            1, "1000060549", nextStamp, "WIP_01_1000060549_2601020513_260727_074500.csv"));

        Assert.False(provider.IsWaitingForNewWipAfterPoEnd(1));
        Assert.Equal("1000060549", await provider.TryGetRunningPoForMillAsync(1, CancellationToken.None));
        Assert.Equal(confirmsBefore + 1, notifier.Confirmations.Count);
        Assert.Contains(notifier.Confirmations, c => c.Mill == 1 && c.Po == "1000060549");
    }

    // T2 â€” 04:15 same-PO out-of-order-by-embedded-time: older SortKeys with fresher mtimes are ignored.
    [Fact]
    public async Task T2_SamePo_out_of_order_embedded_time_with_fresh_mtime_rejected()
    {
        Write("WIP_01_1000060522_2601020503_260727_041120.csv", Utc(2026, 7, 27, 0, 11, 20));
        var provider = CreateProvider();
        Assert.Equal("1000060522", await provider.TryGetRunningPoForMillAsync(1, CancellationToken.None));
        Assert.Equal("260727_041120", GetLastAppliedSortKey(provider, 1));

        var drip = DateTime.UtcNow;
        foreach (var name in new[]
                 {
                     "WIP_01_1000060522_2601020491_260727_021616.csv",
                     "WIP_01_1000060522_2601020492_260727_023602.csv",
                     "WIP_01_1000060522_2601020493_260727_024333.csv",
                     "WIP_01_1000060522_2601020500_260727_034720.csv",
                     "WIP_01_1000060522_2601020503_260727_041120.csv",
                 })
        {
            drip = drip.AddSeconds(8);
            Write(name, drip);
            Assert.False(InvokeTryApply(provider, 1, "1000060522", drip, name));
        }

        Assert.Equal("260727_041120", GetLastAppliedSortKey(provider, 1));
        Assert.Equal("1000060522", await provider.TryGetRunningPoForMillAsync(1, CancellationToken.None));

        drip = drip.AddSeconds(8);
        Write("WIP_01_1000060522_2601020504_260727_042012.csv", drip);
        Assert.True(InvokeTryApply(
            provider, 1, "1000060522", drip, "WIP_01_1000060522_2601020504_260727_042012.csv"));
        Assert.Equal("260727_042012", GetLastAppliedSortKey(provider, 1));
    }

    // T3 â€” SortKey forward but mtime regresses â†’ reject + pins the secondary-guard truth table.
    [Fact]
    public async Task T3_SortKey_forward_mtime_regresses_rejected()
    {
        var olderStamp = DateTime.UtcNow.AddHours(-2);
        var newerStamp = DateTime.UtcNow.AddHours(-1);
        Write("WIP_01_1000060522_2601020500_260727_050000.csv", newerStamp);
        var provider = CreateProvider();
        Assert.Equal("260727_050000", GetLastAppliedSortKey(provider, 1));

        Assert.False(InvokeTryApply(
            provider,
            1,
            "1000060546",
            olderStamp,
            "WIP_01_1000060546_2601020510_260727_060000.csv"));

        Assert.Equal("1000060522", await provider.TryGetRunningPoForMillAsync(1, CancellationToken.None));
        Assert.Equal("260727_050000", GetLastAppliedSortKey(provider, 1));
    }

    // T4 â€” no on-share files for ended PO â†’ floor falls back to PoEndUtc-derived key.
    [Fact]
    public async Task T4_PoEnd_floor_falls_back_to_po_end_utc_when_no_ended_po_files()
    {
        var provider = CreateProvider(); // empty folder â€” no seed
        Assert.Null(await provider.TryGetRunningPoForMillAsync(1, CancellationToken.None));

        var before = DateTime.UtcNow;
        provider.NotifyPoEndForMill(1, "1000060546");
        var after = DateTime.UtcNow;
        Assert.True(provider.IsWaitingForNewWipAfterPoEnd(1));

        var floor = GetEndedPoLastWipSortKey(provider, 1);
        Assert.False(string.IsNullOrEmpty(floor));
        Assert.True(WipSortKey.IsValid(floor));
        // Floor is plant-local formatting of PoEndUtc â€” must sit in the PO-end window.
        Assert.True(
            string.CompareOrdinal(floor, WipSortKey.FromUtc(before.AddSeconds(-2))) >= 0
            && string.CompareOrdinal(floor, WipSortKey.FromUtc(after.AddSeconds(2))) <= 0);

        var stamp = DateTime.UtcNow.AddMinutes(1);
        var futureKey = WipSortKey.FromUtc(stamp.AddMinutes(5));
        var name = $"WIP_01_1000060549_2601020999_{futureKey}.csv";
        Write(name, stamp);
        Assert.True(provider.TrySetRunningPoFromWipFile(1, "1000060549", stamp, name));
        Assert.Equal("1000060549", await provider.TryGetRunningPoForMillAsync(1, CancellationToken.None));
    }

    // T5 â€” genuine Aâ†’Bâ†’A resume: A's new WIP (SortKey after B's floor) reopens Closed A.
    [Fact]
    public async Task T5_Genuine_A_B_A_resume_reopens_closed_po()
    {
        var opts = CreatePlcOptions();
        var lifecycle = new PoLifecycleService(Monitor(opts));
        Assert.True(lifecycle.TryMarkDraining(1, "1000060522", DateTime.UtcNow.AddHours(-2)));
        Assert.True(lifecycle.TryMarkClosed(1, "1000060522"));
        Assert.Equal(PoLifecyclePhase.Closed, lifecycle.GetPhase(1, "1000060522"));

        Write("WIP_01_1000060546_2601020510_260727_054023.csv", Utc(2026, 7, 27, 1, 40, 23));
        Write("WIP_01_1000060546_2601020511_260727_055211.csv", Utc(2026, 7, 27, 1, 52, 11));

        var notifier = new RecordingWipConfirmedNotifier();
        var provider = CreateProvider(notifier);
        Assert.Equal("1000060546", await provider.TryGetRunningPoForMillAsync(1, CancellationToken.None));

        provider.NotifyPoEndForMill(1, "1000060546");
        Assert.Equal("260727_055211", GetEndedPoLastWipSortKey(provider, 1));

        var reopen = CreatePoReopen(lifecycle, opts);
        notifier.Handler = (mill, po) => reopen.TryReopenIfClosed(mill, po, po);

        var stamp = DateTime.UtcNow.AddMinutes(1);
        Write("WIP_01_1000060522_2601020600_260727_080000.csv", stamp);
        Assert.True(provider.TrySetRunningPoFromWipFile(
            1, "1000060522", stamp, "WIP_01_1000060522_2601020600_260727_080000.csv"));

        Assert.Equal("1000060522", await provider.TryGetRunningPoForMillAsync(1, CancellationToken.None));
        Assert.Equal(PoLifecyclePhase.Running, lifecycle.GetPhase(1, "1000060522"));
        Assert.Contains(notifier.Confirmations, c => c.Mill == 1 && c.Po == "1000060522");
    }

    // T6 â€” stale Closed-PO backlog must not apply and must not reopen.
    [Fact]
    public async Task T6_Stale_closed_po_backlog_does_not_reopen()
    {
        var opts = CreatePlcOptions();
        var lifecycle = new PoLifecycleService(Monitor(opts));
        Assert.True(lifecycle.TryMarkDraining(1, "1000060522", DateTime.UtcNow.AddHours(-3)));
        Assert.True(lifecycle.TryMarkClosed(1, "1000060522"));

        Write("WIP_01_1000060546_2601020511_260727_055211.csv", Utc(2026, 7, 27, 1, 52, 11));
        var notifier = new RecordingWipConfirmedNotifier();
        var provider = CreateProvider(notifier);
        provider.NotifyPoEndForMill(1, "1000060546");

        var reopen = CreatePoReopen(lifecycle, opts);
        var reopenCalls = 0;
        notifier.Handler = (mill, po) =>
        {
            reopenCalls++;
            reopen.TryReopenIfClosed(mill, po, po);
        };

        // Stale 522 backlog (SortKey before floor) with refreshed mtime â€” mill waiting.
        var drip = DateTime.UtcNow;
        Assert.False(provider.TrySetRunningPoFromWipFile(
            1, "1000060522", drip, "WIP_01_1000060522_2601020504_260727_042012.csv"));
        Assert.Equal(0, reopenCalls);
        Assert.Equal(PoLifecyclePhase.Closed, lifecycle.GetPhase(1, "1000060522"));

        // Accept a genuine next PO so mill is no longer waiting, then try stale 522 again
        // (covers the non-waiting TryApply path that previously could fire WIP confirmation).
        drip = drip.AddMinutes(1);
        Write("WIP_01_1000060549_2601020513_260727_074500.csv", drip);
        Assert.True(provider.TrySetRunningPoFromWipFile(
            1, "1000060549", drip, "WIP_01_1000060549_2601020513_260727_074500.csv"));
        var confirmsAfterAccept = notifier.Confirmations.Count;

        drip = drip.AddSeconds(10);
        Assert.False(InvokeTryApply(
            provider, 1, "1000060522", drip, "WIP_01_1000060522_2601020509_260727_050214.csv"));
        Assert.Equal(confirmsAfterAccept, notifier.Confirmations.Count);
        Assert.Equal(PoLifecyclePhase.Closed, lifecycle.GetPhase(1, "1000060522"));
        Assert.Equal("1000060549", await provider.TryGetRunningPoForMillAsync(1, CancellationToken.None));
    }

    // T7 lives in WipBundleReconciliationTests.ReconcileAsync_orders_by_embedded_production_time_not_write_stamp.

    // T8 â€” when WIP is waiting (running PO null), ResolvePoNumberAsync falls through to slit PO.
    [Fact]
    public async Task T8_ResolvePo_falls_through_to_slit_when_wip_waiting()
    {
        Write("WIP_01_1000060546_2601020511_260727_055211.csv", Utc(2026, 7, 27, 1, 52, 11));
        var provider = CreateProvider();
        provider.NotifyPoEndForMill(1, "1000060546");
        Assert.True(provider.IsWaitingForNewWipAfterPoEnd(1));
        Assert.Null(await provider.TryGetRunningPoForMillAsync(1, CancellationToken.None));

        var options = Options.Create(CreatePlcOptions());
        var worker = new PlcPoEndQueueWorker(
            new PlcPoEndQueue(),
            new StubPoEndWorkflow(),
            new StubActivePo(new Dictionary<int, string> { [1] = "1000060550" }),
            provider,
            new PoLifecycleService(Monitor(CreatePlcOptions())),
            new PlcHandshakeCoordinator(),
            new PlcHandshakeStatusRegistry(),
            options,
            NullLogger<PlcPoEndQueueWorker>.Instance);

        var resolved = await worker.ResolvePoNumberAsync(
            new PlcPoEndRequest
            {
                MillNo = 1,
                PoId = 0,
                NdtCountFinal = 3,
                CorrelationId = Guid.NewGuid()
            },
            CancellationToken.None);

        Assert.Equal("1000060550", resolved);
    }

    [Fact]
    public void WipSortKey_parses_full_and_short_variants()
    {
        Assert.Equal(
            "260727_054023",
            WipSortKey.TryGetFromFileName("WIP_01_1000060546_2601020510_260727_054023.csv"));
        Assert.Equal(
            "260727_054023",
            WipSortKey.TryGetFromFileName("WIP_01_1000060546_260727_054023.csv"));
        Assert.Null(WipSortKey.TryGetFromFileName("WIP_01_1000060546_1.csv"));
        Assert.Null(WipSortKey.TryGetFromFileName("FG_01_1000060546_260727_054023.csv"));
        Assert.True(WipSortKey.IsValid("260727_054023"));
        Assert.False(WipSortKey.IsValid("991332_997799"));
    }

    private WipBundleRunningPoProvider CreateProvider(IWipConfirmedRunningPoNotifier? notifier = null)
    {
        var options = Options.Create(new NdtBundleOptions
        {
            WaitForWipBundleAfterPoEnd = true,
            WipOrderingUseEmbeddedTimestamp = true,
            MillSlitLive = new MillSlitLiveOptions
            {
                WipBundleFolder = _wipFolder,
                WipBundleAcceptedFolder = _wipFolder
            },
            FgBundleFolder = _wipFolder,
            FgBundleAcceptedFolder = _wipFolder,
            PlcHandshake = new PlcHandshakeOptions
            {
                Mills =
                [
                    new MillConfig { MillNo = 1, PoEndSource = "Plc" },
                    new MillConfig { MillNo = 2, PoEndSource = "Plc" },
                    new MillConfig { MillNo = 3, PoEndSource = "Plc" },
                    new MillConfig { MillNo = 4, PoEndSource = "File" }
                ]
            }
        });

        var provider = new WipBundleRunningPoProvider(
            options,
            NullLogger<WipBundleRunningPoProvider>.Instance,
            notifier ?? new RecordingWipConfirmedNotifier(),
            new FileBasedPoChangeQueue());
        DisableWatchers(provider);
        return provider;
    }

    /// <summary>
    /// Tests drive WIP updates explicitly; live FS events race with Write() and double-apply the same SortKey.
    /// </summary>
    private static void DisableWatchers(WipBundleRunningPoProvider provider)
    {
        foreach (var fieldName in new[] { "_watchBundle", "_watchAccepted", "_watchFgBundle", "_watchFgAccepted" })
        {
            var field = typeof(WipBundleRunningPoProvider).GetField(
                fieldName,
                BindingFlags.Instance | BindingFlags.NonPublic);
            if (field?.GetValue(provider) is not FileSystemWatcher watcher)
                continue;

            watcher.EnableRaisingEvents = false;
            watcher.Dispose();
            field.SetValue(provider, null);
        }
    }

    private static NdtBundleOptions CreatePlcOptions() => new()
    {
        WaitForWipBundleAfterPoEnd = true,
        WipOrderingUseEmbeddedTimestamp = true,
        PoEndFlushMode = "Immediate",
        PlcHandshake = new PlcHandshakeOptions
        {
            Mills =
            [
                new MillConfig { MillNo = 1, PoEndSource = "Plc" },
                new MillConfig { MillNo = 2, PoEndSource = "Plc" },
                new MillConfig { MillNo = 3, PoEndSource = "Plc" },
                new MillConfig { MillNo = 4, PoEndSource = "File" }
            ]
        }
    };

    private static PoReopenService CreatePoReopen(IPoLifecycleService lifecycle, NdtBundleOptions opts) =>
        new(
            lifecycle,
            new NoOpRuntimeStore(),
            new NoOpRepo(),
            Monitor(opts),
            NullLogger<PoReopenService>.Instance);

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

    private void Write(string name, DateTime stampUtc)
    {
        var path = Path.Combine(_wipFolder, name);
        File.WriteAllText(path, "wip");
        File.SetLastWriteTimeUtc(path, stampUtc);
    }

    private static DateTime Utc(int y, int m, int d, int h, int min, int s) =>
        new(y, m, d, h, min, s, DateTimeKind.Utc);

    private static string GetLastAppliedSortKey(WipBundleRunningPoProvider provider, int millNo)
    {
        var mills = typeof(WipBundleRunningPoProvider)
            .GetField("_mills", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(provider)!;
        var st = ((Array)mills).GetValue(millNo - 1)!;
        return (string)st.GetType().GetField("LastAppliedWipSortKey")!.GetValue(st)!;
    }

    private static string GetEndedPoLastWipSortKey(WipBundleRunningPoProvider provider, int millNo)
    {
        var mills = typeof(WipBundleRunningPoProvider)
            .GetField("_mills", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(provider)!;
        var st = ((Array)mills).GetValue(millNo - 1)!;
        return (string)st.GetType().GetField("EndedPoLastWipSortKey")!.GetValue(st)!;
    }

    private static bool InvokeTryApply(
        WipBundleRunningPoProvider provider,
        int millNo,
        string newPo,
        DateTime wipStampUtc,
        string wipFileName)
    {
        var method = typeof(WipBundleRunningPoProvider).GetMethod(
            "TryApplyRunningPoUpdateUnsafe",
            BindingFlags.Instance | BindingFlags.NonPublic);
        return (bool)method!.Invoke(provider, [millNo, newPo, wipStampUtc, wipFileName])!;
    }

    private sealed class RecordingWipConfirmedNotifier : IWipConfirmedRunningPoNotifier
    {
        public List<(int Mill, string Po)> Confirmations { get; } = [];
        public Action<int, string>? Handler { get; set; }

        public void NotifyWipConfirmed(int millNo, string normalizedPo)
        {
            Confirmations.Add((millNo, normalizedPo));
            Handler?.Invoke(millNo, normalizedPo);
        }
    }

    private sealed class StubActivePo(IReadOnlyDictionary<int, string> map) : IActivePoPerMillService
    {
        public IReadOnlyList<string> GetInputSlitReadFolderPaths() => Array.Empty<string>();
        public Task<IReadOnlyDictionary<int, string>> GetLatestPoByMillAsync(CancellationToken cancellationToken) =>
            Task.FromResult(map);
    }

    private sealed class StubPoEndWorkflow : IPoEndWorkflowService
    {
        public Task<PoEndWorkflowResult> ExecuteAsync(
            string poNumber,
            int millNo,
            bool advancePoPlanFile,
            CancellationToken cancellationToken,
            Guid? correlationId = null) =>
            Task.FromResult(new PoEndWorkflowResult());

        public Task<PoEndWorkflowResult> ExecuteAsync(
            string poNumber,
            int millNo,
            bool advancePoPlanFile,
            CancellationToken cancellationToken,
            Guid? correlationId,
            int? plcNdtCountFinal) =>
            Task.FromResult(new PoEndWorkflowResult());
    }

    private sealed class NoOpRuntimeStore : INdtBundleRuntimeStateStore
    {
        public Task EnsureInitializedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public int GetBatchOffset(string poNumber, int millNo) => 0;
        public int GetRunningTotal(string poNumber, int millNo) => 0;
        public void ClearRunningTotal(string poNumber, int millNo) { }
        public void ClearOpenAccumulation(string poNumber, int millNo) { }
        public DateTime GetLastActivityUtc(string poNumber, int millNo) => DateTime.UtcNow;
        public Task SyncBatchSequencesFromBundlesAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public void ApplySlitContribution(string poNumber, int millNo, int ndtPipes, int threshold, out int totalSoFar)
        {
            totalSoFar = ndtPipes;
        }
        public BundleCloseAllocation CloseBundle(string poNumber, int millNo, int closedTotalPcs, int threshold) => new(1, 1);
        public void AdvanceOnPoEnd(string poNumber, int millNo, int threshold) { }
        public int GetEngineBatchNo(string poNumber, int millNo) => 0;
        public void SetEngineBatchNo(string poNumber, int millNo, int batchNo) { }
        public Dictionary<string, int> GetSizeCounts(string poNumber, int millNo) => new(StringComparer.OrdinalIgnoreCase);
        public void SetSizeCounts(string poNumber, int millNo, IReadOnlyDictionary<string, int> counts) { }
        public InputSlitRecord? GetLastRecord(string poNumber, int millNo) => null;
        public void SetLastRecord(string poNumber, int millNo, InputSlitRecord? record) { }
        public Task SaveAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class NoOpRepo : INdtBundleRepository
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
}
