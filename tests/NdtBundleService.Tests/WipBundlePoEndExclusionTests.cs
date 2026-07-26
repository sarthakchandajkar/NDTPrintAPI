using System.Reflection;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.FileBasedPoChange;
using NdtBundleService.Services.PlcHandshake;
using NdtBundleService.Services.PlcHandshake.S7;
using NdtBundleService.Services.PoLifecycle;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class WipBundlePoEndExclusionTests
{
    [Fact]
    public void TryEnqueueFileBasedPoChange_returns_false_for_plc_mill_without_enqueue()
    {
        var queue = new FileBasedPoChangeQueue();
        var provider = CreateProvider(queue, plcMillNo: 2);

        var result = InvokeTryEnqueueFileBasedPoChange(
            provider,
            millNo: 2,
            endedPo: "1000057001",
            newPo: "1000057002",
            wipFileName: "WIP_02_1000057002_1.csv");

        Assert.False(result);
        Assert.True(queue.TryEnqueue(new FileBasedPoChangeRequest { MillNo = 2, NewPo = "probe" }));
    }

    [Fact]
    public void TryApplyRunningPoUpdateUnsafe_plc_mill_updates_running_po_without_enqueue()
    {
        var queue = new FileBasedPoChangeQueue();
        var provider = CreateProvider(queue, plcMillNo: 2);
        var stamp = DateTime.UtcNow;

        Assert.True(provider.TrySetRunningPoFromWipFile(2, "1000057001", stamp, "WIP_02_1000057001_1.csv"));

        var result = InvokeTryApplyRunningPoUpdateUnsafe(
            provider,
            millNo: 2,
            newPo: "1000057002",
            wipStampUtc: stamp.AddMinutes(1),
            wipFileName: "WIP_02_1000057002_1.csv");

        Assert.True(result);
        Assert.True(queue.TryEnqueue(new FileBasedPoChangeRequest { MillNo = 2, NewPo = "probe" }));
    }

    [Fact]
    public void TryApplyRunningPoUpdateUnsafe_file_mill_enqueues_po_end()
    {
        var queue = new FileBasedPoChangeQueue();
        var provider = CreateProvider(queue, fileMillNo: 4);
        var stamp = DateTime.UtcNow;

        Assert.True(provider.TrySetRunningPoFromWipFile(4, "1000057001", stamp, "WIP_04_1000057001_1.csv"));

        var result = InvokeTryApplyRunningPoUpdateUnsafe(
            provider,
            millNo: 4,
            newPo: "1000057002",
            wipStampUtc: stamp.AddMinutes(1),
            wipFileName: "WIP_04_1000057002_1.csv");

        Assert.True(result);
        Assert.False(queue.TryEnqueue(new FileBasedPoChangeRequest { MillNo = 4, NewPo = "probe" }));
        queue.MarkCompleted(4);
    }

    private static WipBundleRunningPoProvider CreateProvider(
        FileBasedPoChangeQueue queue,
        int? plcMillNo = null,
        int? plcMillNo2 = null,
        int? fileMillNo = null,
        string? wipFolder = null)
    {
        var mills = new List<MillConfig>();
        if (plcMillNo is { } plc)
            mills.Add(new MillConfig { MillNo = plc, PoEndSource = "Plc" });
        if (plcMillNo2 is { } plc2)
            mills.Add(new MillConfig { MillNo = plc2, PoEndSource = "Plc" });
        if (fileMillNo is { } file)
            mills.Add(new MillConfig { MillNo = file, PoEndSource = "File" });

        var folder = wipFolder ?? string.Empty;
        var options = Options.Create(new NdtBundleOptions
        {
            WaitForWipBundleAfterPoEnd = true,
            MillSlitLive = new MillSlitLiveOptions
            {
                WipBundleFolder = folder,
                WipBundleAcceptedFolder = folder
            },
            FgBundleFolder = folder,
            FgBundleAcceptedFolder = folder,
            PlcHandshake = new PlcHandshakeOptions { Mills = mills }
        });

        return new WipBundleRunningPoProvider(
            options,
            NullLogger<WipBundleRunningPoProvider>.Instance,
            NullWipConfirmedRunningPoNotifier.Instance,
            queue);
    }

    private sealed class NullWipConfirmedRunningPoNotifier : IWipConfirmedRunningPoNotifier
    {
        public static readonly NullWipConfirmedRunningPoNotifier Instance = new();
        public void NotifyWipConfirmed(int millNo, string normalizedPo) { }
    }

    private static bool InvokeTryEnqueueFileBasedPoChange(
        WipBundleRunningPoProvider provider,
        int millNo,
        string endedPo,
        string newPo,
        string wipFileName)
    {
        var method = typeof(WipBundleRunningPoProvider).GetMethod(
            "TryEnqueueFileBasedPoChange",
            BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(method);

        return (bool)method!.Invoke(
            provider,
            [millNo, endedPo, newPo, DateTime.UtcNow, wipFileName])!;
    }

    [Fact]
    public void TryApplyRunningPoUpdateUnsafe_rejects_stale_wip_stamp_replay()
    {
        var queue = new FileBasedPoChangeQueue();
        var provider = CreateProvider(queue, plcMillNo: 1);
        var newer = DateTime.UtcNow;
        var older = newer.AddHours(-1);

        Assert.True(provider.TrySetRunningPoFromWipFile(1, "1000060288", newer, "WIP_01_1000060288_new.csv"));

        var result = InvokeTryApplyRunningPoUpdateUnsafe(
            provider,
            millNo: 1,
            newPo: "1000060363",
            wipStampUtc: older,
            wipFileName: "WIP_01_1000060363_stale.csv");

        Assert.False(result);
    }

    [Fact]
    public async Task Cold_start_accepts_first_wip_update_per_mill()
    {
        var provider = CreateProvider(new FileBasedPoChangeQueue(), plcMillNo: 1);
        var stamp = DateTime.UtcNow;

        Assert.True(provider.TrySetRunningPoFromWipFile(1, "1000060288", stamp, "WIP_01_1000060288_first.csv"));

        var running = await provider.TryGetRunningPoForMillAsync(1, CancellationToken.None);
        Assert.Equal("1000060288", running);
    }

    [Fact]
    public async Task RunningWipStamp_monotonic_is_per_mill()
    {
        var provider = CreateProvider(new FileBasedPoChangeQueue(), plcMillNo: 1, plcMillNo2: 2);
        var baseStamp = DateTime.UtcNow;

        Assert.True(provider.TrySetRunningPoFromWipFile(1, "1000060288", baseStamp.AddMinutes(10), "WIP_01_new.csv"));
        Assert.True(provider.TrySetRunningPoFromWipFile(2, "1000057001", baseStamp.AddMinutes(1), "WIP_02_old.csv"));

        Assert.False(InvokeTryApplyRunningPoUpdateUnsafe(
            provider, 1, "1000060363", baseStamp, "WIP_01_stale.csv"));

        Assert.True(InvokeTryApplyRunningPoUpdateUnsafe(
            provider, 2, "1000057002", baseStamp.AddMinutes(2), "WIP_02_newer.csv"));

        Assert.Equal("1000060288", await provider.TryGetRunningPoForMillAsync(1, CancellationToken.None));
        Assert.Equal("1000057002", await provider.TryGetRunningPoForMillAsync(2, CancellationToken.None));
    }

    [Fact]
    public async Task After_po_end_newer_wip_for_new_po_passes_while_stale_replay_rejected()
    {
        var wipFolder = Path.Combine(Path.GetTempPath(), "wip-monotonic-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(wipFolder);
        try
        {
            var endedStamp = DateTime.UtcNow.AddHours(-2);
            var staleStamp = endedStamp.AddMinutes(-30);
            var resumeStamp = endedStamp.AddMinutes(45);

            WriteWipFile(wipFolder, "WIP_01_1000060288_2601020437_260726_131201.csv", endedStamp);

            var provider = CreateProvider(new FileBasedPoChangeQueue(), plcMillNo: 1, wipFolder: wipFolder);
            Assert.Equal("1000060288", await provider.TryGetRunningPoForMillAsync(1, CancellationToken.None));

            provider.NotifyPoEndForMill(1, "1000060288");
            Assert.True(provider.IsWaitingForNewWipAfterPoEnd(1));

            Assert.False(provider.TrySetRunningPoFromWipFile(
                1,
                "1000060363",
                staleStamp,
                "WIP_01_1000060363_stale.csv"));

            Assert.True(provider.TrySetRunningPoFromWipFile(
                1,
                "1000060299",
                resumeStamp,
                "WIP_01_1000060299_2601020444_260726_150000.csv"));

            Assert.False(provider.IsWaitingForNewWipAfterPoEnd(1));
            Assert.Equal("1000060299", await provider.TryGetRunningPoForMillAsync(1, CancellationToken.None));
        }
        finally
        {
            try { Directory.Delete(wipFolder, recursive: true); } catch { /* ignore */ }
        }
    }

    private static void WriteWipFile(string folder, string name, DateTime stampUtc)
    {
        var path = Path.Combine(folder, name);
        File.WriteAllText(path, "wip");
        File.SetLastWriteTimeUtc(path, stampUtc);
    }

    [Fact]
    public async Task PlcSlitEnd_uses_active_po_not_flapped_wip_po()
    {
        const string stablePo = "1000060288";
        var runtime = new MiniRuntime();
        await runtime.EnsureInitializedAsync(CancellationToken.None);

        var closed = new List<(string Po, int Pcs)>();
        var closer = new PlcSlitEndBundleCloser(
            Options.Create(new NdtBundleOptions
            {
                CloseTrigger = "Plc",
                PlcHandshake = new PlcHandshakeOptions { SlitEndTriggerByte = -1 }
            }),
            new CapturingPlcCloseEngine(closed, runtime),
            new NoOpOutputWriter(),
            new FixedActivePo(stablePo),
            new PipeSizeStub(),
            new FormationStub(80),
            runtime,
            new MillBundleStateLock(),
            new NoOpPlcCloseRepo(),
            NullLogger<PlcSlitEndBundleCloser>.Instance);

        var mill = new MillConfig { Name = "Mill-1", MillNo = 1 };
        var s7 = new AlwaysHealthyNoOpS7();

        await closer.TryCloseOnSlitEndAsync(1, mill, s7, 0, 1, CancellationToken.None);
        await closer.TryCloseOnSlitEndAsync(1, mill, s7, 6, 1, CancellationToken.None);
        await closer.TryCloseOnSlitEndAsync(1, mill, s7, 0, 2, CancellationToken.None);

        Assert.Equal(stablePo, InputSlitCsvParsing.NormalizePo(stablePo));
        Assert.Equal(6, runtime.GetSizeCounts(stablePo, 1).GetValueOrDefault("Default"));
        Assert.Equal(0, runtime.GetSizeCounts("1000060363", 1).GetValueOrDefault("Default"));
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

    private sealed class CapturingPlcCloseEngine : IBundleEngine
    {
        private readonly List<(string Po, int Pcs)> _closed;
        private readonly MiniRuntime _runtime;

        public CapturingPlcCloseEngine(List<(string Po, int Pcs)> closed, MiniRuntime runtime)
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

        public Task CloseBundleFromPlcAsync(
            string poNumber,
            int millNo,
            string? pipeSize,
            int plcCount,
            Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
            CancellationToken cancellationToken,
            bool allowPartial = false) =>
            Task.CompletedTask;
    }

    private sealed class MiniRuntime : INdtBundleRuntimeStateStore
    {
        private readonly Dictionary<string, Dictionary<string, int>> _sizes = new(StringComparer.OrdinalIgnoreCase);

        private static string Key(string po, int mill) => $"{InputSlitCsvParsing.NormalizePo(po)}|{mill}";

        public Task EnsureInitializedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public int GetBatchOffset(string poNumber, int millNo) => 0;
        public int GetRunningTotal(string poNumber, int millNo) => 0;
        public void ClearRunningTotal(string poNumber, int millNo) { }
        public void ClearOpenAccumulation(string poNumber, int millNo) { }
        public DateTime GetLastActivityUtc(string poNumber, int millNo) => DateTime.UtcNow;
        public Task SyncBatchSequencesFromBundlesAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public void ApplySlitContribution(string poNumber, int millNo, int ndtPipes, int threshold, out int batchNumberForRow, out int totalSoFar)
        {
            batchNumberForRow = 1;
            totalSoFar = ndtPipes;
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
        public InputSlitRecord? GetLastRecord(string poNumber, int millNo) => null;
        public void SetLastRecord(string poNumber, int millNo, InputSlitRecord? record) { }
        public Task SaveAsync(CancellationToken cancellationToken) => Task.CompletedTask;
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
        public Task WriteBundleAsync(InputSlitRecord contextRecord, int ndtBatchNo, int totalNdtPcs, CancellationToken cancellationToken, Guid? correlationId = null) =>
            Task.CompletedTask;
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

    private static bool InvokeTryApplyRunningPoUpdateUnsafe(
        WipBundleRunningPoProvider provider,
        int millNo,
        string newPo,
        DateTime wipStampUtc,
        string wipFileName)
    {
        var method = typeof(WipBundleRunningPoProvider).GetMethod(
            "TryApplyRunningPoUpdateUnsafe",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(method);

        return (bool)method!.Invoke(
            provider,
            [millNo, newPo, wipStampUtc, wipFileName])!;
    }
}
