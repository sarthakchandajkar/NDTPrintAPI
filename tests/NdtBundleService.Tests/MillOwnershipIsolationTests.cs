using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.FileBasedPoChange;
using NdtBundleService.Services.PlcHandshake;
using NdtBundleService.Services.PoLifecycle;
using NdtBundleService.Services.TcpOpenComm;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// Isolation guarantees: foreign-mill rows/files/configs are ignored via <see cref="IMillOwnership"/>,
/// independent of config trim (e.g. InputSlitProcessMills / hand-edited appsettings).
/// </summary>
public sealed class MillOwnershipIsolationTests : IDisposable
{
    private readonly string _wipFolder;
    private readonly FileBasedPoChangeQueue _queue = new();

    public MillOwnershipIsolationTests()
    {
        _wipFolder = Path.Combine(Path.GetTempPath(), "own-iso-" + Guid.NewGuid().ToString("N"));
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

    [Fact]
    public void SlitMonitoringWorker_ownership_excludes_mill_even_when_InputSlitProcessMills_allows_all()
    {
        var o = new NdtBundleOptions { InputSlitProcessMills = null };
        var owned = TestMillOwnership.Mill(1).OwnedMills;

        Assert.True(SlitMonitoringWorker.IsMillAllowedForNdtInputSlit(o, 1, owned));
        Assert.False(SlitMonitoringWorker.IsMillAllowedForNdtInputSlit(o, 4, owned));
    }

    [Fact]
    public void SlitMonitoringWorker_ownership_excludes_even_when_InputSlitProcessMills_lists_foreign_mill()
    {
        var o = new NdtBundleOptions { InputSlitProcessMills = [1, 4] };
        var owned = TestMillOwnership.Mill(1).OwnedMills;

        Assert.True(SlitMonitoringWorker.IsMillAllowedForNdtInputSlit(o, 1, owned));
        Assert.False(SlitMonitoringWorker.IsMillAllowedForNdtInputSlit(o, 4, owned));
    }

    [Fact]
    public void PlcHandshakeWorker_ignores_foreign_mill_configs()
    {
        var mills = new[]
        {
            new MillConfig { MillNo = 1, Name = "M1", IpAddress = "10.0.0.1", PlcHandshakeEnabled = true },
            new MillConfig { MillNo = 4, Name = "M4", IpAddress = "10.0.0.4", PlcHandshakeEnabled = true }
        };

        var selected = PlcHandshakeWorker.SelectOwnedHandshakeMills(mills, TestMillOwnership.Mill(1));

        Assert.Single(selected);
        Assert.Equal(1, selected[0].ResolveMillNo());
    }

    [Fact]
    public void MillTcpOpenCommWorker_ignores_foreign_TcpOpen_mill()
    {
        var options = new NdtBundleOptions
        {
            PlcHandshake = new PlcHandshakeOptions
            {
                Mills =
                [
                    new MillConfig
                    {
                        MillNo = 1,
                        PoEndSource = "TcpOpen",
                        TcpOpenCommHost = "127.0.0.1",
                        TcpOpenPort = 5001
                    },
                    new MillConfig
                    {
                        MillNo = 4,
                        PoEndSource = "TcpOpen",
                        TcpOpenCommHost = "127.0.0.1",
                        TcpOpenPort = 5004
                    }
                ]
            }
        };

        var selected = MillTcpOpenCommWorker.SelectOwnedTcpOpenMills(options, TestMillOwnership.Mill(1));

        Assert.Single(selected);
        Assert.Equal(1, selected[0].ResolveMillNo());
    }

    [Fact]
    public async Task WipBundleReconciliationService_ignores_foreign_file_mill_wip()
    {
        WriteWip("WIP_04_1000057001_010101_100000.csv", DateTime.UtcNow.AddMinutes(-20));
        WriteWip("WIP_04_1000057002_010102_110000.csv", DateTime.UtcNow.AddMinutes(-10));

        var options = Options.Create(new NdtBundleOptions
        {
            UseSqlServerForBundles = true,
            ConnectionString = "Server=.;Database=test;",
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
                    new MillConfig { MillNo = 1, PoEndSource = "File" },
                    new MillConfig { MillNo = 4, PoEndSource = "File" }
                ]
            }
        });

        var sut = new WipBundleReconciliationService(
            options,
            new EmptyBundles(),
            new StubWip(new Dictionary<int, string> { [4] = "1000057001" }),
            _queue,
            TestMillOwnership.Mill(1),
            NullLogger<WipBundleReconciliationService>.Instance);

        var enqueued = await sut.ReconcileAsync(CancellationToken.None);

        Assert.Equal(0, enqueued);
        Assert.True(_queue.TryEnqueue(new FileBasedPoChangeRequest { MillNo = 4, NewPo = "probe" }));
    }

    [Fact]
    public async Task PoLifecycleSweepWorker_ignores_foreign_mill_drain()
    {
        var lifecycle = new ForeignDrainLifecycle();
        var opts = new NdtBundleOptions
        {
            PoEndDrainMinutes = 1,
            AutoCloseOrphanBundles = false,
            PlcHandshake = new PlcHandshakeOptions
            {
                Mills =
                [
                    new MillConfig { MillNo = 1, PoEndSource = "Plc" },
                    new MillConfig { MillNo = 4, PoEndSource = "Plc" }
                ]
            }
        };

        var runtime = new EmptyRuntime();
        var sweep = new PoLifecycleSweepWorker(
            lifecycle,
            null!, // FlushPartials never reached when ownership filters mill 4
            new NdtBatchStateService(new FormationStub(), new PipeSizeStub(), runtime),
            runtime,
            null!, // engine never reached
            new NoOpOutput(),
            new MillBundleStateLock(),
            new StubWip(new Dictionary<int, string>()),
            new TestOptionsMonitor<NdtBundleOptions>(opts),
            TestMillOwnership.Mill(1),
            NullLogger<PoLifecycleSweepWorker>.Instance);

        await sweep.SweepOnceAsync(CancellationToken.None);

        Assert.Equal(0, lifecycle.GetPhaseCalls);
        Assert.Equal(0, lifecycle.TryMarkClosedCalls);
    }

    [Fact]
    public void WipBundleRunningPoProvider_does_not_enqueue_file_po_end_for_foreign_mill()
    {
        var options = Options.Create(new NdtBundleOptions
        {
            WaitForWipBundleAfterPoEnd = true,
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
                    new MillConfig { MillNo = 1, PoEndSource = "File" },
                    new MillConfig { MillNo = 4, PoEndSource = "File" }
                ]
            }
        });

        var provider = new WipBundleRunningPoProvider(
            options,
            NullLogger<WipBundleRunningPoProvider>.Instance,
            NullWipConfirmed.Instance,
            TestMillOwnership.Mill(1),
            _queue);

        var method = typeof(WipBundleRunningPoProvider).GetMethod(
            "TryEnqueueFileBasedPoChange",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        Assert.NotNull(method);

        var enqueued = (bool)method!.Invoke(
            provider,
            [4, "1000057001", "1000057002", DateTime.UtcNow, "WIP_04_1000057002_010102_110000.csv"])!;

        Assert.False(enqueued);
        Assert.True(_queue.TryEnqueue(new FileBasedPoChangeRequest { MillNo = 4, NewPo = "probe" }));
        provider.Dispose();
    }

    [Fact]
    public async Task FileBasedPoChangeWorker_skips_foreign_mill_queue_item()
    {
        var workflow = new RecordingPoEndWorkflow();
        var options = Options.Create(new NdtBundleOptions
        {
            PlcHandshake = new PlcHandshakeOptions
            {
                Mills = [new MillConfig { MillNo = 4, PoEndSource = "File" }]
            }
        });

        var worker = new FileBasedPoChangeWorker(
            _queue,
            workflow,
            new StubWip(new Dictionary<int, string>()),
            new StubActivePo(),
            options,
            TestMillOwnership.Mill(1),
            NullLogger<FileBasedPoChangeWorker>.Instance);

        await worker.ProcessRequestCoreAsync(
            new FileBasedPoChangeRequest
            {
                MillNo = 4,
                EndedPo = "1000057001",
                NewPo = "1000057002",
                WipFileName = "WIP_04_1000057002_010102_110000.csv",
                WipStampUtc = DateTime.UtcNow
            },
            Guid.NewGuid(),
            CancellationToken.None);

        Assert.Equal(0, workflow.ExecuteCalls);
    }

    [Fact]
    public async Task RuntimeStateStore_hydrate_ignores_foreign_mill_bundles()
    {
        var path = Path.Combine(_wipFolder, "runtime-m1.json");
        var options = new TestOptionsMonitor<NdtBundleOptions>(new NdtBundleOptions
        {
            EnableNdtBundleRuntimeStatePersistence = true,
            NdtBundleRuntimeStateFile = path,
            UseSqlServerForBundles = false,
            SyncRuntimeStateFromPrintedBundlesOnly = false,
            RuntimeStatePruning = new RuntimeStatePruningOptions { Enabled = false }
        });

        var repo = new SeedBundles(
        [
            new NdtBundleRecord
            {
                BundleNo = NdtBundleSequence.Format(1, 1),
                MillNo = 1,
                PoNumber = "1000000001",
                PrintedAt = DateTime.UtcNow,
                TotalNdtPcs = 10
            },
            new NdtBundleRecord
            {
                BundleNo = NdtBundleSequence.Format(99, 4),
                MillNo = 4,
                PoNumber = "1000000004",
                PrintedAt = DateTime.UtcNow,
                TotalNdtPcs = 10
            }
        ]);

        var store = new NdtBundleRuntimeStateStore(
            options,
            repo,
            new StubActivePo(),
            TestMillOwnership.Mill(1),
            NullLogger<NdtBundleRuntimeStateStore>.Instance);

        await store.EnsureInitializedAsync(CancellationToken.None);
        await store.SaveAsync(CancellationToken.None);

        var json = await File.ReadAllTextAsync(path);
        Assert.Contains("1000000001|1", json, StringComparison.Ordinal);
        Assert.DoesNotContain("1000000004|4", json, StringComparison.Ordinal);
        Assert.DoesNotContain("\"4\":", json, StringComparison.Ordinal);
    }

    private void WriteWip(string name, DateTime stampUtc)
    {
        var path = Path.Combine(_wipFolder, name);
        File.WriteAllText(path, "wip");
        File.SetLastWriteTimeUtc(path, stampUtc);
    }

    private sealed class ForeignDrainLifecycle : IPoLifecycleService
    {
        public int GetPhaseCalls { get; private set; }
        public int TryMarkClosedCalls { get; private set; }

        public bool TryMarkDraining(int millNo, string poNumber, DateTime endedAtUtc) => true;
        public bool TryMarkClosed(int millNo, string poNumber)
        {
            TryMarkClosedCalls++;
            return true;
        }

        public bool TryReopen(int millNo, string poNumber) => false;
        public bool TryMarkResumeCandidate(int millNo, string poNumber) => false;
        public bool IsResumeCandidate(int millNo, string poNumber) => false;
        public PoLifecyclePhase GetPhase(int millNo, string poNumber)
        {
            GetPhaseCalls++;
            return PoLifecyclePhase.Draining;
        }

        public IReadOnlyList<PoLifecycleDrainEntry> GetExpiredDrains(DateTime utcNow, TimeSpan drainWindow) =>
        [
            new PoLifecycleDrainEntry(4, "1000060999", utcNow.AddHours(-2), PoLifecyclePhase.Draining)
        ];

        public IReadOnlyList<PoLifecycleDrainEntry> GetClosedEntries() => [];
    }

    private sealed class RecordingPoEndWorkflow : IPoEndWorkflowService
    {
        public int ExecuteCalls { get; private set; }

        public Task<PoEndWorkflowResult> ExecuteAsync(
            string poNumber,
            int millNo,
            bool advancePoPlanFile,
            CancellationToken cancellationToken,
            Guid? correlationId = null)
        {
            ExecuteCalls++;
            return Task.FromResult(new PoEndWorkflowResult { PoNumber = poNumber, MillNo = millNo });
        }

        public Task<PoEndWorkflowResult> ExecuteAsync(
            string poNumber,
            int millNo,
            bool advancePoPlanFile,
            CancellationToken cancellationToken,
            Guid? correlationId,
            int? plcNdtCountFinal) =>
            ExecuteAsync(poNumber, millNo, advancePoPlanFile, cancellationToken, correlationId);
    }

    private sealed class StubWip(IReadOnlyDictionary<int, string> map) : IWipBundleRunningPoProvider
    {
        public Task<string?> TryGetRunningPoForMillAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult(map.TryGetValue(millNo, out var po) ? po : null);

        public void NotifyPoEndForMill(int millNo, string endedPo) { }
        public bool IsWaitingForNewWipAfterPoEnd(int millNo) => false;
        public bool TryGetPoEndWaitContext(int millNo, out bool waitingForNewWip, out string? endedPo)
        {
            waitingForNewWip = false;
            endedPo = null;
            return true;
        }

        public bool ResumeRunningWipForMill(int millNo) => false;
        public bool TrySetRunningPoFromWipFile(int millNo, string newPo, DateTime wipStampUtc, string wipFileName) => false;
    }

    private sealed class StubActivePo : IActivePoPerMillService
    {
        public Task<IReadOnlyDictionary<int, string>> GetLatestPoByMillAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<int, string>>(new Dictionary<int, string>());

        public IReadOnlyList<string> GetInputSlitReadFolderPaths() => [];
    }

    private sealed class NullWipConfirmed : IWipConfirmedRunningPoNotifier
    {
        public static readonly NullWipConfirmed Instance = new();
        public void NotifyWipConfirmed(int millNo, string normalizedPo) { }
    }

    private class EmptyBundles : INdtBundleRepository
    {
        public Task RecordBundleAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RecordBundlePendingPrintAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task UpdateBundlePrintStatusAsync(string bundleNo, string printStatus, string? printError, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<IReadOnlyList<NdtBundleRecord>> GetStuckPrintsAsync(TimeSpan olderThan, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<NdtBundleRecord>>([]);
        public virtual Task<IReadOnlyList<NdtBundleRecord>> GetBundlesAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<NdtBundleRecord>>([]);
        public Task<NdtBundleRecord?> GetByBatchNoAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(null);
        public Task UpdateBundlePipesAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<int> UpdateOutputCsvFilesForBundleAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<IReadOnlyList<(string SlitNo, int NdtPipes)>> GetSlitsForBatchAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string SlitNo, int NdtPipes)>>([]);
        public Task<int> UpdateOutputCsvFilesForSlitAsync(string batchNo, string slitNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task UpdateBundleTotalInDatabaseAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<bool> UpdateBundleSummaryCsvAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.FromResult(false);
        public Task<int> TrySyncBundleTotalFromSlitsAsync(string batchNo, bool forceFromSlits, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<(int RowsRemoved, IReadOnlyList<RemovedSlitRowTraceRef> TraceRefs)> DeletePerSlitOutputRowsForBatchSlitsAsync(
            string batchNo, IReadOnlyList<string> slitNos, CancellationToken cancellationToken) =>
            Task.FromResult((0, (IReadOnlyList<RemovedSlitRowTraceRef>)[]));
        public Task<NdtBundleRecord?> GetLatestPrintedBundleForMillAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(null);
        public Task<bool> HasPrintedBundleForPoAsync(int millNo, string poNumber, CancellationToken cancellationToken) =>
            Task.FromResult(false);
        public Task TrySetPlcCloseMetadataAsync(int engineBatchSequence, int millNo, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<int> MarkManualReviewAsync(string poNumber, int millNo, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<(string BundleNo, int EngineSequence, int PlcTotal)?> TryGetAwaitingPlcReconBatchAsync(
            string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<(string, int, int)?>(null);
        public Task<IReadOnlyList<PlcCsvReconAwaitingBundle>> ListAwaitingPlcReconBatchesAsync(
            string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<PlcCsvReconAwaitingBundle>>([]);
        public Task<PlcCsvReconResult?> TryFinalizePlcReconBundleAsync(
            string bundleNo, int slitSum, int reconWindowMinutes, DateTime utcNow, bool force, CancellationToken cancellationToken) =>
            Task.FromResult<PlcCsvReconResult?>(null);
        public Task<IReadOnlyList<PlcCsvReconResult>> TryFinalizeReadyPlcReconBundlesAsync(
            string poNumber, int millNo, int reconWindowMinutes, DateTime utcNow, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<PlcCsvReconResult>>([]);
        public Task<PlcCsvReconResult?> TryReconcilePlcClosedBundleAsync(
            string poNumber, int millNo, int slitSum, CancellationToken cancellationToken) =>
            Task.FromResult<PlcCsvReconResult?>(null);
        public Task<PlcCsvReconResult?> TryForceFinalizeAwaitingReconOnReopenAsync(
            string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<PlcCsvReconResult?>(null);
    }

    private sealed class SeedBundles(IReadOnlyList<NdtBundleRecord> bundles) : EmptyBundles
    {
        public override Task<IReadOnlyList<NdtBundleRecord>> GetBundlesAsync(CancellationToken cancellationToken) =>
            Task.FromResult(bundles);
    }

    private sealed class EmptyRuntime : INdtBundleRuntimeStateStore
    {
        public Task EnsureInitializedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public int GetBatchOffset(string poNumber, int millNo) => 0;
        public int GetRunningTotal(string poNumber, int millNo) => 0;
        public void ClearRunningTotal(string poNumber, int millNo) { }
        public void ClearOpenAccumulation(string poNumber, int millNo) { }
        public DateTime GetLastActivityUtc(string poNumber, int millNo) => DateTime.MinValue;
        public void ApplySlitContribution(string poNumber, int millNo, int ndtPipes, int threshold, out int totalSoFar) =>
            totalSoFar = 0;
        public BundleCloseAllocation CloseBundle(string poNumber, int millNo, int closedTotalPcs, int threshold) =>
            new(0);
        public void AdvanceOnPoEnd(string poNumber, int millNo, int threshold) { }
        public Task SyncBatchSequencesFromBundlesAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public int GetEngineBatchNo(string poNumber, int millNo) => 0;
        public void SetEngineBatchNo(string poNumber, int millNo, int batchNo) { }
        public Dictionary<string, int> GetSizeCounts(string poNumber, int millNo) => new();
        public void SetSizeCounts(string poNumber, int millNo, IReadOnlyDictionary<string, int> counts) { }
        public InputSlitRecord? GetLastRecord(string poNumber, int millNo) => null;
        public void SetLastRecord(string poNumber, int millNo, InputSlitRecord? record) { }
        public Task SaveAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class NoOpOutput : IBundleOutputWriter
    {
        public Task WriteBundleAsync(
            InputSlitRecord contextRecord,
            int ndtBatchNo,
            int totalNdtPcs,
            CancellationToken cancellationToken,
            Guid? correlationId = null) =>
            Task.CompletedTask;
    }

    private sealed class FormationStub : IFormationChartProvider
    {
        public Task<IReadOnlyDictionary<string, FormationChartEntry>> GetFormationChartAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<string, FormationChartEntry>>(
                new Dictionary<string, FormationChartEntry>(StringComparer.OrdinalIgnoreCase)
                {
                    ["Default"] = new FormationChartEntry { PipeSize = "Default", RequiredNdtPcs = 20 }
                });

        public void InvalidateCache() { }
    }

    private sealed class PipeSizeStub : IPipeSizeProvider
    {
        public Task<IReadOnlyDictionary<string, string>> GetPipeSizeByPoAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<string, string>>(new Dictionary<string, string>());

        public IReadOnlyDictionary<string, string>? TryGetCachedPipeSizes() => null;

        public Task<string?> TryGetPipeSizeForPoAsync(string poNumber, CancellationToken cancellationToken) =>
            Task.FromResult<string?>("Default");
    }

    private sealed class TestOptionsMonitor<T>(T value) : IOptionsMonitor<T> where T : class
    {
        public T CurrentValue { get; } = value;
        public T Get(string? name) => CurrentValue;
        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }
}
