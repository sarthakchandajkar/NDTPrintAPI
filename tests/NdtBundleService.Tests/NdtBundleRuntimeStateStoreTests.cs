using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class NdtBundleRuntimeStateStoreTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _statePath;

    public NdtBundleRuntimeStateStoreTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "NdtBundleRuntimeStateTests_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        _statePath = Path.Combine(_tempDir, "NdtBundleRuntimeState.json");
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            // Best-effort cleanup for temp test folder.
        }
    }

    [Fact]
    public async Task EnsureInitialized_PartialSlotSurvivesHigherMillFloorOnRestart()
    {
        WriteStateFile(new
        {
            version = 1,
            updatedUtc = "2026-06-10T10:00:00Z",
            millMaxSequence = new Dictionary<string, int> { ["1"] = 36 },
            mills = new Dictionary<string, object>
            {
                ["PO-100|1"] = new
                {
                    poNumber = "PO-100",
                    millNo = 1,
                    batchOffset = 34,
                    runningTotal = 8,
                    engineBatchNo = 34,
                    sizeCounts = new Dictionary<string, int> { ["Default"] = 8 },
                    lastActivityUtc = "2026-06-10T10:00:00Z"
                }
            }
        });

        var store = CreateStore(
            activePoByMill: new Dictionary<int, string> { [1] = "PO-100" },
            gracePeriodDays: 14);

        await store.EnsureInitializedAsync(CancellationToken.None);

        store.ApplySlitContribution("PO-100", 1, ndtPipes: 0, threshold: 15, out var batchNumber, out var totalSoFar);

        Assert.Equal(34, store.GetBatchOffset("PO-100", 1));
        Assert.Equal(8, totalSoFar);
        Assert.Equal(35, batchNumber);
    }

    [Fact]
    public async Task EnsureInitialized_PrunesOldIdleSlotButKeepsActivePartialSlot()
    {
        WriteStateFile(new
        {
            version = 1,
            updatedUtc = "2026-05-01T10:00:00Z",
            millMaxSequence = new Dictionary<string, int> { ["1"] = 36 },
            mills = new Dictionary<string, object>
            {
                ["PO-ACTIVE|1"] = new
                {
                    poNumber = "PO-ACTIVE",
                    millNo = 1,
                    batchOffset = 34,
                    runningTotal = 8,
                    engineBatchNo = 34,
                    sizeCounts = new Dictionary<string, int> { ["Default"] = 8 },
                    lastActivityUtc = "2026-06-10T10:00:00Z"
                },
                ["PO-OLD|1"] = new
                {
                    poNumber = "PO-OLD",
                    millNo = 1,
                    batchOffset = 20,
                    runningTotal = 0,
                    engineBatchNo = 20,
                    sizeCounts = new Dictionary<string, int>(),
                    lastActivityUtc = "2026-04-01T10:00:00Z"
                }
            }
        });

        var store = CreateStore(
            activePoByMill: new Dictionary<int, string> { [1] = "PO-ACTIVE" },
            gracePeriodDays: 14);

        await store.EnsureInitializedAsync(CancellationToken.None);

        Assert.Equal(8, store.GetRunningTotal("PO-ACTIVE", 1));

        var savedJson = await File.ReadAllTextAsync(_statePath);
        Assert.Contains("PO-ACTIVE", savedJson, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("PO-OLD", savedJson, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ApplySlitContribution_DoesNotBurnSequenceAtThreshold_AllocationIsCloseOnly()
    {
        var store = CreateStore(new Dictionary<int, string>(), gracePeriodDays: 14);
        await store.EnsureInitializedAsync(CancellationToken.None);

        store.ApplySlitContribution("PO-200", 2, ndtPipes: 10, threshold: 15, out var batch1, out var total1);
        Assert.Equal(1, batch1);
        Assert.Equal(10, total1);

        store.ApplySlitContribution("PO-200", 2, ndtPipes: 5, threshold: 15, out var batch2, out var total2);
        Assert.Equal(1, batch2);
        Assert.Equal(15, total2);
        // Provisional stamp held; BatchOffset stays 0 until CloseBundle
        Assert.Equal(0, store.GetBatchOffset("PO-200", 2));
        Assert.Equal(15, store.GetRunningTotal("PO-200", 2));

        store.ApplySlitContribution("PO-200", 2, ndtPipes: 0, threshold: 15, out var batch3, out _);
        Assert.Equal(1, batch3);

        var closed = store.CloseBundle("PO-200", 2, closedTotalPcs: 15, threshold: 15);
        Assert.Equal(1, closed.FinalSequence);
        Assert.Equal(1, closed.ProvisionalSequence);
        Assert.Equal(1, store.GetBatchOffset("PO-200", 2));
        Assert.Equal(0, store.GetRunningTotal("PO-200", 2));
    }

    [Fact]
    public async Task SaveAsync_PersistsLastActivityUtc()
    {
        var store = CreateStore(new Dictionary<int, string>(), gracePeriodDays: 14);
        await store.EnsureInitializedAsync(CancellationToken.None);

        store.ApplySlitContribution("PO-300", 3, ndtPipes: 4, threshold: 15, out _, out _);
        await store.SaveAsync(CancellationToken.None);

        Assert.True(File.Exists(_statePath));
        var json = await File.ReadAllTextAsync(_statePath);
        Assert.Contains("lastActivityUtc", json, StringComparison.OrdinalIgnoreCase);
    }

    private NdtBundleRuntimeStateStore CreateStore(
        IReadOnlyDictionary<int, string> activePoByMill,
        int gracePeriodDays)
    {
        var options = new TestOptionsMonitor<NdtBundleOptions>(new NdtBundleOptions
        {
            EnableNdtBundleRuntimeStatePersistence = true,
            NdtBundleRuntimeStateFile = _statePath,
            OutputBundleFolder = _tempDir,
            InitialMillBatchNumbers = new Dictionary<string, string>(),
            RuntimeStatePruning = new RuntimeStatePruningOptions
            {
                Enabled = true,
                RunOnStartup = true,
                GracePeriodDays = gracePeriodDays
            }
        });

        return new NdtBundleRuntimeStateStore(
            options,
            new EmptyBundleRepository(),
            new FixedActivePoPerMillService(activePoByMill),
            NullLogger<NdtBundleRuntimeStateStore>.Instance);
    }

    private void WriteStateFile(object payload)
    {
        var json = System.Text.Json.JsonSerializer.Serialize(payload, new System.Text.Json.JsonSerializerOptions
        {
            WriteIndented = true,
            PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase
        });
        File.WriteAllText(_statePath, json);
    }

    private sealed class TestOptionsMonitor<T> : IOptionsMonitor<T> where T : class
    {
        public TestOptionsMonitor(T value) => CurrentValue = value;

        public T CurrentValue { get; }

        public T Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }

    private sealed class EmptyBundleRepository : INdtBundleRepository
    {
        public Task RecordBundleAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RecordBundlePendingPrintAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task UpdateBundlePrintStatusAsync(string bundleNo, string printStatus, string? printError, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<IReadOnlyList<NdtBundleRecord>> GetStuckPrintsAsync(TimeSpan olderThan, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
        public Task<IReadOnlyList<NdtBundleRecord>> GetBundlesAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
        public Task<NdtBundleRecord?> GetByBatchNoAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(null);
        public Task<NdtBundleRecord?> GetLatestPrintedBundleForMillAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(null);
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
        public Task UpdateBundlePipesAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<int> UpdateOutputCsvFilesForBundleAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<IReadOnlyList<(string SlitNo, int NdtPipes)>> GetSlitsForBatchAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string SlitNo, int NdtPipes)>>(Array.Empty<(string, int)>());
        public Task<int> UpdateOutputCsvFilesForSlitAsync(string batchNo, string slitNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task UpdateBundleTotalInDatabaseAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<bool> UpdateBundleSummaryCsvAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) =>
            Task.FromResult(false);
        public Task<int> TrySyncBundleTotalFromSlitsAsync(string batchNo, bool forceFromSlits, CancellationToken cancellationToken) =>
            Task.FromResult(0);
        public Task<(int RowsRemoved, IReadOnlyList<RemovedSlitRowTraceRef> TraceRefs)> DeletePerSlitOutputRowsForBatchSlitsAsync(
            string batchNo,
            IReadOnlyList<string> slitNumbers,
            CancellationToken cancellationToken) =>
            Task.FromResult<(int, IReadOnlyList<RemovedSlitRowTraceRef>)>((0, Array.Empty<RemovedSlitRowTraceRef>()));
    }

    private sealed class FixedActivePoPerMillService(IReadOnlyDictionary<int, string> active) : IActivePoPerMillService
    {
        public Task<IReadOnlyDictionary<int, string>> GetLatestPoByMillAsync(CancellationToken cancellationToken) =>
            Task.FromResult(active);

        public IReadOnlyList<string> GetInputSlitReadFolderPaths() => Array.Empty<string>();
    }
}
