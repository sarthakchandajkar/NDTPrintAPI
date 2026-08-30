using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>Mill-wide close allocator under fill-to-target (no provisional stamp).</summary>
public sealed class MillWideAllocatorAndPoEndIdempotencyTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _statePath;

    public MillWideAllocatorAndPoEndIdempotencyTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "MillWideAlloc_" + Guid.NewGuid().ToString("N"));
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
            // best-effort
        }
    }

    [Fact]
    public async Task Overlapping_POs_close_order_yields_unique_mill_wide_sequences()
    {
        var store = CreateStore();
        await store.EnsureInitializedAsync(CancellationToken.None);

        store.SetSizeCounts("1000060364", 1, new Dictionary<string, int> { ["Default"] = 5 });
        var b1 = store.CloseBundle("1000060363", 1, 15, 15);
        Assert.Equal(0, b1.FinalSequence);
        var b2 = store.CloseBundle("1000060363", 1, 15, 15);
        Assert.Equal(0, b2.FinalSequence);
        var aFlush = store.CloseBundle("1000060364", 1, 5, 15);
        Assert.Equal(0, aFlush.FinalSequence);
        Assert.Equal(0, store.GetRunningTotal("1000060363", 1));
        Assert.Equal(0, store.GetRunningTotal("1000060364", 1));
    }

    [Fact]
    public async Task ApplySlitContribution_does_not_burn_sequence_without_close()
    {
        var store = CreateStore();
        await store.EnsureInitializedAsync(CancellationToken.None);

        store.ApplySlitContribution("PO-A", 1, 15, threshold: 15, out var total1);
        Assert.Equal(15, total1);
        store.ApplySlitContribution("PO-A", 1, 15, threshold: 15, out var total2);
        Assert.Equal(30, total2);

        var close = store.CloseBundle("PO-A", 1, 20, 15);
        Assert.Equal(0, close.FinalSequence);
    }

    [Fact]
    public void CsvFillLogic_revision_uses_delta()
    {
        var (filled, _, _, _) = CsvFillLogic.ApplyFilledDelta(49, 56, 46 - 56, 20);
        Assert.Equal(46, filled);
    }

    private NdtBundleRuntimeStateStore CreateStore()
    {
        var options = new NdtBundleOptions
        {
            EnableNdtBundleRuntimeStatePersistence = true,
            NdtBundleRuntimeStateFile = _statePath,
            UseSqlServerForBundles = false,
            InitialMillBatchNumbers = new Dictionary<string, string>
            {
                ["1"] = "1226100000"
            }
        };
        return new NdtBundleRuntimeStateStore(
            new TestOptionsMonitor<NdtBundleOptions>(options),
            new EmptyBundleRepo(),
            new EmptyActivePo(),
            TestMillOwnership.Monolith(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<NdtBundleRuntimeStateStore>.Instance);
    }

    private sealed class TestOptionsMonitor<T> : Microsoft.Extensions.Options.IOptionsMonitor<T>
    {
        public TestOptionsMonitor(T value) => CurrentValue = value;
        public T CurrentValue { get; }
        public T Get(string? name) => CurrentValue;
        public IDisposable OnChange(Action<T, string?> listener) => NullDisposable.Instance;
        private sealed class NullDisposable : IDisposable
        {
            public static readonly NullDisposable Instance = new();
            public void Dispose() { }
        }
    }

    private sealed class EmptyBundleRepo : INdtBundleRepository
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
        public Task UpdateBundlePipesAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<int> UpdateOutputCsvFilesForBundleAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<IReadOnlyList<(string SlitNo, int NdtPipes)>> GetSlitsForBatchAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string, int)>>(Array.Empty<(string, int)>());
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
        public Task TrySetPlcCloseMetadataAsync(int engineBatchSequence, int millNo, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<(string BundleNo, int EngineSequence, int PlcTotal)?> TryGetAwaitingPlcReconBatchAsync(
            string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<(string, int, int)?>(null);
        public Task<IReadOnlyList<PlcCsvReconAwaitingBundle>> ListAwaitingPlcReconBatchesAsync(
            string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<PlcCsvReconAwaitingBundle>>(Array.Empty<PlcCsvReconAwaitingBundle>());
        public Task<PlcCsvReconResult?> TryReconcilePlcClosedBundleAsync(
            string poNumber, int millNo, int slitSum, CancellationToken cancellationToken) =>
            Task.FromResult<PlcCsvReconResult?>(null);
        public Task<PlcCsvReconResult?> TryFinalizePlcReconBundleAsync(
            string bundleNo, int slitSum, int reconWindowMinutes, DateTime utcNow, bool force, CancellationToken cancellationToken) =>
            Task.FromResult<PlcCsvReconResult?>(null);
        public Task<IReadOnlyList<PlcCsvReconResult>> TryFinalizeReadyPlcReconBundlesAsync(
            string poNumber, int millNo, int reconWindowMinutes, DateTime utcNow, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<PlcCsvReconResult>>(Array.Empty<PlcCsvReconResult>());
        public Task<PlcCsvReconResult?> TryForceFinalizeAwaitingReconOnReopenAsync(
            string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<PlcCsvReconResult?>(null);
    }

    private sealed class EmptyActivePo : IActivePoPerMillService
    {
        public Task<IReadOnlyDictionary<int, string>> GetLatestPoByMillAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<int, string>>(new Dictionary<int, string>());

        public IReadOnlyList<string> GetInputSlitReadFolderPaths() => Array.Empty<string>();
    }
}
