using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.FileBasedPoChange;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class WipBundleReconciliationTests : IDisposable
{
    private readonly string _wipFolder;
    private readonly FileBasedPoChangeQueue _queue = new();

    public WipBundleReconciliationTests()
    {
        _wipFolder = Path.Combine(Path.GetTempPath(), "wip-recon-" + Guid.NewGuid().ToString("N"));
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
    public async Task ReconcileAsync_skips_non_file_mill()
    {
        WriteWipFile("WIP_02_1000057001_010101_120000.csv", DateTime.UtcNow.AddMinutes(-10));
        WriteWipFile("WIP_02_1000057002_010102_120000.csv", DateTime.UtcNow.AddMinutes(-5));

        var sut = CreateService(
            mills:
            [
                new MillConfig { MillNo = 1, PoEndSource = "Plc" },
                new MillConfig { MillNo = 2, PoEndSource = "Plc" },
                new MillConfig { MillNo = 3, PoEndSource = "Plc" },
                new MillConfig { MillNo = 4, PoEndSource = "File" }
            ],
            runningPoByMill: new Dictionary<int, string> { [2] = "1000057001" },
            hasPrintedBundleForPo: (_, _) => Task.FromResult(false));

        var enqueued = await sut.ReconcileAsync(CancellationToken.None);

        Assert.Equal(0, enqueued);
        Assert.True(_queue.TryEnqueue(new FileBasedPoChangeRequest { MillNo = 2, NewPo = "probe" }));
    }

    [Fact]
    public async Task ReconcileAsync_enqueues_missed_transition_for_file_mill()
    {
        WriteWipFile("WIP_04_1000057001_010101_100000.csv", DateTime.UtcNow.AddMinutes(-20));
        WriteWipFile("WIP_04_1000057002_010102_110000.csv", DateTime.UtcNow.AddMinutes(-10));

        var sut = CreateService(
            mills:
            [
                new MillConfig { MillNo = 1, PoEndSource = "Plc" },
                new MillConfig { MillNo = 2, PoEndSource = "Plc" },
                new MillConfig { MillNo = 3, PoEndSource = "Plc" },
                new MillConfig { MillNo = 4, PoEndSource = "File" }
            ],
            runningPoByMill: new Dictionary<int, string> { [4] = "1000057001" },
            hasPrintedBundleForPo: (_, _) => Task.FromResult(false));

        var enqueued = await sut.ReconcileAsync(CancellationToken.None);

        Assert.Equal(1, enqueued);
        Assert.False(_queue.TryEnqueue(new FileBasedPoChangeRequest { MillNo = 4, NewPo = "probe" }));
        _queue.MarkCompleted(4);
    }

    [Fact]
    public async Task ReconcileAsync_skips_when_durable_bundle_exists_for_new_po()
    {
        WriteWipFile("WIP_04_1000057001_010101_100000.csv", DateTime.UtcNow.AddMinutes(-20));
        WriteWipFile("WIP_04_1000057002_010102_110000.csv", DateTime.UtcNow.AddMinutes(-10));

        var sut = CreateService(
            mills:
            [
                new MillConfig { MillNo = 1, PoEndSource = "Plc" },
                new MillConfig { MillNo = 2, PoEndSource = "Plc" },
                new MillConfig { MillNo = 3, PoEndSource = "Plc" },
                new MillConfig { MillNo = 4, PoEndSource = "File" }
            ],
            runningPoByMill: new Dictionary<int, string> { [4] = "1000057001" },
            hasPrintedBundleForPo: (_, po) => Task.FromResult(po == "1000057002"));

        var enqueued = await sut.ReconcileAsync(CancellationToken.None);

        Assert.Equal(0, enqueued);
    }

    [Fact]
    public async Task ReconcileAsync_skips_when_running_po_already_matches_new_po()
    {
        WriteWipFile("WIP_04_1000057001_010101_100000.csv", DateTime.UtcNow.AddMinutes(-20));
        WriteWipFile("WIP_04_1000057002_010102_110000.csv", DateTime.UtcNow.AddMinutes(-10));

        var sut = CreateService(
            mills:
            [
                new MillConfig { MillNo = 1, PoEndSource = "Plc" },
                new MillConfig { MillNo = 2, PoEndSource = "Plc" },
                new MillConfig { MillNo = 3, PoEndSource = "Plc" },
                new MillConfig { MillNo = 4, PoEndSource = "File" }
            ],
            runningPoByMill: new Dictionary<int, string> { [4] = "1000057002" },
            hasPrintedBundleForPo: (_, _) => Task.FromResult(false));

        var enqueued = await sut.ReconcileAsync(CancellationToken.None);

        Assert.Equal(0, enqueued);
    }

    [Fact]
    public async Task ReconcileAsync_respects_queue_dedup_without_duplicate_enqueue()
    {
        WriteWipFile("WIP_04_1000057001_010101_100000.csv", DateTime.UtcNow.AddMinutes(-20));
        WriteWipFile("WIP_04_1000057002_010102_110000.csv", DateTime.UtcNow.AddMinutes(-10));

        Assert.True(_queue.TryEnqueue(new FileBasedPoChangeRequest
        {
            MillNo = 4,
            EndedPo = "1000057001",
            NewPo = "1000057002",
            WipFileName = "WIP_04_1000057002_010102_110000.csv"
        }));

        var sut = CreateService(
            mills:
            [
                new MillConfig { MillNo = 1, PoEndSource = "Plc" },
                new MillConfig { MillNo = 2, PoEndSource = "Plc" },
                new MillConfig { MillNo = 3, PoEndSource = "Plc" },
                new MillConfig { MillNo = 4, PoEndSource = "File" }
            ],
            runningPoByMill: new Dictionary<int, string> { [4] = "1000057001" },
            hasPrintedBundleForPo: (_, _) => Task.FromResult(false));

        var enqueued = await sut.ReconcileAsync(CancellationToken.None);

        Assert.Equal(0, enqueued);
    }

    private WipBundleReconciliationService CreateService(
        IReadOnlyList<MillConfig> mills,
        IReadOnlyDictionary<int, string> runningPoByMill,
        Func<int, string, Task<bool>> hasPrintedBundleForPo)
    {
        var millConfigs = mills.Count > 0
            ? mills.ToList()
            :
            [
                new MillConfig { MillNo = 1, PoEndSource = "Plc" },
                new MillConfig { MillNo = 2, PoEndSource = "Plc" },
                new MillConfig { MillNo = 3, PoEndSource = "Plc" },
                new MillConfig { MillNo = 4, PoEndSource = "File" }
            ];

        var options = Options.Create(new NdtBundleOptions
        {
            UseSqlServerForBundles = true,
            ConnectionString = "Server=.;Database=test;",
            MillSlitLive = new MillSlitLiveOptions
            {
                WipBundleFolder = _wipFolder,
                WipBundleAcceptedFolder = _wipFolder
            },
            FgBundleFolder = _wipFolder,
            FgBundleAcceptedFolder = _wipFolder,
            PlcHandshake = new PlcHandshakeOptions { Mills = millConfigs }
        });

        return new WipBundleReconciliationService(
            options,
            new StubBundleRepository(hasPrintedBundleForPo),
            new StubWipRunningPoProvider(runningPoByMill),
            _queue,
            NullLogger<WipBundleReconciliationService>.Instance);
    }

    private void WriteWipFile(string fileName, DateTime stampUtc)
    {
        var path = Path.Combine(_wipFolder, fileName);
        File.WriteAllText(path, "wip");
        File.SetLastWriteTimeUtc(path, stampUtc);
    }

    private sealed class StubBundleRepository(Func<int, string, Task<bool>> hasPrintedBundleForPo) : INdtBundleRepository
    {
        public Task<bool> HasPrintedBundleForPoAsync(int millNo, string poNumber, CancellationToken cancellationToken) =>
            hasPrintedBundleForPo(millNo, poNumber);

        public Task<int> MarkManualReviewAsync(string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult(0);

        public Task<NdtBundleRecord?> GetLatestPrintedBundleForMillAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(null);

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
        public Task<int> UpdateOutputCsvFilesForBundleAsync(string batchNo, int newPipes, CancellationToken cancellationToken) =>
            Task.FromResult(0);
        public Task<IReadOnlyList<(string SlitNo, int NdtPipes)>> GetSlitsForBatchAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string SlitNo, int NdtPipes)>>(Array.Empty<(string, int)>());
        public Task<int> UpdateOutputCsvFilesForSlitAsync(string batchNo, string slitNo, int newPipes, CancellationToken cancellationToken) =>
            Task.FromResult(0);
        public Task UpdateBundleTotalInDatabaseAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.CompletedTask;
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

    private sealed class StubWipRunningPoProvider(IReadOnlyDictionary<int, string> runningPoByMill) : IWipBundleRunningPoProvider
    {
        public Task<string?> TryGetRunningPoForMillAsync(int millNo, CancellationToken cancellationToken)
        {
            runningPoByMill.TryGetValue(millNo, out var po);
            return Task.FromResult<string?>(po);
        }

        public bool IsWaitingForNewWipAfterPoEnd(int millNo) => false;
        public bool ResumeRunningWipForMill(int millNo) => false;
        public bool TrySetRunningPoFromWipFile(int millNo, string newPo, DateTime wipStampUtc, string wipFileName) => true;
        public void NotifyPoEndForMill(int millNo, string endedPo) { }
    }
}
