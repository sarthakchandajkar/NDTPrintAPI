using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class NdtBundleRepositoryReconcileSlitTests : IDisposable
{
    private readonly string _tempDir;

    public NdtBundleRepositoryReconcileSlitTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "NdtBundleReconcileSlitTests_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
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
            // Best-effort cleanup.
        }
    }

    [Fact]
    public async Task UpdateOutputCsvFilesForSlitAsync_MatchesEmptySlitCellToDashKey()
    {
        var batchNo = "012600099";
        var path = Path.Combine(_tempDir, "slit_output.csv");
        await File.WriteAllTextAsync(path,
            "PO Number,Slit No,NDT Pipes,Rejected P,Slit Start Time,Slit Finish Time,Mill No,NDT Short Length Pipe,Rejected Short Length Pipe,NDT Batch No\n" +
            $"1000055673,,12,0,2026-06-17T10:00:00,2026-06-17T11:00:00,1,,,{batchNo}\n");

        var repo = CreateRepository(_tempDir);
        var updated = await repo.UpdateOutputCsvFilesForSlitAsync(batchNo, "—", 18, CancellationToken.None);

        Assert.Equal(1, updated);
        var line = (await File.ReadAllLinesAsync(path))[1];
        var cols = ReconcileCsvParsing.SplitCsvLine(line);
        Assert.Equal("18", cols[2]);
    }

    [Fact]
    public async Task UpdateOutputCsvFilesForSlitAsync_UpdatesQuotedPoRow()
    {
        var batchNo = "012600100";
        var path = Path.Combine(_tempDir, "quoted.csv");
        await File.WriteAllTextAsync(path,
            "PO Number,Slit No,NDT Pipes,Rejected P,Slit Start Time,Slit Finish Time,Mill No,NDT Short Length Pipe,Rejected Short Length Pipe,NDT Batch No\n" +
            $"\"PO,100\",05,9,0,2026-06-17T10:00:00,2026-06-17T11:00:00,2,,,{batchNo}\n");

        var repo = CreateRepository(_tempDir);
        var updated = await repo.UpdateOutputCsvFilesForSlitAsync(batchNo, "05", 14, CancellationToken.None);

        Assert.Equal(1, updated);
        var line = (await File.ReadAllLinesAsync(path))[1];
        var cols = ReconcileCsvParsing.SplitCsvLine(line);
        Assert.Equal("14", cols[2]);
        Assert.Equal("PO,100", cols[0]);
    }

    [Fact]
    public async Task UpdateOutputCsvFilesForSlitAsync_OnlyScansMatchingFilename()
    {
        var batchNo = "1226100100";
        var targetSlit = "2603832_05";
        var targetPath = Path.Combine(_tempDir, $"{targetSlit}_260615_1000059046.csv");
        await File.WriteAllTextAsync(targetPath,
            "PO Number,Slit No,NDT Pipes,Rejected P,Slit Start Time,Slit Finish Time,Mill No,NDT Short Length Pipe,Rejected Short Length Pipe,NDT Batch No\n" +
            $"1000059046,{targetSlit},5,0,2026-06-15T10:00:00,2026-06-15T11:00:00,1,,,{batchNo}\n");

        var otherPath = Path.Combine(_tempDir, "2604061_01_260615_1000059046.csv");
        await File.WriteAllTextAsync(otherPath,
            "PO Number,Slit No,NDT Pipes,Rejected P,Slit Start Time,Slit Finish Time,Mill No,NDT Short Length Pipe,Rejected Short Length Pipe,NDT Batch No\n" +
            $"1000059046,2604061_01,9,0,2026-06-15T10:00:00,2026-06-15T11:00:00,1,,,{batchNo}\n");

        var repo = CreateRepository(_tempDir);
        var updated = await repo.UpdateOutputCsvFilesForSlitAsync(batchNo, targetSlit, 2, CancellationToken.None);

        Assert.Equal(1, updated);
        var targetLine = (await File.ReadAllLinesAsync(targetPath))[1];
        var otherLine = (await File.ReadAllLinesAsync(otherPath))[1];
        Assert.Equal("2", ReconcileCsvParsing.SplitCsvLine(targetLine)[2]);
        Assert.Equal("9", ReconcileCsvParsing.SplitCsvLine(otherLine)[2]);
    }

    [Fact]
    public async Task UpdateOutputCsvFilesForBundleAsync_UpdatesLastRowInLastMatchingFile()
    {
        var batchNo = "1226100103";
        var file1 = Path.Combine(_tempDir, "2603832_04_260615_1000059046.csv");
        var file2 = Path.Combine(_tempDir, "2603832_05_260615_1000059046.csv");
        const string header =
            "PO Number,Slit No,NDT Pipes,Rejected P,Slit Start Time,Slit Finish Time,Mill No,NDT Short Length Pipe,Rejected Short Length Pipe,NDT Batch No\n";
        await File.WriteAllTextAsync(file1, header + $"1000059046,2603832_04,10,0,2026-06-15T10:00:00,2026-06-15T11:00:00,1,,,{batchNo}\n");
        await File.WriteAllTextAsync(file2, header + $"1000059046,2603832_05,20,0,2026-06-15T10:00:00,2026-06-15T11:00:00,1,,,{batchNo}\n");

        var repo = CreateRepository(_tempDir);
        var updated = await repo.UpdateOutputCsvFilesForBundleAsync(batchNo, 33, CancellationToken.None);

        Assert.Equal(1, updated);
        var line1 = (await File.ReadAllLinesAsync(file1))[1];
        var line2 = (await File.ReadAllLinesAsync(file2))[1];
        Assert.Equal("10", ReconcileCsvParsing.SplitCsvLine(line1)[2]);
        Assert.Equal("23", ReconcileCsvParsing.SplitCsvLine(line2)[2]);
    }

    [Fact]
    public async Task TrySyncBundleTotalFromSlitsAsync_UpdatesSummaryCsvWhenStoredTotalIsZero()
    {
        var batchNo = "1226300099";
        var summaryFolder = Path.Combine(_tempDir, "summary");
        Directory.CreateDirectory(summaryFolder);
        var summaryPath = Path.Combine(summaryFolder, $"NDT_Bundle_{batchNo}.csv");
        await File.WriteAllTextAsync(summaryPath,
            "PO Number,Slit No,NDT Pipes,Rejected P,Slit Start Time,Slit Finish Time,Mill No,NDT Short Length Pipe,Rejected Short Length Pipe,NDT Batch No\n" +
            $"1000055673,01,0,0,2026-06-17T10:00:00,2026-06-17T11:00:00,3,,,{batchNo}\n");

        var slitPath = Path.Combine(_tempDir, "slit_output.csv");
        await File.WriteAllTextAsync(slitPath,
            "PO Number,Slit No,NDT Pipes,Rejected P,Slit Start Time,Slit Finish Time,Mill No,NDT Short Length Pipe,Rejected Short Length Pipe,NDT Batch No\n" +
            $"1000055673,01,15,0,2026-06-17T10:00:00,2026-06-17T11:00:00,3,,,{batchNo}\n");

        var repo = CreateRepository(_tempDir, summaryFolder);
        var synced = await repo.TrySyncBundleTotalFromSlitsAsync(batchNo, forceFromSlits: false, CancellationToken.None);

        Assert.Equal(15, synced);
        var line = (await File.ReadAllLinesAsync(summaryPath))[1];
        var cols = ReconcileCsvParsing.SplitCsvLine(line);
        Assert.Equal("15", cols[2]);
    }

    private static NdtBundleRepository CreateRepository(string outputFolder, string? summaryFolder = null)
    {
        var options = Options.Create(new NdtBundleOptions
        {
            OutputBundleFolder = outputFolder,
            BundleSummaryOutputFolder = summaryFolder ?? outputFolder,
            UseSqlServerForBundles = false
        });
        var monitor = new TestOptionsMonitor<NdtBundleOptions>(options.Value);
        return new NdtBundleRepository(monitor, new NoOpSqlWriteTracker(), NullLogger<NdtBundleRepository>.Instance);
    }

    private sealed class TestOptionsMonitor<T> : IOptionsMonitor<T>
    {
        public TestOptionsMonitor(T value) => CurrentValue = value;
        public T CurrentValue { get; private set; }
        public T Get(string? name) => CurrentValue;
        public IDisposable OnChange(Action<T, string?> listener) => NullDisposable.Instance;
        public void Set(T value) => CurrentValue = value;
    }

    private sealed class NoOpSqlWriteTracker : ISqlTraceabilityWriteTracker
    {
        public void RecordFailure(string operation, string error, string? detail = null) { }
        public void RecordSuccess(string operation, string? detail = null) { }
        public IReadOnlyList<SqlTraceabilityWriteResult> GetRecentResults() => Array.Empty<SqlTraceabilityWriteResult>();
    }

    private sealed class NullDisposable : IDisposable
    {
        public static readonly NullDisposable Instance = new();
        public void Dispose() { }
    }
}
