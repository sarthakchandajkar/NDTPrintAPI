using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class CsvBundleOutputWriterPrintStatusTests
{
    private static readonly InputSlitRecord SampleRecord = new()
    {
        PoNumber = "1000057001",
        SlitNo = "1",
        NdtPipes = 15,
        RejectedPipes = 0,
        MillNo = 2,
        SlitStartTime = DateTime.UtcNow.AddHours(-1),
        SlitFinishTime = DateTime.UtcNow
    };

    [Fact]
    public async Task WriteBundleAsync_print_success_sets_pending_then_printed()
    {
        var repo = new TrackingBundleRepository();
        var printer = new StubTagPrinter(returnsSuccess: true);
        var writer = CreateWriter(repo, printer);

        await writer.WriteBundleAsync(SampleRecord, 42, 15, CancellationToken.None, Guid.NewGuid());

        Assert.Equal([BundlePrintStatus.Pending, BundlePrintStatus.Printed], repo.StatusTransitions);
        Assert.Null(repo.LastError);
    }

    [Fact]
    public async Task WriteBundleAsync_print_returns_false_sets_print_failed()
    {
        var repo = new TrackingBundleRepository();
        var printer = new StubTagPrinter(returnsSuccess: false);
        var writer = CreateWriter(repo, printer);

        await writer.WriteBundleAsync(SampleRecord, 42, 15, CancellationToken.None, Guid.NewGuid());

        Assert.Equal([BundlePrintStatus.Pending, BundlePrintStatus.PrintFailed], repo.StatusTransitions);
        Assert.Contains("returned false", repo.LastError ?? string.Empty, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task WriteBundleAsync_print_throws_sets_print_failed_without_propagating()
    {
        var repo = new TrackingBundleRepository();
        var printer = new StubTagPrinter(throwsException: true);
        var writer = CreateWriter(repo, printer);

        var ex = await Record.ExceptionAsync(() =>
            writer.WriteBundleAsync(SampleRecord, 42, 15, CancellationToken.None, Guid.NewGuid()));

        Assert.Null(ex);
        Assert.Equal([BundlePrintStatus.Pending, BundlePrintStatus.PrintFailed], repo.StatusTransitions);
        Assert.Equal("printer offline", repo.LastError);
    }

    [Fact]
    public async Task WriteBundleAsync_no_printer_sets_pending_then_printed()
    {
        var repo = new TrackingBundleRepository();
        var writer = CreateWriter(repo, tagPrinter: null);

        await writer.WriteBundleAsync(SampleRecord, 42, 15, CancellationToken.None);

        Assert.Equal([BundlePrintStatus.Pending, BundlePrintStatus.Printed], repo.StatusTransitions);
    }

    private static CsvBundleOutputWriter CreateWriter(
        INdtBundleRepository repo,
        INdtTagPrinter? tagPrinter = null)
    {
        var options = Options.Create(new NdtBundleOptions
        {
            EnableBundleSummaryCsvFiles = false,
            OutputBundleFolder = Path.GetTempPath()
        });

        return new CsvBundleOutputWriter(
            options,
            repo,
            NullLogger<CsvBundleOutputWriter>.Instance,
            tagPrinter);
    }

    private sealed class TrackingBundleRepository : INdtBundleRepository
    {
        public List<string> StatusTransitions { get; } = [];
        public string? LastError { get; private set; }

        public Task RecordBundlePendingPrintAsync(NdtBundleRecord record, CancellationToken cancellationToken)
        {
            StatusTransitions.Add(BundlePrintStatus.Pending);
            return Task.CompletedTask;
        }

        public Task UpdateBundlePrintStatusAsync(
            string bundleNo,
            string printStatus,
            string? printError,
            CancellationToken cancellationToken)
        {
            StatusTransitions.Add(printStatus);
            LastError = printError;
            return Task.CompletedTask;
        }

        public Task RecordBundleAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
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
        public Task<NdtBundleRecord?> GetLatestPrintedBundleForMillAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(null);
        public Task<bool> HasPrintedBundleForPoAsync(int millNo, string poNumber, CancellationToken cancellationToken) =>
            Task.FromResult(false);
        public Task<int> MarkManualReviewAsync(string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult(0);
        public Task<IReadOnlyList<NdtBundleRecord>> GetStuckPrintsAsync(TimeSpan olderThan, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
    }

    private sealed class StubTagPrinter(bool returnsSuccess = true, bool throwsException = false) : INdtTagPrinter
    {
        public Task<bool> PrintBundleTagAsync(
            InputSlitRecord record,
            int batchNumber,
            int totalNdtPcs,
            bool isReprint,
            CancellationToken cancellationToken = default)
        {
            if (throwsException)
                throw new InvalidOperationException("printer offline");

            return Task.FromResult(returnsSuccess);
        }
    }
}
