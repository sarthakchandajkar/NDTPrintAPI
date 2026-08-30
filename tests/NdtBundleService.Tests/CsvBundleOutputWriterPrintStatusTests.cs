using Microsoft.Extensions.Logging;
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

    [Fact]
    public async Task WriteBundleAsync_sql_allocate_failure_does_not_print_or_invent_batch()
    {
        var repo = new TrackingBundleRepository();
        var printer = new CountingTagPrinter();
        var mill = new FakeMillSequence { ThrowOnAllocate = true };
        var logger = new CollectingLogger();
        var writer = CreateWriter(repo, printer, logger, mill);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.WriteBundleAsync(SampleRecord, 0, 15, CancellationToken.None));

        Assert.Equal(BundleCloseFailure.AllocateUnavailable, ex.Message);
        Assert.Empty(repo.StatusTransitions);
        Assert.Equal(0, printer.Calls);
        Assert.Equal(0, mill.CurrentSequence);
        Assert.Empty(mill.InsertedBundleNos);
        Assert.DoesNotContain("00000", string.Join('\n', logger.Messages), StringComparison.Ordinal);
        Assert.Contains(BundleCloseFailure.AllocateUnavailable, string.Join('\n', logger.Messages), StringComparison.Ordinal);
    }

    [Fact]
    public async Task WriteBundleAsync_sql_off_sequence_zero_refuses_without_fabricated_batch()
    {
        var repo = new TrackingBundleRepository();
        var printer = new CountingTagPrinter();
        var logger = new CollectingLogger();
        var writer = CreateWriter(repo, printer, logger);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            writer.WriteBundleAsync(SampleRecord, 0, 15, CancellationToken.None));

        Assert.Equal(BundleCloseFailure.AllocateUnavailable, ex.Message);
        Assert.Empty(repo.StatusTransitions);
        Assert.Equal(0, printer.Calls);
        Assert.DoesNotContain("00000", string.Join('\n', logger.Messages), StringComparison.Ordinal);
    }

    private static CsvBundleOutputWriter CreateWriter(
        INdtBundleRepository repo,
        INdtTagPrinter? tagPrinter = null,
        ILogger<CsvBundleOutputWriter>? logger = null,
        IMillSequenceService? millSequence = null)
    {
        var options = Options.Create(new NdtBundleOptions
        {
            EnableBundleSummaryCsvFiles = false,
            OutputBundleFolder = Path.GetTempPath()
        });

        return new CsvBundleOutputWriter(
            options,
            repo,
            NoOpCsvFillService.Instance,
            logger ?? NullLogger<CsvBundleOutputWriter>.Instance,
            tagPrinter,
            millSequence: millSequence);
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
        public Task<IReadOnlyList<NdtBundleRecord>> GetStuckPrintsAsync(TimeSpan olderThan, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
    }

    private sealed class CountingTagPrinter : INdtTagPrinter
    {
        public int Calls { get; private set; }

        public Task<bool> PrintBundleTagAsync(
            InputSlitRecord record,
            int batchNumber,
            int totalNdtPcs,
            bool isReprint,
            CancellationToken cancellationToken = default)
        {
            Calls++;
            return Task.FromResult(true);
        }
    }

    private sealed class FakeMillSequence : IMillSequenceService
    {
        public bool ThrowOnAllocate { get; set; }
        public int CurrentSequence { get; private set; }
        public List<string> InsertedBundleNos { get; } = [];

        public bool IsEnabled => true;

        public Task<(int Sequence, string Formatted)> AllocateAndInsertBundleAsync(
            NdtBundleRecord pending,
            CancellationToken cancellationToken)
        {
            if (ThrowOnAllocate)
                throw new InvalidOperationException("SQL unavailable");

            CurrentSequence++;
            var formatted = NdtBundleSequence.Format(CurrentSequence, pending.MillNo);
            pending.BundleNo = formatted;
            InsertedBundleNos.Add(formatted);
            return Task.FromResult((CurrentSequence, formatted));
        }

        public Task SeedMissingRowsAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<IReadOnlyList<MillSequenceSnapshot>> GetSnapshotsAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<MillSequenceSnapshot>>(Array.Empty<MillSequenceSnapshot>());
        public Task<MillSequenceSnapshot?> GetSnapshotAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<MillSequenceSnapshot?>(null);
        public Task<int> GetLiveMaxSequenceAsync(int millNo, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<int> AllocateNextInTxAsync(
            Microsoft.Data.SqlClient.SqlConnection conn,
            Microsoft.Data.SqlClient.SqlTransaction tx,
            int millNo,
            string updatedBy,
            string reason,
            CancellationToken cancellationToken) =>
            Task.FromResult(0);
        public Task<MillSequenceSetResult> SetCurrentSequenceAsync(
            int millNo, int currentSequence, string reason, string updatedBy, bool forceBelowLiveMax,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();
        public Task<bool> TryRollbackIfHighestInTxAsync(
            Microsoft.Data.SqlClient.SqlConnection conn,
            Microsoft.Data.SqlClient.SqlTransaction tx,
            int millNo,
            int sourceSequence,
            string updatedBy,
            string reason,
            CancellationToken cancellationToken) =>
            Task.FromResult(false);
        public Task EnsureScanDoesNotExceedTableAsync(int millNo, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class CollectingLogger : ILogger<CsvBundleOutputWriter>
    {
        public List<string> Messages { get; } = [];
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Messages.Add(formatter(state, exception));
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
