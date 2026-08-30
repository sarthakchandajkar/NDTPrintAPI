using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class UploadNdtBundleFileServiceTests : IDisposable
{
    private readonly string _root;

    public UploadNdtBundleFileServiceTests()
    {
        _root = Path.Combine(Path.GetTempPath(), "ndt-upload-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path.Combine(_root, "process"));
        Directory.CreateDirectory(Path.Combine(_root, "upload"));
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_root))
                Directory.Delete(_root, recursive: true);
        }
        catch
        {
            // best-effort
        }
    }

    [Fact]
    public async Task GenerateForBatchAsync_writes_one_row_using_revisual_ok()
    {
        WriteProcessCsv("1226100001", po: "1000057001", ndtPcs: 50, ok: 47);
        WriteProcessCsv("1226100002", po: "1000057002", ndtPcs: 40, ok: 39);

        var sut = CreateSut();
        var result = await sut.GenerateForBatchAsync("1226100001", CancellationToken.None);

        Assert.Equal(1, result.RowCount);
        Assert.Equal("1226100001", result.NdtBatchNo);
        Assert.True(File.Exists(result.FilePath));
        var lines = await File.ReadAllLinesAsync(result.FilePath);
        Assert.Equal(2, lines.Length);
        Assert.Contains("1226100001", lines[1], StringComparison.Ordinal);
        Assert.Contains("47", lines[1], StringComparison.Ordinal);
        Assert.DoesNotContain("1226100002", lines[1], StringComparison.Ordinal);
        Assert.DoesNotContain("1000057002", lines[1], StringComparison.Ordinal);
    }

    [Fact]
    public async Task GenerateForBatchAsync_requires_revisual_process_csv()
    {
        var sut = CreateSut();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => sut.GenerateForBatchAsync("1226100001", CancellationToken.None));
        Assert.Equal(UploadNdtBundleFileService.RevisualRequiredMessage, ex.Message);
        Assert.Empty(Directory.GetFiles(Path.Combine(_root, "upload")));
    }

    private UploadNdtBundleFileService CreateSut()
    {
        var options = Options.Create(new NdtBundleOptions
        {
            NdtProcessOutputFolder = Path.Combine(_root, "process"),
            UploadNdtBundleFilesFolder = Path.Combine(_root, "upload")
        });
        return new UploadNdtBundleFileService(
            options,
            new StubBundleRepository(),
            new NoOpTraceability(),
            NullLogger<UploadNdtBundleFileService>.Instance);
    }

    private void WriteProcessCsv(string batch, string po, int ndtPcs, int ok)
    {
        var path = Path.Combine(_root, "process", $"NDT_process_{po}_{batch}.csv");
        File.WriteAllLines(path,
        [
            "PO Number, NDT BATCH NO, NDT Pcs, OK, Visual Reject, Hydrotest Reject, Re visual Reject, Bundle Start, Bundle End",
            $"{po}, {batch}, {ndtPcs}, {ok}, 1, 1, 1, 01.01.2026 10:00:00, 01.01.2026 12:00:00"
        ]);
    }

    private sealed class StubBundleRepository : INdtBundleRepository
    {
        public Task<NdtBundleRecord?> GetByBatchNoAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(new NdtBundleRecord
            {
                BundleNo = batchNo,
                PoNumber = "1000057001",
                MillNo = 1,
                SlitNo = "2603832_05",
                TotalNdtPcs = 47
            });

        public Task RecordBundleAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RecordBundlePendingPrintAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task UpdateBundlePrintStatusAsync(string bundleNo, string printStatus, string? printError, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<IReadOnlyList<NdtBundleRecord>> GetBundlesAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
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
        public Task TrySetPlcCloseMetadataAsync(int engineBatchSequence, int millNo, CancellationToken cancellationToken) => Task.CompletedTask;
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

    private sealed class NoOpTraceability : ITraceabilityRepository
    {
        public Task RecordUploadBundleRowsAsync(string generatedFile, IReadOnlyList<UploadBundleRow> rows, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task RecordInputSlitRowsAsync(string sourceFile, IReadOnlyList<(InputSlitRecord Record, int SourceRowNumber)> rows, CancellationToken cancellationToken, DateTime? sourceLastWriteTimeUtc = null) => Task.CompletedTask;
        public Task<bool> IsInputSlitFileVersionImportedAsync(string sourceFileFullPath, DateTime fileLastWriteTimeUtc, CancellationToken cancellationToken) => Task.FromResult(false);
        public Task<bool> IsInputSlitFileSeenAsync(string sourceFileFullPath, DateTime fileLastWriteTimeUtc, CancellationToken cancellationToken) => Task.FromResult(false);
        public Task MarkInputSlitFileSeenAsync(string sourceFileFullPath, DateTime fileLastWriteTimeUtc, string reason, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<OutputSlitBatchCorrectionResult> UpdateOutputSlitBatchNoAsync(string poNumber, int millNo, string oldBatchNo, string newBatchNo, CancellationToken cancellationToken) =>
            Task.FromResult(OutputSlitBatchCorrectionResult.NoOp);
        public Task<string?> TryGetExistingOutputSlitBatchAsync(string sourceFileFullPath, string poNumber, int millNo, CancellationToken cancellationToken) => Task.FromResult<string?>(null);
        public Task<IReadOnlyList<string>> GetSapFrozenSourceFilesAsync(IReadOnlyList<string> sourceFiles, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());
        public Task RecordOutputSlitRowsAsync(string sourceFile, IReadOnlyList<(InputSlitRecord Record, string NdtBatchNo, int SourceRowNumber)> rows, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RecordManualStationRunAsync(string poNumber, string ndtBatchNo, int ndtPcs, int okPcs, int rejectPcs, string workStation, DateTime start, DateTime end, string? hydrotestingType, string sourceFile, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RecordNdtProcessConsolidatedAsync(string poNumber, string ndtBatchNo, int ndtPcs, int okPcs, int visualReject, int hydrotestReject, int revisualReject, DateTime bundleStart, DateTime bundleEnd, string outputFilePath, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RecordBundleLabelAsync(string poNumber, int millNo, string? specification, string? type, string? pipeSize, string? length, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task DeleteOutputSlitRowsForRemovedOutputLinesAsync(string ndtBatchNo, IReadOnlyList<RemovedSlitRowTraceRef> refs, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task UpsertManualStationRunAsync(string poNumber, string ndtBatchNo, int ndtPcs, int okPcs, int rejectPcs, string workStation, DateTime start, DateTime end, string? hydrotestingType, string sourceFile, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<int> UpdateOutputSlitRowNdtPipesByBatchAndSlitAsync(string ndtBatchNo, string slitNo, int newNdtPipes, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task SyncOutputSlitRowsFromPerSlitCsvForBatchAsync(string ndtBatchNo, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task UpdateNdtProcessConsolidatedFromStationsAsync(string poNumber, string ndtBatchNo, int ndtPcs, int okPcs, int visualReject, int hydrotestReject, int revisualReject, DateTime? bundleStart, DateTime? bundleEnd, string? outputFilePath, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
