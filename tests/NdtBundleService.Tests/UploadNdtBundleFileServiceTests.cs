using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class UploadBundleSlitNoSelectionTests
{
    [Fact]
    public void Pick_prefers_unclaimed_context()
    {
        var picked = UploadBundleSlitNoSelection.Pick(
            "2605848_03",
            [("2601111_01", DateTime.UtcNow)],
            Array.Empty<string>());
        Assert.Equal("2605848_03", picked);
    }

    [Fact]
    public void Pick_skips_claimed_context_and_uses_latest_finish()
    {
        var older = DateTime.UtcNow.AddHours(-2);
        var newer = DateTime.UtcNow.AddHours(-1);
        var picked = UploadBundleSlitNoSelection.Pick(
            "2605848_03",
            [
                ("2609999_09", newer),
                ("2601111_01", older)
            ],
            new[] { "2605848_03" });
        Assert.Equal("2609999_09", picked);
    }

    [Fact]
    public void Pick_skips_claimed_output_slits()
    {
        var picked = UploadBundleSlitNoSelection.Pick(
            "",
            [
                ("AAA_01", DateTime.UtcNow),
                ("BBB_02", DateTime.UtcNow.AddMinutes(-5))
            ],
            new[] { "AAA_01" });
        Assert.Equal("BBB_02", picked);
    }

    [Fact]
    public void Pick_returns_empty_when_all_claimed()
    {
        var picked = UploadBundleSlitNoSelection.Pick(
            "AAA_01",
            [("AAA_01", DateTime.UtcNow), ("BBB_02", DateTime.UtcNow)],
            new[] { "AAA_01", "BBB_02" });
        Assert.Equal("", picked);
    }
}

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
        Assert.EndsWith(".csv", result.FilePath, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(".xlsx", result.FilePath, StringComparison.OrdinalIgnoreCase);

        var bytes = await File.ReadAllBytesAsync(result.FilePath);
        Assert.True(bytes.Length >= 3);
        Assert.Equal(0xEF, bytes[0]);
        Assert.Equal(0xBB, bytes[1]);
        Assert.Equal(0xBF, bytes[2]);

        var lines = Encoding.UTF8.GetString(bytes).TrimStart('\uFEFF').Split('\n', StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(2, lines.Length);
        Assert.Contains("1226100001", lines[1], StringComparison.Ordinal);
        Assert.Contains("47", lines[1], StringComparison.Ordinal);
        Assert.Contains("2603832_05", lines[1], StringComparison.Ordinal);
        Assert.DoesNotContain("1226100002", lines[1], StringComparison.Ordinal);
        Assert.DoesNotContain("1000057002", lines[1], StringComparison.Ordinal);
    }

    [Fact]
    public async Task GenerateForBatchAsync_falls_back_to_latest_unclaimed_output_slit()
    {
        WriteProcessCsv("1226100001", po: "1000057001", ndtPcs: 50, ok: 47);
        var older = DateTime.UtcNow.AddHours(-3);
        var newer = DateTime.UtcNow.AddHours(-1);
        var sut = CreateSut(
            contextSlitNo: "",
            outputSlits: [("2601111_01", older), ("2609999_09", newer)],
            claimed: Array.Empty<string>());

        var result = await sut.GenerateForBatchAsync("1226100001", CancellationToken.None);
        var text = await File.ReadAllTextAsync(result.FilePath, Encoding.UTF8);
        Assert.Contains("2609999_09", text, StringComparison.Ordinal);
        Assert.DoesNotContain("2601111_01", text.Split('\n')[1], StringComparison.Ordinal);
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

    private UploadNdtBundleFileService CreateSut(
        string contextSlitNo = "2603832_05",
        IReadOnlyList<(string SlitNo, DateTime? SlitFinishTime)>? outputSlits = null,
        IReadOnlyCollection<string>? claimed = null)
    {
        var options = Options.Create(new NdtBundleOptions
        {
            NdtProcessOutputFolder = Path.Combine(_root, "process"),
            UploadNdtBundleFilesFolder = Path.Combine(_root, "upload")
        });
        return new UploadNdtBundleFileService(
            options,
            new StubBundleRepository(contextSlitNo),
            new StubTraceability(outputSlits, claimed),
            new StubWipLabel(),
            new StubSlitAccepted(),
            new StubFgBundle(),
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

    private sealed class StubWipLabel : IWipLabelProvider
    {
        public Task<WipLabelInfo?> GetWipLabelAsync(string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<WipLabelInfo?>(new WipLabelInfo
            {
                PipeGrade = "X52",
                PipeThickness = "9.53",
                PipeLength = "12",
                PipeWeightPerMeter = "20"
            });
    }

    private sealed class StubSlitAccepted : ISlitAcceptedRepository
    {
        public Task<bool> EnsureImportReadyAsync(CancellationToken cancellationToken) => Task.FromResult(false);
        public Task<bool> IsImportSourceFilePresentAsync(string sourceFileKey, CancellationToken cancellationToken) => Task.FromResult(false);
        public Task<int> InsertImportRowsAsync(string sourceFileKey, IReadOnlyList<SlitAcceptedRow> rows, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<int> InsertAuditRowsAsync(string sourceFileKey, IReadOnlyList<CsvFolderAuditRow> rows, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<string?> TryGetLatestSlitWidthAsync(string slitNo, CancellationToken cancellationToken) => Task.FromResult<string?>(null);
    }

    private sealed class StubFgBundle : IFgBundleRepository
    {
        public Task<bool> EnsureImportReadyAsync(CancellationToken cancellationToken) => Task.FromResult(false);
        public Task<bool> IsImportSourceFilePresentAsync(string sourceFileKey, CancellationToken cancellationToken) => Task.FromResult(false);
        public Task<int> InsertImportRowsAsync(string sourceFileKey, IReadOnlyList<FgBundleRow> rows, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<int> InsertAuditRowsAsync(string sourceFileKey, IReadOnlyList<CsvFolderAuditRow> rows, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<string?> TryGetLatestPipeGradeAsync(string poNumber, int? millNo, CancellationToken cancellationToken) => Task.FromResult<string?>(null);
    }

    private sealed class StubTraceability : ITraceabilityRepository
    {
        private readonly IReadOnlyList<(string SlitNo, DateTime? SlitFinishTime)> _outputSlits;
        private readonly IReadOnlyCollection<string> _claimed;

        public StubTraceability(
            IReadOnlyList<(string SlitNo, DateTime? SlitFinishTime)>? outputSlits,
            IReadOnlyCollection<string>? claimed)
        {
            _outputSlits = outputSlits ?? Array.Empty<(string, DateTime?)>();
            _claimed = claimed ?? Array.Empty<string>();
        }

        public Task<IReadOnlyList<(string SlitNo, DateTime? SlitFinishTime)>> GetOutputSlitsByFinishTimeForBatchAsync(
            string ndtBatchNo,
            CancellationToken cancellationToken) =>
            Task.FromResult(_outputSlits);

        public Task<IReadOnlyCollection<string>> GetSlitNosClaimedByOtherBundlesAsync(
            string excludeBundleNo,
            CancellationToken cancellationToken) =>
            Task.FromResult(_claimed);

        public Task RecordUploadBundleRowsAsync(string generatedFile, IReadOnlyList<UploadBundleRow> rows, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task RecordInputSlitRowsAsync(string sourceFile, IReadOnlyList<(InputSlitRecord Record, int SourceRowNumber)> rows, CancellationToken cancellationToken, DateTime? sourceLastWriteTimeUtc = null) => Task.CompletedTask;
        public Task<bool> IsInputSlitFileVersionImportedAsync(string sourceFileFullPath, DateTime fileLastWriteTimeUtc, CancellationToken cancellationToken) => Task.FromResult(false);
        public Task<bool> IsInputSlitFileSeenAsync(string sourceFileFullPath, DateTime fileLastWriteTimeUtc, int millNo, CancellationToken cancellationToken) => Task.FromResult(false);
        public Task MarkInputSlitFileSeenAsync(string sourceFileFullPath, DateTime fileLastWriteTimeUtc, string reason, int millNo, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<OutputSlitBatchCorrectionResult> UpdateOutputSlitBatchNoAsync(string poNumber, int millNo, string oldBatchNo, string newBatchNo, CancellationToken cancellationToken) =>
            Task.FromResult(OutputSlitBatchCorrectionResult.NoOp);
        public Task<string?> TryGetExistingOutputSlitBatchAsync(string sourceFileFullPath, string poNumber, int millNo, CancellationToken cancellationToken) => Task.FromResult<string?>(null);
        public Task<IReadOnlyList<string>> GetSapFrozenSourceFilesAsync(IReadOnlyList<string> sourceFiles, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());
        public Task RecordOutputSlitRowsAsync(string sourceFile, IReadOnlyList<(InputSlitRecord Record, string NdtBatchNo, int SourceRowNumber, bool LinkBundleParent)> rows, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RecordManualStationRunAsync(string poNumber, string ndtBatchNo, int ndtPcs, int okPcs, int rejectPcs, string workStation, DateTime start, DateTime end, string? hydrotestingType, string sourceFile, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RecordManualStationPrintAsync(string ndtBatchNo, string workStation, string printStatus, string? printError, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<IReadOnlyList<ManualStationPrintedTag>> GetManualStationPrintedTagsAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<ManualStationPrintedTag>>(Array.Empty<ManualStationPrintedTag>());
        public Task<IReadOnlyList<ManualStationPrintedTag>> GetManualStationRunsForBatchAsync(string ndtBatchNo, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<ManualStationPrintedTag>>(Array.Empty<ManualStationPrintedTag>());
        public Task RecordNdtProcessConsolidatedAsync(string poNumber, string ndtBatchNo, int ndtPcs, int okPcs, int visualReject, int hydrotestReject, int revisualReject, DateTime bundleStart, DateTime bundleEnd, string outputFilePath, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RecordBundleLabelAsync(string poNumber, int millNo, string? specification, string? type, string? pipeSize, string? length, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task DeleteOutputSlitRowsForRemovedOutputLinesAsync(string ndtBatchNo, IReadOnlyList<RemovedSlitRowTraceRef> refs, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task UpsertManualStationRunAsync(string poNumber, string ndtBatchNo, int ndtPcs, int okPcs, int rejectPcs, string workStation, DateTime start, DateTime end, string? hydrotestingType, string sourceFile, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<int> UpdateOutputSlitRowNdtPipesByBatchAndSlitAsync(string ndtBatchNo, string slitNo, int newNdtPipes, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task SyncOutputSlitRowsFromPerSlitCsvForBatchAsync(string ndtBatchNo, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task UpdateNdtProcessConsolidatedFromStationsAsync(string poNumber, string ndtBatchNo, int ndtPcs, int okPcs, int visualReject, int hydrotestReject, int revisualReject, DateTime? bundleStart, DateTime? bundleEnd, string? outputFilePath, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task UpsertInputSlitPendingAsync(string sourceFileFullPath, DateTime sourceLastWriteTimeUtc, int millNo, IReadOnlyList<(InputSlitRecord Record, int SourceRowNumber, string? PipeSize)> rows, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<IReadOnlyList<InputSlitPendingClaim>> ListAwaitingInputSlitPendingAsync(IReadOnlyList<int> millNos, CancellationToken cancellationToken) => Task.FromResult<IReadOnlyList<InputSlitPendingClaim>>(Array.Empty<InputSlitPendingClaim>());
        public Task MarkInputSlitPendingCompletedAsync(long pendingId, string ndtBatchNo, string? outputFile, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task TryCompleteInputSlitPendingByKeyAsync(string sourceFileName, DateTime sourceLastWriteTimeUtc, int millNo, string ndtBatchNo, string? outputFile, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class StubBundleRepository : INdtBundleRepository
    {
        private readonly string _slitNo;

        public StubBundleRepository(string slitNo) => _slitNo = slitNo;

        public Task<NdtBundleRecord?> GetByBatchNoAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(new NdtBundleRecord
            {
                BundleNo = batchNo,
                PoNumber = "1000057001",
                MillNo = 1,
                SlitNo = _slitNo,
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
}
