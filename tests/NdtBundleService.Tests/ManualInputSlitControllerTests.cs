using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Controllers;
using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class ManualInputSlitControllerTests : IDisposable
{
    private readonly string _outputFolder;

    public ManualInputSlitControllerTests()
    {
        _outputFolder = Path.Combine(Path.GetTempPath(), "manual-ndt-out-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_outputFolder);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_outputFolder))
                Directory.Delete(_outputFolder, recursive: true);
        }
        catch
        {
            /* ignore */
        }
    }

    [Fact]
    public async Task CreateManualFile_writes_ndt_output_csv_with_batch_column_and_sap_times()
    {
        var trace = new CapturingTraceability();
        var sap = new NoOpSapStatus();
        var sut = new InputSlitsController(
            Options.Create(new NdtBundleOptions { OutputBundleFolder = _outputFolder }),
            trace,
            sap,
            NullLogger<InputSlitsController>.Instance);

        var result = await sut.CreateManualFile(
            new ManualInputSlitRequest
            {
                PoNumber = "1000060999",
                MillNo = 1,
                SlitNo = "06",
                NdtPipes = 12,
                RejectedPipes = 0,
                NdtBatchNo = "BND-1000060999-01",
                SlitStartTime = "27.09.2026 14:53:31",
                SlitFinishTime = "27.09.2026 15:10:00"
            },
            CancellationToken.None);

        Assert.IsType<OkObjectResult>(result);

        var expectedName = "06_260927_1000060999";
        var path = Path.Combine(_outputFolder, expectedName);
        Assert.True(File.Exists(path));
        var lines = await File.ReadAllLinesAsync(path);
        Assert.Equal(InputSlitsController.ManualNdtOutputCsvHeader, lines[0]);
        Assert.Contains("1000060999", lines[1], StringComparison.Ordinal);
        Assert.Contains("BND-1000060999-01", lines[1], StringComparison.Ordinal);
        Assert.Contains("27.09.2026 14:53:31", lines[1], StringComparison.Ordinal);
        Assert.Contains("27.09.2026 15:10:00", lines[1], StringComparison.Ordinal);
        Assert.Contains(",12,", lines[1], StringComparison.Ordinal);
        Assert.Single(trace.Calls);
        Assert.Equal(path, trace.Calls[0].SourceFile);
        Assert.Equal("BND-1000060999-01", trace.Calls[0].BatchNo);
        Assert.True(sap.Written);
    }

    [Fact]
    public async Task CreateManualFile_does_not_write_to_input_slit_folder()
    {
        var inbox = Path.Combine(Path.GetTempPath(), "manual-inbox-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(inbox);
        try
        {
            var sut = new InputSlitsController(
                Options.Create(new NdtBundleOptions
                {
                    InputSlitFolder = inbox,
                    OutputBundleFolder = _outputFolder
                }),
                new CapturingTraceability(),
                new NoOpSapStatus(),
                NullLogger<InputSlitsController>.Instance);

            await sut.CreateManualFile(
                new ManualInputSlitRequest
                {
                    PoNumber = "1000060888",
                    MillNo = 2,
                    SlitNo = "03",
                    NdtPipes = 5,
                    NdtBatchNo = "BATCH-A",
                    SlitStartTime = "27.09.2026 10:00:00",
                    SlitFinishTime = "27.09.2026 10:05:00"
                },
                CancellationToken.None);

            Assert.Empty(Directory.GetFiles(inbox));
            Assert.True(File.Exists(Path.Combine(_outputFolder, "03_260927_1000060888")));
        }
        finally
        {
            try { Directory.Delete(inbox, recursive: true); } catch { /* ignore */ }
        }
    }

    [Fact]
    public async Task CreateManualFile_rejects_missing_batch()
    {
        var sut = new InputSlitsController(
            Options.Create(new NdtBundleOptions { OutputBundleFolder = _outputFolder }),
            new CapturingTraceability(),
            new NoOpSapStatus(),
            NullLogger<InputSlitsController>.Instance);

        var result = await sut.CreateManualFile(
            new ManualInputSlitRequest
            {
                PoNumber = "1",
                MillNo = 1,
                SlitNo = "01",
                NdtPipes = 1,
                SlitStartTime = "27.09.2026 14:53:31",
                SlitFinishTime = "27.09.2026 14:54:00"
            },
            CancellationToken.None);

        Assert.IsType<BadRequestObjectResult>(result);
    }

    [Fact]
    public async Task CreateManualFile_rejects_invalid_time_format()
    {
        var sut = new InputSlitsController(
            Options.Create(new NdtBundleOptions { OutputBundleFolder = _outputFolder }),
            new CapturingTraceability(),
            new NoOpSapStatus(),
            NullLogger<InputSlitsController>.Instance);

        var result = await sut.CreateManualFile(
            new ManualInputSlitRequest
            {
                PoNumber = "1",
                MillNo = 1,
                SlitNo = "01",
                NdtPipes = 1,
                NdtBatchNo = "B1",
                SlitStartTime = "2026-09-27 14:53:31",
                SlitFinishTime = "27.09.2026 14:54:00"
            },
            CancellationToken.None);

        Assert.IsType<BadRequestObjectResult>(result);
    }

    [Fact]
    public async Task CreateManualFile_rejects_invalid_mill()
    {
        var sut = new InputSlitsController(
            Options.Create(new NdtBundleOptions { OutputBundleFolder = _outputFolder }),
            new CapturingTraceability(),
            new NoOpSapStatus(),
            NullLogger<InputSlitsController>.Instance);

        var result = await sut.CreateManualFile(
            new ManualInputSlitRequest
            {
                PoNumber = "1",
                MillNo = 9,
                SlitNo = "01",
                NdtPipes = 1,
                NdtBatchNo = "B1",
                SlitStartTime = "27.09.2026 14:53:31",
                SlitFinishTime = "27.09.2026 14:54:00"
            },
            CancellationToken.None);

        Assert.IsType<BadRequestObjectResult>(result);
    }

    [Fact]
    public async Task CreateManualFile_accepts_pasted_time_with_extra_whitespace()
    {
        var sut = new InputSlitsController(
            Options.Create(new NdtBundleOptions { OutputBundleFolder = _outputFolder }),
            new CapturingTraceability(),
            new NoOpSapStatus(),
            NullLogger<InputSlitsController>.Instance);

        var result = await sut.CreateManualFile(
            new ManualInputSlitRequest
            {
                PoNumber = "1000061839",
                MillNo = 1,
                SlitNo = "06",
                NdtPipes = 1,
                NdtBatchNo = "1226100001",
                SlitStartTime = "  \"27.09.2026  14:53:31\"  ",
                SlitFinishTime = "27.09.2026\t15:00:00\r\n"
            },
            CancellationToken.None);

        Assert.IsType<OkObjectResult>(result);
        var path = Path.Combine(_outputFolder, "06_260927_1000061839");
        var lines = await File.ReadAllLinesAsync(path);
        Assert.Contains("27.09.2026 14:53:31", lines[1], StringComparison.Ordinal);
        Assert.Contains("27.09.2026 15:00:00", lines[1], StringComparison.Ordinal);
    }

    [Fact]
    public void BuildManualOutputFileName_is_slit_yymmdd_po()
    {
        var name = InputSlitsController.BuildManualOutputFileName(
            "06",
            new DateTime(2026, 9, 27, 14, 53, 31),
            "1000061839");
        Assert.Equal("06_260927_1000061839", name);
    }

    private sealed class CapturingTraceability : ITraceabilityRepository
    {
        public List<(string SourceFile, string BatchNo)> Calls { get; } = new();

        public Task RecordInputSlitRowsAsync(
            string sourceFile,
            IReadOnlyList<(InputSlitRecord Record, int SourceRowNumber)> rows,
            CancellationToken cancellationToken,
            DateTime? sourceLastWriteTimeUtc = null) =>
            Task.CompletedTask;

        public Task<bool> IsInputSlitFileVersionImportedAsync(
            string sourceFileFullPath, DateTime fileLastWriteTimeUtc, CancellationToken cancellationToken) =>
            Task.FromResult(false);

        public Task<bool> IsInputSlitFileSeenAsync(
            string sourceFileFullPath, DateTime fileLastWriteTimeUtc, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult(false);

        public Task MarkInputSlitFileSeenAsync(
            string sourceFileFullPath, DateTime fileLastWriteTimeUtc, string reason, int millNo, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task<OutputSlitBatchCorrectionResult> UpdateOutputSlitBatchNoAsync(
            string poNumber, int millNo, string oldBatchNo, string newBatchNo, CancellationToken cancellationToken) =>
            Task.FromResult(OutputSlitBatchCorrectionResult.NoOp);

        public Task<string?> TryGetExistingOutputSlitBatchAsync(
            string sourceFileFullPath, string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<string?>(null);

        public Task<IReadOnlyList<string>> GetSapFrozenSourceFilesAsync(
            IReadOnlyList<string> sourceFiles, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());

        public Task RecordOutputSlitRowsAsync(
            string sourceFile,
            IReadOnlyList<(InputSlitRecord Record, string NdtBatchNo, int SourceRowNumber, bool LinkBundleParent)> rows,
            CancellationToken cancellationToken)
        {
            foreach (var r in rows)
                Calls.Add((sourceFile, r.NdtBatchNo));
            return Task.CompletedTask;
        }

        public Task RecordManualStationRunAsync(
            string poNumber, string ndtBatchNo, int ndtPcs, int okPcs, int rejectPcs, string workStation,
            DateTime start, DateTime end, string? hydrotestingType, string sourceFile, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task RecordManualStationPrintAsync(
            string ndtBatchNo, string workStation, string printStatus, string? printError, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task RecordNdtProcessConsolidatedAsync(
            string poNumber, string ndtBatchNo, int ndtPcs, int okPcs, int visualReject, int hydrotestReject,
            int revisualReject, DateTime bundleStart, DateTime bundleEnd, string outputFilePath, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task RecordBundleLabelAsync(
            string poNumber, int millNo, string? specification, string? type, string? pipeSize, string? length,
            CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task RecordUploadBundleRowsAsync(
            string generatedFile, IReadOnlyList<UploadBundleRow> rows, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task DeleteOutputSlitRowsForRemovedOutputLinesAsync(
            string ndtBatchNo, IReadOnlyList<RemovedSlitRowTraceRef> refs, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task UpsertManualStationRunAsync(
            string poNumber, string ndtBatchNo, int ndtPcs, int okPcs, int rejectPcs, string workStation,
            DateTime start, DateTime end, string? hydrotestingType, string sourceFile, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task<int> UpdateOutputSlitRowNdtPipesByBatchAndSlitAsync(
            string ndtBatchNo, string slitNo, int ndtPipes, CancellationToken cancellationToken) =>
            Task.FromResult(0);

        public Task SyncOutputSlitRowsFromPerSlitCsvForBatchAsync(string ndtBatchNo, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task UpdateNdtProcessConsolidatedFromStationsAsync(
            string poNumber, string ndtBatchNo, int ndtPcs, int okPcs, int visualReject, int hydrotestReject,
            int revisualReject, DateTime? bundleStart, DateTime? bundleEnd, string? outputFilePath, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class NoOpSapStatus : IOutputSlitSapStatusRepository
    {
        public bool Written { get; private set; }
        public bool Enabled => true;

        public Task<OutputSlitSapStatusApplyResult> ApplyObservationsAsync(
            IReadOnlyList<OutputSlitSapStatusObservation> observations,
            CancellationToken cancellationToken) =>
            Task.FromResult(OutputSlitSapStatusApplyResult.Empty);

        public Task RecordResubmitDriftSyncedEventAsync(string fileName, string pendingFolder, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task RecordOutputFileWrittenAsync(
            string fileName,
            DateTime? fileLastWriteTimeUtc,
            string outputFolder,
            CancellationToken cancellationToken)
        {
            Written = true;
            return Task.CompletedTask;
        }

        public Task<IReadOnlyDictionary<string, OutputSlitSapFileStatus>> GetStatusesForFilesAsync(
            IReadOnlyCollection<string> fileNames,
            CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<string, OutputSlitSapFileStatus>>(
                new Dictionary<string, OutputSlitSapFileStatus>(StringComparer.OrdinalIgnoreCase));
    }
}
