using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using Serilog.Context;

namespace NdtBundleService.Services;

/// <summary>
/// Writes one CSV file per completed bundle. Sends ZPL to the printer only when NdtBundle:EnableNdtTagZplAndPrint is true.
/// Sequence is allocated in SQL (Mill_Sequence) in the same transaction as the NDT_Bundle insert.
/// </summary>
public sealed class CsvBundleOutputWriter : IBundleOutputWriter
{
    private readonly NdtBundleOptions _options;
    private readonly INdtBundleRepository _bundleRepository;
    private readonly ICsvFillService _csvFill;
    private readonly INdtTagPrinter? _tagPrinter;
    private readonly ITraceabilityRepository? _traceability;
    private readonly IWipLabelProvider? _wipLabelProvider;
    private readonly IMillSequenceService? _millSequence;
    private readonly ILogger<CsvBundleOutputWriter> _logger;

    public CsvBundleOutputWriter(
        IOptions<NdtBundleOptions> options,
        INdtBundleRepository bundleRepository,
        ICsvFillService csvFill,
        ILogger<CsvBundleOutputWriter> logger,
        INdtTagPrinter? tagPrinter = null,
        ITraceabilityRepository? traceability = null,
        IWipLabelProvider? wipLabelProvider = null,
        IMillSequenceService? millSequence = null)
    {
        _options = options.Value;
        _bundleRepository = bundleRepository;
        _csvFill = csvFill;
        _tagPrinter = tagPrinter;
        _traceability = traceability;
        _wipLabelProvider = wipLabelProvider;
        _millSequence = millSequence;
        _logger = logger;
    }

    public async Task<int> WriteBundleAsync(InputSlitRecord contextRecord, int ndtBatchNo, int totalNdtPcs, CancellationToken cancellationToken, Guid? correlationId = null)
    {
        using (correlationId is { } id ? LogContext.PushProperty("CorrelationId", id) : null)
        {
            return await WriteBundleCoreAsync(contextRecord, ndtBatchNo, totalNdtPcs, cancellationToken, correlationId).ConfigureAwait(false);
        }
    }

    private async Task<int> WriteBundleCoreAsync(InputSlitRecord contextRecord, int ndtBatchNo, int totalNdtPcs, CancellationToken cancellationToken, Guid? correlationId)
    {
        if (totalNdtPcs <= 0)
        {
            _logger.LogDebug(
                "Skipping NDT bundle output for PO {PO} Mill {Mill}: zero NDT pipes.",
                contextRecord.PoNumber,
                contextRecord.MillNo);
            return 0;
        }

        var (sequence, ndtBatchNoFormatted) = await ResolveSequenceAndInsertAsync(
            contextRecord, ndtBatchNo, totalNdtPcs, cancellationToken).ConfigureAwait(false);
        ndtBatchNo = sequence;
        var bundleFolder = NdtBundleOutputPaths.ResolveBundleSummaryWriteFolder(_options);
        if (_options.EnableBundleSummaryCsvFiles)
        {
            if (string.IsNullOrWhiteSpace(bundleFolder))
            {
                _logger.LogWarning(
                    "BundleSummaryOutputFolder is not configured; NDT_Bundle CSV will not be written.");
            }
            else
            {
                Directory.CreateDirectory(bundleFolder);
                var fileName = NdtBundleOutputPaths.GetBundleCsvFileName(ndtBatchNoFormatted);
                var path = Path.Combine(bundleFolder, fileName);

                var lines = new List<string>
                {
                    BundleCloseCsv.Header
                };

                lines.Add(BundleCloseCsv.FormatLine(contextRecord, totalNdtPcs, ndtBatchNoFormatted));

                await File.WriteAllLinesAsync(path, lines, cancellationToken);
                _logger.LogInformation("Wrote bundle CSV: {Path}", path);
            }
        }
        else
        {
            _logger.LogDebug("Skipping NDT_Bundle summary CSV for {BatchNo} (NdtBundle:EnableBundleSummaryCsvFiles=false).", ndtBatchNoFormatted);
        }

        if (_millSequence is not { IsEnabled: true })
        {
            var record = BuildPendingRecord(contextRecord, ndtBatchNoFormatted, totalNdtPcs);
            await _bundleRepository.RecordBundlePendingPrintAsync(record, cancellationToken).ConfigureAwait(false);
        }

        await _csvFill
            .TryInitializeFillTargetAsync(ndtBatchNoFormatted, totalNdtPcs, closeSource: null, cancellationToken)
            .ConfigureAwait(false);
        await TryRecordBundleLabelAsync(contextRecord.PoNumber, contextRecord.MillNo, cancellationToken).ConfigureAwait(false);

        if (_tagPrinter is null)
        {
            _logger.LogDebug(
                "No tag printer configured for bundle {BatchNo}; marking Print_Status=Printed without ZPL attempt.",
                ndtBatchNoFormatted);
            await _bundleRepository.UpdateBundlePrintStatusAsync(
                ndtBatchNoFormatted,
                BundlePrintStatus.Printed,
                null,
                cancellationToken).ConfigureAwait(false);
        }
        else
        {
            try
            {
                var printResult = await _tagPrinter.PrintBundleTagAsync(
                    contextRecord,
                    ndtBatchNo,
                    totalNdtPcs,
                    isReprint: false,
                    cancellationToken).ConfigureAwait(false);

                if (printResult.Success)
                {
                    _logger.LogInformation(
                        "Bundle {BatchNo} tag print succeeded; Print_Status=Printed. CorrelationId {CorrelationId}",
                        ndtBatchNoFormatted,
                        correlationId);
                    await _bundleRepository.UpdateBundlePrintStatusAsync(
                        ndtBatchNoFormatted,
                        BundlePrintStatus.Printed,
                        null,
                        cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    var error = printResult.ErrorDetail ?? "PrintBundleTagAsync returned false.";
                    _logger.LogError(
                        "Bundle {BatchNo} tag print failed: {Error} CorrelationId {CorrelationId}",
                        ndtBatchNoFormatted,
                        error,
                        correlationId);
                    await _bundleRepository.UpdateBundlePrintStatusAsync(
                        ndtBatchNoFormatted,
                        BundlePrintStatus.PrintFailed,
                        error,
                        cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "Auto-print failed for bundle {BatchNo}. CorrelationId {CorrelationId}",
                    ndtBatchNoFormatted,
                    correlationId);
                await _bundleRepository.UpdateBundlePrintStatusAsync(
                    ndtBatchNoFormatted,
                    BundlePrintStatus.PrintFailed,
                    ex.Message,
                    cancellationToken).ConfigureAwait(false);
            }
        }

        return sequence;
    }

    private async Task TryRecordBundleLabelAsync(string poNumber, int millNo, CancellationToken cancellationToken)
    {
        if (_traceability is null || _wipLabelProvider is null)
            return;

        try
        {
            var wip = await _wipLabelProvider.GetWipLabelAsync(poNumber, millNo, cancellationToken).ConfigureAwait(false);
            if (wip is null)
                return;

            await _traceability.RecordBundleLabelAsync(
                poNumber,
                millNo,
                specification: wip.PipeGrade,
                type: wip.PipeType,
                pipeSize: wip.PipeSize,
                length: wip.PipeLength,
                cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to persist Bundle_Label for PO {PoNumber} mill {MillNo}.", poNumber, millNo);
        }
    }

    /// <summary>
    /// When SQL + Mill_Sequence is enabled, allocate and insert NDT_Bundle in one transaction.
    /// Tests / CSV-only mode keep the engine-passed integer (must be &gt; 0).
    /// </summary>
    private async Task<(int Sequence, string Formatted)> ResolveSequenceAndInsertAsync(
        InputSlitRecord contextRecord,
        int passedSequence,
        int totalNdtPcs,
        CancellationToken cancellationToken)
    {
        if (_millSequence is { IsEnabled: true })
        {
            try
            {
                return await _millSequence
                    .AllocateAndInsertBundleAsync(
                        BuildPendingRecord(contextRecord, bundleNo: string.Empty, totalNdtPcs),
                        cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, BundleCloseFailure.AllocateUnavailable);
                throw new InvalidOperationException(BundleCloseFailure.AllocateUnavailable, ex);
            }
        }

        if (passedSequence <= 0)
        {
            _logger.LogError(BundleCloseFailure.AllocateUnavailable);
            throw new InvalidOperationException(BundleCloseFailure.AllocateUnavailable);
        }

        return (passedSequence, NdtBundleSequence.Format(passedSequence, contextRecord.MillNo));
    }

    private static NdtBundleRecord BuildPendingRecord(InputSlitRecord contextRecord, string bundleNo, int totalNdtPcs) =>
        new()
        {
            BundleNo = bundleNo,
            PoNumber = contextRecord.PoNumber,
            MillNo = contextRecord.MillNo,
            TotalNdtPcs = totalNdtPcs,
            TargetNdtPcs = totalNdtPcs,
            CsvFilled = 0,
            CsvFillState = CsvFillState.PlcClosed,
            SlitNo = contextRecord.SlitNo,
            SlitStartTime = contextRecord.SlitStartTime,
            SlitFinishTime = contextRecord.SlitFinishTime,
            RejectedPipes = contextRecord.RejectedPipes,
            NdtShortLengthPipe = contextRecord.NdtShortLengthPipe,
            RejectedShortLengthPipe = contextRecord.RejectedShortLengthPipe
        };
}
