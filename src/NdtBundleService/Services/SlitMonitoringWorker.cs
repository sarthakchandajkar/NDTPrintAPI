using System.Globalization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Background service that periodically scans the Input Slit CSV folder,
/// feeds records into the bundle engine, and reacts to PO-end signals.
/// </summary>
public sealed class SlitMonitoringWorker : BackgroundService
{
    private readonly NdtBundleOptions _options;
    private readonly IBundleEngine _bundleEngine;
    private readonly IBundleOutputWriter _outputWriter;
    private readonly INdtBatchStateService _batchState;
    private readonly ICurrentPoPlanService? _currentPoPlanService;
    private readonly IPlcClient _plcClient;
    private readonly ITraceabilityRepository _traceability;
    private readonly ILogger<SlitMonitoringWorker> _logger;

    // Simple in-memory set to avoid reprocessing files during one runtime.
    private readonly HashSet<string> _processedFiles = new(StringComparer.OrdinalIgnoreCase);

    public SlitMonitoringWorker(
        IOptions<NdtBundleOptions> options,
        IBundleEngine bundleEngine,
        IBundleOutputWriter outputWriter,
        INdtBatchStateService batchState,
        IPlcClient plcClient,
        ITraceabilityRepository traceability,
        ILogger<SlitMonitoringWorker> logger)
        : this(options, bundleEngine, outputWriter, batchState, null, plcClient, traceability, logger) { }

    public SlitMonitoringWorker(
        IOptions<NdtBundleOptions> options,
        IBundleEngine bundleEngine,
        IBundleOutputWriter outputWriter,
        INdtBatchStateService batchState,
        ICurrentPoPlanService? currentPoPlanService,
        IPlcClient plcClient,
        ITraceabilityRepository traceability,
        ILogger<SlitMonitoringWorker> logger)
    {
        _options = options.Value;
        _bundleEngine = bundleEngine;
        _outputWriter = outputWriter;
        _batchState = batchState;
        _currentPoPlanService = currentPoPlanService;
        _plcClient = plcClient;
        _traceability = traceability;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (string.IsNullOrWhiteSpace(_options.InputSlitFolder))
        {
            _logger.LogWarning("InputSlitFolder is not configured. Worker will not process any files.");
            return;
        }

        if (!Directory.Exists(_options.InputSlitFolder))
        {
            _logger.LogWarning(
                "InputSlitFolder does not exist or is not reachable (service will not create it): {Folder}",
                _options.InputSlitFolder);
            return;
        }

        _logger.LogInformation("SlitMonitoringWorker started. Watching folder {Folder}", _options.InputSlitFolder);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessNewSlitFilesAsync(stoppingToken).ConfigureAwait(false);

                // Poll PO-end signal. When PLC is connected: if GetPoEndAsync returns true, call the same logic as Simulate PO End
                // (current PO/mill from ICurrentPoPlanService or context, then IncrementBatchOnPoEndAsync + AdvanceToNextPoAsync).
                _ = await _plcClient.GetPoEndAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while processing slit files or PLC signals.");
            }

            var delaySeconds = Math.Max(1, _options.PollIntervalSeconds);
            await Task.Delay(TimeSpan.FromSeconds(delaySeconds), stoppingToken).ConfigureAwait(false);
        }
    }

    private async Task ProcessNewSlitFilesAsync(CancellationToken cancellationToken)
    {
        var files = Directory.EnumerateFiles(_options.InputSlitFolder, "*.csv");
        var minUtc = SourceFileEligibility.ParseMinUtc(_options);

        foreach (var file in files)
        {
            if (_processedFiles.Contains(file))
                continue;

            if (!SourceFileEligibility.IncludeFileUtc(File.GetLastWriteTimeUtc(file), minUtc))
                continue;

            _logger.LogInformation("Processing Input Slit CSV file {File}", file);

            try
            {
                var (headerLine, rows) = await ReadSlitFileWithRawLinesAsync(file, cancellationToken).ConfigureAwait(false);

                // Build output content: same format as input with one extra column "NDT Batch No".
                var outputLines = new List<string> { headerLine.TrimEnd() + ",NDT Batch No" };
                var inputRowsForSql = new List<(InputSlitRecord Record, int SourceRowNumber)>();
                var outputRowsForSql = new List<(InputSlitRecord Record, string NdtBatchNo, int SourceRowNumber)>();

                var sourceRowNumber = 2; // CSV header is row 1
                foreach (var row in rows)
                {
                    if (row.Record is null)
                    {
                        // Keep source content intact; append blank batch for non-parseable rows.
                        outputLines.Add(row.RawLine.TrimEnd() + ",");
                        sourceRowNumber++;
                        continue;
                    }

                    var record = row.Record;
                    var (batchNumber, _, _) = await _batchState.GetBatchForRecordAsync(record.PoNumber, record.MillNo, record.NdtPipes, cancellationToken).ConfigureAwait(false);
                    var ndtBatchNoFormatted = FormatNdtBatchNo(batchNumber, record.MillNo);
                    outputLines.Add(row.RawLine.TrimEnd() + "," + ndtBatchNoFormatted);
                    inputRowsForSql.Add((record, sourceRowNumber));
                    outputRowsForSql.Add((record, ndtBatchNoFormatted, sourceRowNumber));
                    sourceRowNumber++;

                    // Let the bundle engine decide when a bundle is full; it calls the callback to write CSV and print tag.
                    try
                    {
                        await _bundleEngine.ProcessSlitRecordAsync(record, async (contextRecord, batchNo, totalNdtPcs) =>
                        {
                            try
                            {
                                await _outputWriter.WriteBundleAsync(contextRecord, batchNo, totalNdtPcs, cancellationToken).ConfigureAwait(false);
                                _logger.LogInformation(
                                    "Bundle output completed for {BatchNo} ({Pcs} pcs).",
                                    FormatNdtBatchNo(batchNo, contextRecord.MillNo),
                                    totalNdtPcs);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Tag print failed for bundle {BatchNo}.", FormatNdtBatchNo(batchNo, contextRecord.MillNo));
                            }
                        }, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Bundle engine failed for record in {File}; output CSV was already written.", file);
                    }
                }

                // Write one output file with the same name as the input file.
                var outputFolder = _options.OutputBundleFolder;
                if (!string.IsNullOrWhiteSpace(outputFolder))
                {
                    Directory.CreateDirectory(outputFolder);
                    var outputFileName = Path.GetFileName(file);
                    var outputPath = Path.Combine(outputFolder, outputFileName);
                    await File.WriteAllLinesAsync(outputPath, outputLines, cancellationToken).ConfigureAwait(false);
                    _logger.LogInformation("Wrote bundle CSV: {Path}", outputPath);
                }

                // Best-effort SQL traceability; CSV flow should not fail if SQL is down.
                await _traceability.RecordInputSlitRowsAsync(file, inputRowsForSql, cancellationToken).ConfigureAwait(false);
                await _traceability.RecordOutputSlitRowsAsync(file, outputRowsForSql, cancellationToken).ConfigureAwait(false);

                _processedFiles.Add(file);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process Input Slit CSV file {File}", file);
            }
        }
    }

    private static string FormatNdtBatchNo(int sequenceNumber, int millNo)
    {
        var yy = (DateTime.Now.Year % 100).ToString("D2", CultureInfo.InvariantCulture);
        var millDigit = (millNo >= 1 && millNo <= 4) ? millNo.ToString(CultureInfo.InvariantCulture) : "1";
        var seq = sequenceNumber.ToString("D5", CultureInfo.InvariantCulture);
        return "12" + yy + millDigit + seq;
    }

    /// <summary>
    /// Reads slit CSV and returns the header line and a list of (raw data line, parsed record) for each valid row.
    /// </summary>
    private static async Task<(string HeaderLine, List<(string RawLine, InputSlitRecord? Record)> Rows)> ReadSlitFileWithRawLinesAsync(string path, CancellationToken cancellationToken)
    {
        var rows = new List<(string RawLine, InputSlitRecord? Record)>();

        await using var stream = File.OpenRead(path);
        using var reader = new StreamReader(stream);

        var headerRaw = await reader.ReadLineAsync();
        if (headerRaw is null)
            return (string.Empty, rows);

        var headerLine = InputSlitCsvParsing.StripBom(headerRaw);
        var headers = InputSlitCsvParsing.SplitCsvFields(headerLine);

        var poIndex = InputSlitCsvParsing.HeaderIndex(headers, "PO Number", "PO_No", "PO No");
        var slitIndex = InputSlitCsvParsing.HeaderIndex(headers, "Slit No");
        var ndtIndex = InputSlitCsvParsing.HeaderIndex(headers, "NDT Pipes");
        var rejectedIndex = InputSlitCsvParsing.HeaderIndex(headers, "Rejected P");
        var startIndex = InputSlitCsvParsing.HeaderIndex(headers, "Slit Start Time");
        var finishIndex = InputSlitCsvParsing.HeaderIndex(headers, "Slit Finish Time");
        var millIndex = InputSlitCsvParsing.HeaderIndex(headers, "Mill No", "Mill Number");
        var shortIndex = InputSlitCsvParsing.HeaderIndex(headers, "NDT Short Length Pipe");
        var rejShortIndex = InputSlitCsvParsing.HeaderIndex(headers, "Rejected Short Length Pipe");

        while (!reader.EndOfStream)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var line = await reader.ReadLineAsync();
            if (string.IsNullOrWhiteSpace(line))
                continue;

            var cols = InputSlitCsvParsing.SplitCsvFields(line);
            if (cols.Length == 0)
                continue;

            var poRaw = GetString(cols, poIndex);
            var poNumber = string.IsNullOrWhiteSpace(poRaw) ? string.Empty : InputSlitCsvParsing.NormalizePo(poRaw);

            var millParsed = GetMillNo(cols, millIndex);

            var record = new InputSlitRecord
            {
                PoNumber = poNumber,
                SlitNo = GetString(cols, slitIndex),
                NdtPipes = GetIntFlexible(cols, ndtIndex),
                RejectedPipes = GetIntFlexible(cols, rejectedIndex),
                SlitStartTime = GetDateTime(cols, startIndex),
                SlitFinishTime = GetDateTime(cols, finishIndex),
                MillNo = millParsed,
                NdtShortLengthPipe = GetString(cols, shortIndex),
                RejectedShortLengthPipe = GetString(cols, rejShortIndex)
            };

            // Preserve all input rows in output. Only rows with a PO can participate in batch computation.
            rows.Add((line, string.IsNullOrWhiteSpace(record.PoNumber) ? null : record));
        }

        return (headerLine, rows);
    }

    private static int GetMillNo(string[] cols, int index)
    {
        var raw = GetString(cols, index);
        return InputSlitCsvParsing.TryParseMillNo(raw, out var m) ? m : 0;
    }

    private static int GetIntFlexible(string[] cols, int index)
    {
        if (index < 0 || index >= cols.Length)
            return 0;
        return InputSlitCsvParsing.TryParseIntFlexible(cols[index].Trim(), out var v) ? v : 0;
    }

    private static string GetString(string[] cols, int index)
    {
        if (index < 0 || index >= cols.Length)
            return string.Empty;
        return cols[index].Trim();
    }

    private static DateTime? GetDateTime(string[] cols, int index)
    {
        if (index < 0 || index >= cols.Length)
            return null;
        var raw = cols[index].Trim();
        if (string.IsNullOrEmpty(raw))
            return null;
        return DateTime.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var dt) ? dt : null;
    }
}

