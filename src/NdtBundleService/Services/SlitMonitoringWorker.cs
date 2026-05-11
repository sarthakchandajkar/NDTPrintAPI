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
    private readonly PlcPoEndPollHandler _plcPoEndPollHandler;
    private readonly ITraceabilityRepository _traceability;
    private readonly IWipBundleRunningPoProvider _wipRunningPo;
    private readonly IMillNdtCountReader _millNdtCountReader;
    private readonly ILogger<SlitMonitoringWorker> _logger;

    // Simple in-memory set to avoid reprocessing files during one runtime.
    private readonly HashSet<string> _processedFiles = new(StringComparer.OrdinalIgnoreCase);

    public SlitMonitoringWorker(
        IOptions<NdtBundleOptions> options,
        IBundleEngine bundleEngine,
        IBundleOutputWriter outputWriter,
        INdtBatchStateService batchState,
        PlcPoEndPollHandler plcPoEndPollHandler,
        ITraceabilityRepository traceability,
        IWipBundleRunningPoProvider wipRunningPo,
        IMillNdtCountReader millNdtCountReader,
        ILogger<SlitMonitoringWorker> logger)
    {
        _options = options.Value;
        _bundleEngine = bundleEngine;
        _outputWriter = outputWriter;
        _batchState = batchState;
        _plcPoEndPollHandler = plcPoEndPollHandler;
        _traceability = traceability;
        _wipRunningPo = wipRunningPo;
        _millNdtCountReader = millNdtCountReader;
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

                await _plcPoEndPollHandler.PollAsync(stoppingToken).ConfigureAwait(false);
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
        var live = _options.MillSlitLive;

        foreach (var file in files)
        {
            if (_processedFiles.Contains(file))
                continue;

            if (!SourceFileEligibility.IncludeFileUtc(File.GetLastWriteTimeUtc(file), minUtc))
                continue;

            _logger.LogInformation("Processing Input Slit CSV file {File}", file);

            try
            {
                var (headerLine, rows, ndtColumnIndex) = await ReadSlitFileWithRawLinesAsync(file, cancellationToken).ConfigureAwait(false);

                var qualifyingForLive = rows
                    .Select(r => r.Record)
                    .Where(r => r is not null
                                && live.Enabled
                                && r.MillNo == live.ApplyToMillNo
                                && !string.IsNullOrWhiteSpace(r.PoNumber))
                    .ToList();

                int? plcNdt = null;
                string? wipPo = null;
                var liveSingleSlitRow = live.Enabled && qualifyingForLive.Count == 1;
                if (live.Enabled && qualifyingForLive.Count > 1)
                {
                    _logger.LogWarning(
                        "MillSlitLive: file {File} has {N} slit row(s) for mill {M}; PLC NDT applies only to a single slit row per file. Using CSV counts for this file.",
                        file,
                        qualifyingForLive.Count,
                        live.ApplyToMillNo);
                }

                if (liveSingleSlitRow)
                {
                    plcNdt = await _millNdtCountReader.TryReadNdtPipesCountAsync(cancellationToken).ConfigureAwait(false);
                    wipPo = await _wipRunningPo.TryGetRunningPoForMillAsync(live.ApplyToMillNo, cancellationToken).ConfigureAwait(false);
                    if (plcNdt.HasValue)
                    {
                        _logger.LogInformation(
                            "MillSlitLive: using PLC NDT={Ndt} and WIP-folder PO={Po} for mill {Mill} (file {File}).",
                            plcNdt.Value,
                            wipPo ?? "(unchanged)",
                            live.ApplyToMillNo,
                            Path.GetFileName(file));
                    }
                    else
                    {
                        _logger.LogWarning(
                            "MillSlitLive: PLC NDT read failed; using CSV NDT count for mill {Mill} (file {File}).",
                            live.ApplyToMillNo,
                            Path.GetFileName(file));
                    }

                    if (string.IsNullOrWhiteSpace(wipPo))
                    {
                        _logger.LogWarning(
                            "MillSlitLive: no WIP bundle filename PO for mill {Mill}; using slit CSV PO (file {File}).",
                            live.ApplyToMillNo,
                            Path.GetFileName(file));
                    }
                }

                // Build output content: same format as input with one extra column "NDT Batch No".
                var outputLines = new List<string> { headerLine.TrimEnd() + ",NDT Batch No" };
                var inputRowsForSql = new List<(InputSlitRecord Record, int SourceRowNumber)>();
                var outputRowsForSql = new List<(InputSlitRecord Record, string NdtBatchNo, int SourceRowNumber)>();

                var sourceRowNumber = 2; // CSV header is row 1
                string? poOverrideForFileName = null;
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
                    var useLiveThisRow = liveSingleSlitRow
                                         && record.MillNo == live.ApplyToMillNo
                                         && !string.IsNullOrWhiteSpace(record.PoNumber);

                    var effectivePo = useLiveThisRow && !string.IsNullOrWhiteSpace(wipPo)
                        ? InputSlitCsvParsing.NormalizePo(wipPo)
                        : record.PoNumber;
                    var effectiveNdt = useLiveThisRow && plcNdt.HasValue
                        ? plcNdt.Value
                        : record.NdtPipes;

                    if (useLiveThisRow && !string.IsNullOrWhiteSpace(wipPo))
                        poOverrideForFileName = effectivePo;

                    var effectiveRecord = new InputSlitRecord
                    {
                        PoNumber = effectivePo,
                        SlitNo = record.SlitNo,
                        NdtPipes = effectiveNdt,
                        RejectedPipes = record.RejectedPipes,
                        SlitStartTime = record.SlitStartTime,
                        SlitFinishTime = record.SlitFinishTime,
                        MillNo = record.MillNo,
                        NdtShortLengthPipe = record.NdtShortLengthPipe,
                        RejectedShortLengthPipe = record.RejectedShortLengthPipe,
                    };

                    var (batchNumber, _, _) = await _batchState
                        .GetBatchForRecordAsync(effectiveRecord.PoNumber, effectiveRecord.MillNo, effectiveRecord.NdtPipes, cancellationToken)
                        .ConfigureAwait(false);
                    var ndtBatchNoFormatted = FormatNdtBatchNo(batchNumber, effectiveRecord.MillNo);

                    var rawOut = row.RawLine;
                    if (useLiveThisRow && plcNdt.HasValue && ndtColumnIndex >= 0)
                        rawOut = InputSlitCsvParsing.ReplaceFieldAtIndex(row.RawLine, ndtColumnIndex, effectiveNdt.ToString(CultureInfo.InvariantCulture));

                    outputLines.Add(rawOut.TrimEnd() + "," + ndtBatchNoFormatted);
                    inputRowsForSql.Add((record, sourceRowNumber));
                    outputRowsForSql.Add((effectiveRecord, ndtBatchNoFormatted, sourceRowNumber));
                    sourceRowNumber++;

                    // Let the bundle engine decide when a bundle is full; it calls the callback to write CSV and print tag.
                    try
                    {
                        await _bundleEngine.ProcessSlitRecordAsync(effectiveRecord, async (contextRecord, batchNo, totalNdtPcs) =>
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

                // Write one output file: SlitNumber_Date_PONumber.csv (under OutputBundleFolder).
                var outputFolder = _options.OutputBundleFolder;
                string? outputPath = null;
                if (!string.IsNullOrWhiteSpace(outputFolder))
                {
                    Directory.CreateDirectory(outputFolder);
                    var outputFileName = BuildOutputSlitCsvFileName(file, rows, poOverrideForFileName);
                    outputPath = Path.Combine(outputFolder, outputFileName);
                    if (File.Exists(outputPath))
                        outputPath = Path.Combine(outputFolder, Path.GetFileNameWithoutExtension(outputFileName) + "_" + DateTime.Now.ToString("HHmmss", CultureInfo.InvariantCulture) + ".csv");
                    await File.WriteAllLinesAsync(outputPath, outputLines, cancellationToken).ConfigureAwait(false);
                    _logger.LogInformation("Wrote bundle CSV: {Path}", outputPath);
                }

                // Best-effort SQL traceability; CSV flow should not fail if SQL is down.
                await _traceability.RecordInputSlitRowsAsync(file, inputRowsForSql, cancellationToken).ConfigureAwait(false);
                await _traceability.RecordOutputSlitRowsAsync(outputPath ?? file, outputRowsForSql, cancellationToken).ConfigureAwait(false);

                _processedFiles.Add(file);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process Input Slit CSV file {File}", file);
            }
        }
    }

    /// <summary>SlitNumber_Date_PONumber.csv using the first data row with a PO (date from slit times or file mtime).</summary>
    private static string BuildOutputSlitCsvFileName(
        string inputFilePath,
        List<(string RawLine, InputSlitRecord? Record)> rows,
        string? poNumberOverride)
    {
        var first = rows.Select(r => r.Record).FirstOrDefault(r => r is not null && !string.IsNullOrWhiteSpace(r.PoNumber));
        var po = !string.IsNullOrWhiteSpace(poNumberOverride)
            ? poNumberOverride
            : first?.PoNumber ?? "NA";
        var slit = string.IsNullOrWhiteSpace(first?.SlitNo) ? "NoSlit" : first!.SlitNo.Trim();
        var date = (first?.SlitStartTime ?? first?.SlitFinishTime)?.Date
            ?? File.GetLastWriteTime(inputFilePath).Date;
        var safeSlit = CsvOutputFileNaming.SanitizeToken(slit, "NoSlit");
        var safePo = CsvOutputFileNaming.SanitizeToken(po, "NA");
        return $"{safeSlit}_{date:yyyyMMdd}_{safePo}.csv";
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
    private static async Task<(string HeaderLine, List<(string RawLine, InputSlitRecord? Record)> Rows, int NdtColumnIndex)> ReadSlitFileWithRawLinesAsync(
        string path,
        CancellationToken cancellationToken)
    {
        var rows = new List<(string RawLine, InputSlitRecord?)>();

        await using var stream = File.OpenRead(path);
        using var reader = new StreamReader(stream);

        var headerRaw = await reader.ReadLineAsync();
        if (headerRaw is null)
            return (string.Empty, rows, -1);

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

        return (headerLine, rows, ndtIndex);
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
