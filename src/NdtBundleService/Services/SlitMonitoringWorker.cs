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
/// At startup, each CSV already in the inbox is recorded with its current LastWriteTimeUtc so we do not backlog-generate outputs.
/// When the same path is written again (newer LastWriteTimeUtc), it is processed. Brand-new paths are processed when they appear.
/// Inbox files may have no extension (SAP) or <c>.csv</c>; only reads them—never moves or deletes source files in <see cref="NdtBundleOptions.InputSlitFolder"/>.
/// </summary>
public sealed class SlitMonitoringWorker : BackgroundService
{
    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly IBundleEngine _bundleEngine;
    private readonly IBundleOutputWriter _outputWriter;
    private readonly INdtBatchStateService _batchState;
    private readonly PlcPoEndPollHandler _plcPoEndPollHandler;
    private readonly ITraceabilityRepository _traceability;
    private readonly IWipBundleRunningPoProvider _wipRunningPo;
    private readonly IMillNdtCountReader _millNdtCountReader;
    private readonly ILogger<SlitMonitoringWorker> _logger;

    // Per input path: last LastWriteTimeUtc we treated as fully handled (seed baseline or successful run).
    // Same path with a newer timestamp (SAP overwrite / same-name export) is processed again; unchanged files are skipped.
    private readonly Dictionary<string, DateTime> _inputSlitLastHandledWriteUtc = new(StringComparer.OrdinalIgnoreCase);

    public SlitMonitoringWorker(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        IBundleEngine bundleEngine,
        IBundleOutputWriter outputWriter,
        INdtBatchStateService batchState,
        PlcPoEndPollHandler plcPoEndPollHandler,
        ITraceabilityRepository traceability,
        IWipBundleRunningPoProvider wipRunningPo,
        IMillNdtCountReader millNdtCountReader,
        ILogger<SlitMonitoringWorker> logger)
    {
        _optionsMonitor = optionsMonitor;
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
        if (!await WaitForInputSlitFolderExistsAsync(stoppingToken).ConfigureAwait(false))
            return;

        if (stoppingToken.IsCancellationRequested)
            return;

        var inbox = (_optionsMonitor.CurrentValue.InputSlitFolder ?? string.Empty).Trim();
        _logger.LogInformation("SlitMonitoringWorker started. Watching folder {Folder}", inbox);

        SeedPreExistingInputSlitCsvsAsProcessed();

        var outputFolder = (_optionsMonitor.CurrentValue.OutputBundleFolder ?? string.Empty).Trim();
        if (string.IsNullOrEmpty(outputFolder))
        {
            _logger.LogWarning(
                "NdtBundle:OutputBundleFolder is not set. NDT Input Slit CSV files (with NDT Batch No) will not be written. " +
                "Set it to e.g. Z:\\To SAP\\TM\\NDT\\NDT Input Slit\\Input Slit.");
        }
        else
        {
            _logger.LogInformation(
                "NDT Input Slit output CSV folder (NdtBundle:OutputBundleFolder): {Folder}",
                outputFolder);
        }

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

            var delaySeconds = Math.Max(1, _optionsMonitor.CurrentValue.PollIntervalSeconds);
            await Task.Delay(TimeSpan.FromSeconds(delaySeconds), stoppingToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Waits until <see cref="NdtBundleOptions.InputSlitFolder"/> exists. Mapped drives (e.g. Z:\) often appear only after
    /// user logon; Local System never sees them—use a service account with the drive or a UNC path.
    /// </summary>
    private async Task<bool> WaitForInputSlitFolderExistsAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var folder = (_optionsMonitor.CurrentValue.InputSlitFolder ?? string.Empty).Trim();
            if (string.IsNullOrEmpty(folder))
            {
                _logger.LogWarning("InputSlitFolder is not configured. SlitMonitoringWorker will not process files.");
                return false;
            }

            try
            {
                if (Directory.Exists(folder))
                    return true;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error while checking Input Slit folder {Folder}.", folder);
            }

            _logger.LogWarning(
                "InputSlitFolder is not reachable: {Folder}. Mapped drives (Z:\\) are per-user—run the service under that user or switch to a UNC path. Retrying in 10s.",
                folder);
            await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken).ConfigureAwait(false);
        }

        return false;
    }

    /// <summary>
    /// Records each existing slit inbox file (no extension or <c>.csv</c>) with its current LastWriteTimeUtc so we do not backlog-generate NDT outputs for slits
    /// that completed before this process started. Overwrites of the same path (newer timestamp) are picked up on later polls.
    /// </summary>
    private void SeedPreExistingInputSlitCsvsAsProcessed()
    {
        var folder = (_optionsMonitor.CurrentValue.InputSlitFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return;

        foreach (var path in InputSlitInboxEnumeration.EnumerateFiles(folder))
        {
            var full = Path.GetFullPath(path);
            try
            {
                _inputSlitLastHandledWriteUtc[full] = File.GetLastWriteTimeUtc(full);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Input Slit seed: could not read LastWriteTimeUtc for {File}", full);
            }
        }

        var n = _inputSlitLastHandledWriteUtc.Count;
        if (n > 0)
        {
            _logger.LogInformation(
                "Input Slit: recorded {Count} slit file(s) already in {Folder} at service start (no NDT output for those versions). New paths or same path with a newer LastWriteTimeUtc will be processed.",
                n,
                folder);
        }
        else
        {
            _logger.LogInformation("Input Slit: no pre-existing slit files in {Folder} at service start.", folder);
        }
    }

    private async Task ProcessNewSlitFilesAsync(CancellationToken cancellationToken)
    {
        var o = _optionsMonitor.CurrentValue;
        var inputFolder = (o.InputSlitFolder ?? string.Empty).Trim();
        if (string.IsNullOrEmpty(inputFolder) || !Directory.Exists(inputFolder))
        {
            _logger.LogWarning(
                "Input Slit folder is not available this poll cycle: {Folder}. Skipping until it is reachable again.",
                string.IsNullOrEmpty(inputFolder) ? "(not configured)" : inputFolder);
            return;
        }

        IEnumerable<string> filesEnumerable;
        try
        {
            filesEnumerable = InputSlitInboxEnumeration.EnumerateFiles(inputFolder);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Cannot enumerate slit files in Input Slit folder {Folder}.", inputFolder);
            return;
        }

        var minUtc = SourceFileEligibility.ParseMinUtc(o);
        var live = o.MillSlitLive;

        foreach (var file in filesEnumerable)
        {
            var fileFull = Path.GetFullPath(file);
            DateTime lwUtc;
            try
            {
                lwUtc = File.GetLastWriteTimeUtc(fileFull);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Input Slit: could not read LastWriteTimeUtc for {File}", fileFull);
                continue;
            }

            if (_inputSlitLastHandledWriteUtc.TryGetValue(fileFull, out var lastHandledUtc) && lwUtc <= lastHandledUtc)
                continue;

            if (!SourceFileEligibility.IncludeFileUtc(lwUtc, minUtc))
            {
                if (minUtc.HasValue)
                {
                    _logger.LogWarning(
                        "Skipping Input Slit file {File}: LastWriteUtc {LastWrite:o} is before NdtBundle:MinSourceFileLastWriteUtc {Min:o}. Clear or update MinSourceFileLastWriteUtc if this file should be processed.",
                        fileFull,
                        lwUtc,
                        minUtc.Value);
                }

                continue;
            }

            _logger.LogInformation("Processing Input Slit file {File}", fileFull);

            try
            {
                var (headerLine, rows, ndtColumnIndex) = await ReadSlitFileWithRawLinesAsync(fileFull, cancellationToken).ConfigureAwait(false);

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
                        fileFull,
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
                            Path.GetFileName(fileFull));
                    }
                    else
                    {
                        _logger.LogWarning(
                            "MillSlitLive: PLC NDT read failed; using CSV NDT count for mill {Mill} (file {File}).",
                            live.ApplyToMillNo,
                            Path.GetFileName(fileFull));
                    }

                    if (string.IsNullOrWhiteSpace(wipPo))
                    {
                        _logger.LogWarning(
                            "MillSlitLive: no WIP bundle filename PO for mill {Mill}; using slit CSV PO (file {File}).",
                            live.ApplyToMillNo,
                            Path.GetFileName(fileFull));
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

                    // MillSlitLive: PLC value is for live display in the output NDT Pipes column only.
                    // NDT Batch No, bundle state, and tag printing follow the Input Slit CSV row (slit end),
                    // so thresholds apply to whole-slit totals (e.g. 9 + 6 → one label for 15), not PLC step deltas.
                    var bundleNdtPipes = useLiveThisRow && plcNdt.HasValue ? record.NdtPipes : effectiveRecord.NdtPipes;
                    var bundleRecord = CloneRecordWithNdt(effectiveRecord, bundleNdtPipes);

                    var (bn, _, _) = await _batchState
                        .GetBatchForRecordAsync(
                            bundleRecord.PoNumber,
                            bundleRecord.MillNo,
                            bundleRecord.NdtPipes,
                            cancellationToken)
                        .ConfigureAwait(false);
                    var batchNumber = bn;

                    try
                    {
                        await _bundleEngine.ProcessSlitRecordAsync(
                            bundleRecord,
                            async (contextRecord, batchNo, totalNdtPcs) =>
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
                            },
                            cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Bundle engine failed for record in {File}; output CSV was already written.", fileFull);
                    }

                    var ndtBatchNoFormatted = FormatNdtBatchNo(batchNumber, effectiveRecord.MillNo);

                    var rawOut = row.RawLine;
                    if (useLiveThisRow && plcNdt.HasValue && ndtColumnIndex >= 0)
                        rawOut = InputSlitCsvParsing.ReplaceFieldAtIndex(row.RawLine, ndtColumnIndex, effectiveNdt.ToString(CultureInfo.InvariantCulture));

                    outputLines.Add(rawOut.TrimEnd() + "," + ndtBatchNoFormatted);
                    inputRowsForSql.Add((record, sourceRowNumber));
                    outputRowsForSql.Add((bundleRecord, ndtBatchNoFormatted, sourceRowNumber));
                    sourceRowNumber++;
                }

                // Write one output file: SlitNumber_Date_PONumber.csv (under OutputBundleFolder).
                var outputFolder = (o.OutputBundleFolder ?? string.Empty).Trim();
                string? outputPath = null;
                if (!string.IsNullOrWhiteSpace(outputFolder))
                {
                    Directory.CreateDirectory(outputFolder);
                    var outputFileName = BuildOutputSlitCsvFileName(fileFull, rows, poOverrideForFileName);
                    outputPath = Path.Combine(outputFolder, outputFileName);
                    if (File.Exists(outputPath))
                        outputPath = Path.Combine(outputFolder, Path.GetFileNameWithoutExtension(outputFileName) + "_" + DateTime.Now.ToString("HHmmss", CultureInfo.InvariantCulture) + ".csv");
                    await File.WriteAllLinesAsync(outputPath, outputLines, cancellationToken).ConfigureAwait(false);
                    _logger.LogInformation("Wrote bundle CSV: {Path}", outputPath);
                }

                // Best-effort SQL traceability; CSV flow should not fail if SQL is down.
                await _traceability.RecordInputSlitRowsAsync(fileFull, inputRowsForSql, cancellationToken).ConfigureAwait(false);
                await _traceability.RecordOutputSlitRowsAsync(outputPath ?? fileFull, outputRowsForSql, cancellationToken).ConfigureAwait(false);

                _inputSlitLastHandledWriteUtc[fileFull] = File.GetLastWriteTimeUtc(fileFull);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process Input Slit file {File}", fileFull);
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

    private static InputSlitRecord CloneRecordWithNdt(InputSlitRecord r, int ndtPipes) =>
        new()
        {
            PoNumber = r.PoNumber,
            SlitNo = r.SlitNo,
            NdtPipes = ndtPipes,
            RejectedPipes = r.RejectedPipes,
            SlitStartTime = r.SlitStartTime,
            SlitFinishTime = r.SlitFinishTime,
            MillNo = r.MillNo,
            NdtShortLengthPipe = r.NdtShortLengthPipe,
            RejectedShortLengthPipe = r.RejectedShortLengthPipe,
        };

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
