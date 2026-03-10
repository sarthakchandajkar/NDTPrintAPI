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
    private readonly ILogger<SlitMonitoringWorker> _logger;

    // Simple in-memory set to avoid reprocessing files during one runtime.
    private readonly HashSet<string> _processedFiles = new(StringComparer.OrdinalIgnoreCase);

    public SlitMonitoringWorker(
        IOptions<NdtBundleOptions> options,
        IBundleEngine bundleEngine,
        IBundleOutputWriter outputWriter,
        INdtBatchStateService batchState,
        IPlcClient plcClient,
        ILogger<SlitMonitoringWorker> logger)
        : this(options, bundleEngine, outputWriter, batchState, null, plcClient, logger) { }

    public SlitMonitoringWorker(
        IOptions<NdtBundleOptions> options,
        IBundleEngine bundleEngine,
        IBundleOutputWriter outputWriter,
        INdtBatchStateService batchState,
        ICurrentPoPlanService? currentPoPlanService,
        IPlcClient plcClient,
        ILogger<SlitMonitoringWorker> logger)
    {
        _options = options.Value;
        _bundleEngine = bundleEngine;
        _outputWriter = outputWriter;
        _batchState = batchState;
        _currentPoPlanService = currentPoPlanService;
        _plcClient = plcClient;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (string.IsNullOrWhiteSpace(_options.InputSlitFolder))
        {
            _logger.LogWarning("InputSlitFolder is not configured. Worker will not process any files.");
            return;
        }

        Directory.CreateDirectory(_options.InputSlitFolder);

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

        foreach (var file in files)
        {
            if (_processedFiles.Contains(file))
                continue;

            _logger.LogInformation("Processing Input Slit CSV file {File}", file);

            try
            {
                var (headerLine, rows) = await ReadSlitFileWithRawLinesAsync(file, cancellationToken).ConfigureAwait(false);

                if (rows.Count == 0)
                {
                    _logger.LogWarning("Input Slit CSV file {File} has no valid data rows (or PO Number empty). Skipping output.", file);
                    _processedFiles.Add(file);
                    continue;
                }

                // When PoPlanFolder is set: only process records for the current PO. Leave file unprocessed if no match so we retry when that PO becomes current.
                List<(string RawLine, InputSlitRecord Record)> rowsToProcess = rows;
                if (!string.IsNullOrWhiteSpace(_options.PoPlanFolder) && _currentPoPlanService != null)
                {
                    var currentPo = await _currentPoPlanService.GetCurrentPoNumberAsync(cancellationToken).ConfigureAwait(false);
                    if (string.IsNullOrEmpty(currentPo))
                    {
                        _logger.LogDebug("No current PO from PoPlanFolder; skipping slit file {File}.", file);
                        continue;
                    }
                    rowsToProcess = rows.Where(r => r.Record.PoNumber.Equals(currentPo.Trim(), StringComparison.OrdinalIgnoreCase)).ToList();
                    if (rowsToProcess.Count == 0)
                    {
                        _logger.LogDebug("Slit file {File} has no rows for current PO {PO}; leaving unprocessed until that PO is current.", file, currentPo);
                        continue;
                    }
                }

                // Build output content: same format as input with one extra column "NDT Batch No".
                var outputLines = new List<string> { headerLine.TrimEnd() + ",NDT Batch No" };

                foreach (var (rawLine, record) in rowsToProcess)
                {
                    var (batchNumber, totalSoFar, threshold) = await _batchState.GetBatchForRecordAsync(record.PoNumber, record.MillNo, record.NdtPipes, cancellationToken).ConfigureAwait(false);
                    var ndtBatchNoFormatted = FormatNdtBatchNo(batchNumber, _options.ShopId);
                    outputLines.Add(rawLine.TrimEnd() + "," + ndtBatchNoFormatted);

                    // Let the bundle engine decide when a bundle is full; it calls the callback to write CSV and print tag.
                    try
                    {
                        await _bundleEngine.ProcessSlitRecordAsync(record, async (contextRecord, batchNo, totalNdtPcs) =>
                        {
                            try
                            {
                                await _outputWriter.WriteBundleAsync(contextRecord, batchNo, totalNdtPcs, cancellationToken).ConfigureAwait(false);
                                _logger.LogInformation("Auto-print triggered for bundle {BatchNo} ({Pcs} pcs).", FormatNdtBatchNo(batchNo, _options.ShopId), totalNdtPcs);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, "Tag print failed for bundle {BatchNo}.", FormatNdtBatchNo(batchNo, _options.ShopId));
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

                _processedFiles.Add(file);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process Input Slit CSV file {File}", file);
            }
        }
    }

    private static string FormatNdtBatchNo(int sequenceNumber, string? shopIdRaw)
    {
        var yy = (DateTime.Now.Year % 100).ToString("D2", CultureInfo.InvariantCulture);
        var raw = (shopIdRaw ?? "01").Trim();
        var shopId = raw.Length >= 2 ? raw[..2].PadLeft(2, '0') : raw.PadLeft(2, '0');
        var seq = sequenceNumber.ToString("D5", CultureInfo.InvariantCulture);
        return "9" + yy + shopId + seq;
    }

    /// <summary>
    /// Reads slit CSV and returns the header line and a list of (raw data line, parsed record) for each valid row.
    /// </summary>
    private static async Task<(string HeaderLine, List<(string RawLine, InputSlitRecord Record)> Rows)> ReadSlitFileWithRawLinesAsync(string path, CancellationToken cancellationToken)
    {
        var rows = new List<(string RawLine, InputSlitRecord Record)>();

        await using var stream = File.OpenRead(path);
        using var reader = new StreamReader(stream);

        var headerLine = await reader.ReadLineAsync();
        if (headerLine is null)
            return (string.Empty, rows);

        var headers = headerLine.Split(',');

        int poIndex = GetIndex(headers, "PO Number");
        int slitIndex = GetIndex(headers, "Slit No");
        int ndtIndex = GetIndex(headers, "NDT Pipes");
        int rejectedIndex = GetIndex(headers, "Rejected P");
        int startIndex = GetIndex(headers, "Slit Start Time");
        int finishIndex = GetIndex(headers, "Slit Finish Time");
        int millIndex = GetIndex(headers, "Mill No");
        int shortIndex = GetIndex(headers, "NDT Short Length Pipe");
        int rejShortIndex = GetIndex(headers, "Rejected Short Length Pipe");

        while (!reader.EndOfStream)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var line = await reader.ReadLineAsync();
            if (string.IsNullOrWhiteSpace(line))
                continue;

            var cols = line.Split(',');
            if (cols.Length == 0)
                continue;

            var record = new InputSlitRecord
            {
                PoNumber = GetString(cols, poIndex),
                SlitNo = GetString(cols, slitIndex),
                NdtPipes = GetInt(cols, ndtIndex),
                RejectedPipes = GetInt(cols, rejectedIndex),
                SlitStartTime = GetDateTime(cols, startIndex),
                SlitFinishTime = GetDateTime(cols, finishIndex),
                MillNo = GetInt(cols, millIndex),
                NdtShortLengthPipe = GetString(cols, shortIndex),
                RejectedShortLengthPipe = GetString(cols, rejShortIndex)
            };

            if (!string.IsNullOrWhiteSpace(record.PoNumber))
                rows.Add((line, record));
        }

        return (headerLine, rows);
    }

    private static int GetIndex(IReadOnlyList<string> headers, string name)
    {
        for (var i = 0; i < headers.Count; i++)
        {
            if (headers[i].Trim().Equals(name, StringComparison.OrdinalIgnoreCase))
                return i;
        }

        return -1;
    }

    private static string GetString(string[] cols, int index)
    {
        if (index < 0 || index >= cols.Length)
            return string.Empty;
        return cols[index].Trim();
    }

    private static int GetInt(string[] cols, int index)
    {
        if (index < 0 || index >= cols.Length)
            return 0;
        return int.TryParse(cols[index].Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) ? value : 0;
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

