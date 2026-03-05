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
    private readonly IPlcClient _plcClient;
    private readonly ILogger<SlitMonitoringWorker> _logger;

    // Per (PO, Mill) running NDT pipe totals and batch numbers for demo batching logic.
    private readonly Dictionary<(string Po, int Mill), int> _totalNdtByKey = new();
    private readonly Dictionary<(string Po, int Mill), int> _currentBatchByKey = new();

    // Simple in-memory set to avoid reprocessing files during one runtime.
    private readonly HashSet<string> _processedFiles = new(StringComparer.OrdinalIgnoreCase);

    public SlitMonitoringWorker(
        IOptions<NdtBundleOptions> options,
        IBundleEngine bundleEngine,
        IBundleOutputWriter outputWriter,
        IPlcClient plcClient,
        ILogger<SlitMonitoringWorker> logger)
    {
        _options = options.Value;
        _bundleEngine = bundleEngine;
        _outputWriter = outputWriter;
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

                // Poll PO-end signal; for now we do not know PO context for the bit, so this is a placeholder.
                // A real implementation would read current PO/mill context and call HandlePoEndAsync accordingly.
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
                var records = await ReadSlitFileAsync(file, cancellationToken).ConfigureAwait(false);

                foreach (var record in records)
                {
                    // Let the engine maintain its own internal state (for PO-end etc.),
                    // but do not rely on its callbacks for CSV output in this demo.
                    await _bundleEngine.ProcessSlitRecordAsync(
                        record,
                        (_, _, _) => Task.CompletedTask,
                        cancellationToken).ConfigureAwait(false);

                    await HandlePerRecordOutputAsync(record, cancellationToken).ConfigureAwait(false);
                }

                _processedFiles.Add(file);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process Input Slit CSV file {File}", file);
            }
        }
    }

    private async Task HandlePerRecordOutputAsync(InputSlitRecord record, CancellationToken cancellationToken)
    {
        const int threshold = 10; // NDTPcsPerBundle for Pipe Size 2.5 in this demo

        var key = (record.PoNumber, record.MillNo);

        if (!_currentBatchByKey.TryGetValue(key, out var currentBatch))
        {
            currentBatch = 1;
            _currentBatchByKey[key] = currentBatch;
            _totalNdtByKey[key] = 0;
        }

        // Update running NDT count for this PO/mill.
        _totalNdtByKey[key] = _totalNdtByKey[key] + record.NdtPipes;
        var totalSoFar = _totalNdtByKey[key];

        // Compute batch number so it only increments when total NDT pipes crosses multiples of the threshold.
        var sequence = Math.Max(1, ((totalSoFar - 1) / threshold) + 1);
        _currentBatchByKey[key] = sequence;

        // Always write one CSV per incoming slit file, mirroring the input format with an extra NDT Batch No column.
        await WritePerRecordCsvAsync(record, sequence, cancellationToken).ConfigureAwait(false);

        // When we reach an exact multiple of the threshold, this represents a full bundle and a tag print.
        if (totalSoFar % threshold == 0)
        {
            var ndtBatchNoFormatted = FormatNdtBatchNo(sequence, _options.ShopId);
            // Use the existing output writer to trigger the (stub) label printer for the tag.
            await _outputWriter.WriteBundleAsync(record, sequence, totalSoFar, cancellationToken).ConfigureAwait(false);
        }
    }

    private static string FormatNdtBatchNo(int sequenceNumber, string? shopIdRaw)
    {
        var yy = (DateTime.Now.Year % 100).ToString("D2", CultureInfo.InvariantCulture);
        var raw = (shopIdRaw ?? "01").Trim();
        var shopId = raw.Length >= 2 ? raw[..2].PadLeft(2, '0') : raw.PadLeft(2, '0');
        var seq = sequenceNumber.ToString("D5", CultureInfo.InvariantCulture);
        return "0" + yy + shopId + seq;
    }

    private async Task WritePerRecordCsvAsync(InputSlitRecord record, int ndtBatchNo, CancellationToken cancellationToken)
    {
        var folder = _options.OutputBundleFolder;
        if (string.IsNullOrWhiteSpace(folder))
        {
            _logger.LogWarning("OutputBundleFolder is not configured; per-record CSV will not be written.");
            return;
        }

        Directory.CreateDirectory(folder);

        var date = record.SlitStartTime?.Date ?? DateTime.Now.Date;
        var datePart = date.ToString("yyMMdd", CultureInfo.InvariantCulture);
        var slitPart = string.IsNullOrWhiteSpace(record.SlitNo) ? "POEnd" : record.SlitNo;
        var fileName = $"{slitPart}_{datePart}_{record.PoNumber}.csv";
        var path = Path.Combine(folder, fileName);

        var ndtBatchNoFormatted = FormatNdtBatchNo(ndtBatchNo, _options.ShopId);

        var lines = new List<string>
        {
            "PO Number,Slit No,NDT Pipes,Rejected P,Slit Start Time,Slit Finish Time,Mill No,NDT Short Length Pipe,Rejected Short Length Pipe,NDT Batch No"
        };

        string FormatDate(DateTime? dt)
        {
            if (dt is null) return string.Empty;
            return dt.Value.ToString("dd.MM.yyyy HH:mm:ss", CultureInfo.InvariantCulture);
        }

        var line = string.Join(",",
            Escape(record.PoNumber),
            Escape(record.SlitNo),
            record.NdtPipes.ToString(CultureInfo.InvariantCulture),
            record.RejectedPipes.ToString(CultureInfo.InvariantCulture),
            Escape(FormatDate(record.SlitStartTime)),
            Escape(FormatDate(record.SlitFinishTime)),
            record.MillNo.ToString(CultureInfo.InvariantCulture),
            Escape(record.NdtShortLengthPipe),
            Escape(record.RejectedShortLengthPipe),
            ndtBatchNoFormatted);

        lines.Add(line);

        await File.WriteAllLinesAsync(path, lines, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation("Wrote per-record bundle CSV: {Path}", path);
    }

    private static string Escape(string value)
    {
        if (value.IndexOfAny(new[] { ',', '"', '\r', '\n' }) >= 0)
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        return value;
    }

    private static async Task<IReadOnlyList<InputSlitRecord>> ReadSlitFileAsync(string path, CancellationToken cancellationToken)
    {
        var result = new List<InputSlitRecord>();

        await using var stream = File.OpenRead(path);
        using var reader = new StreamReader(stream);

        var headerLine = await reader.ReadLineAsync();
        if (headerLine is null)
            return result;

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
                result.Add(record);
        }

        return result;
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

