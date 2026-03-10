using System.Globalization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Writes one CSV file per completed bundle and sends the tag to the printer (ZPL) when configured.
/// </summary>
public sealed class CsvBundleOutputWriter : IBundleOutputWriter
{
    private readonly NdtBundleOptions _options;
    private readonly INdtBundleRepository _bundleRepository;
    private readonly INdtTagPrinter? _tagPrinter;
    private readonly ILogger<CsvBundleOutputWriter> _logger;

    public CsvBundleOutputWriter(
        IOptions<NdtBundleOptions> options,
        INdtBundleRepository bundleRepository,
        ILogger<CsvBundleOutputWriter> logger,
        INdtTagPrinter? tagPrinter = null)
    {
        _options = options.Value;
        _bundleRepository = bundleRepository;
        _tagPrinter = tagPrinter;
        _logger = logger;
    }

    public async Task WriteBundleAsync(InputSlitRecord contextRecord, int ndtBatchNo, int totalNdtPcs, CancellationToken cancellationToken)
    {
        var folder = _options.OutputBundleFolder;
        if (string.IsNullOrWhiteSpace(folder))
        {
            _logger.LogWarning("OutputBundleFolder is not configured; bundle CSV will not be written.");
            return;
        }

        Directory.CreateDirectory(folder);

        // NDT_Batch_No format: 10 chars = 0 (fixed) + YY (year) + ShopId (01-04) + Sequence (5 digits)
        var ndtBatchNoFormatted = FormatNdtBatchNo(ndtBatchNo);

        // Write bundle summary to a distinct file (actual total for this bundle, e.g. 11 pipes).
        // Do not overwrite per-slit output files (SlitNo_date_PO.csv) which keep per-row NDT pipe counts.
        var fileName = $"NDT_Bundle_{ndtBatchNoFormatted}.csv";
        var path = Path.Combine(folder, fileName);

        var lines = new List<string>
        {
            "PO Number,Slit No,NDT Pipes,Rejected P,Slit Start Time,Slit Finish Time,Mill No,NDT Short Length Pipe,Rejected Short Length Pipe,NDT Batch No"
        };

        var line = string.Join(",",
            Escape(contextRecord.PoNumber),
            Escape(contextRecord.SlitNo),
            totalNdtPcs.ToString(CultureInfo.InvariantCulture),
            contextRecord.RejectedPipes.ToString(CultureInfo.InvariantCulture),
            Escape(contextRecord.SlitStartTime?.ToString("O") ?? string.Empty),
            Escape(contextRecord.SlitFinishTime?.ToString("O") ?? string.Empty),
            contextRecord.MillNo.ToString(CultureInfo.InvariantCulture),
            Escape(contextRecord.NdtShortLengthPipe),
            Escape(contextRecord.RejectedShortLengthPipe),
            ndtBatchNoFormatted);

        lines.Add(line);

        await File.WriteAllLinesAsync(path, lines, cancellationToken);
        _logger.LogInformation("Wrote bundle CSV: {Path}", path);

        var record = new NdtBundleRecord
        {
            BundleNo = ndtBatchNoFormatted,
            PoNumber = contextRecord.PoNumber,
            MillNo = contextRecord.MillNo,
            TotalNdtPcs = totalNdtPcs,
            SlitNo = contextRecord.SlitNo,
            SlitStartTime = contextRecord.SlitStartTime,
            SlitFinishTime = contextRecord.SlitFinishTime,
            RejectedPipes = contextRecord.RejectedPipes,
            NdtShortLengthPipe = contextRecord.NdtShortLengthPipe,
            RejectedShortLengthPipe = contextRecord.RejectedShortLengthPipe
        };
        await _bundleRepository.RecordBundleAsync(record, cancellationToken).ConfigureAwait(false);

        if (_tagPrinter != null)
        {
            try
            {
                await _tagPrinter.PrintBundleTagAsync(contextRecord, ndtBatchNo, totalNdtPcs, isReprint: false, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Auto-print failed for bundle {BatchNo}.", ndtBatchNoFormatted);
            }
        }
    }

    /// <summary>
    /// Formats NDT_Batch_No per spec: 10 characters.
    /// Position 1: Fixed "9"
    /// Position 2-3: YY (2-digit year)
    /// Position 4-5: Shop ID (01, 02, 03, 04)
    /// Position 6-10: Sequence number (5 digits, zero-padded)
    /// </summary>
    private string FormatNdtBatchNo(int sequenceNumber)
    {
        var yy = (DateTime.Now.Year % 100).ToString("D2", CultureInfo.InvariantCulture);
        var raw = (_options.ShopId ?? "01").Trim();
        var shopId = raw.Length >= 2 ? raw[..2].PadLeft(2, '0') : raw.PadLeft(2, '0');
        var seq = sequenceNumber.ToString("D5", CultureInfo.InvariantCulture);
        return "9" + yy + shopId + seq;
    }

    private static string Escape(string value)
    {
        if (value.IndexOfAny(new[] { ',', '"', '\r', '\n' }) >= 0)
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        return value;
    }
}

