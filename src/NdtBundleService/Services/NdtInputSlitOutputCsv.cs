using System.Globalization;
using System.Text;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Builds NDT Input Slit output CSV text (Excel-openable) from SQL-shaped rows + NDT Batch No.
/// Same header/columns as Manual NDT Input Slit and SlitMonitoringWorker output.
/// </summary>
public static class NdtInputSlitOutputCsv
{
    public const string Header =
        "PO Number,Slit No,NDT Pipes,Rejected P,Slit Start Time,Slit Finish Time,Mill No,NDT Short Length Pipe,Rejected Short Length Pipe,NDT Batch No";

    public const string SapSlitDateTimeFormat = "dd.MM.yyyy HH:mm:ss";

    public static IReadOnlyList<string> BuildLines(
        IEnumerable<(InputSlitRecord Record, string NdtBatchNo)> rows)
    {
        var lines = new List<string> { Header };
        foreach (var (record, batchNo) in rows)
            lines.Add(FormatDataLine(record, batchNo));
        return lines;
    }

    public static string FormatDataLine(InputSlitRecord record, string ndtBatchNo) =>
        string.Join(",",
            CsvEscape(record.PoNumber),
            CsvEscape(record.SlitNo),
            record.NdtPipes.ToString(CultureInfo.InvariantCulture),
            record.RejectedPipes.ToString(CultureInfo.InvariantCulture),
            CsvEscape(FormatSapDateTime(record.SlitStartTime)),
            CsvEscape(FormatSapDateTime(record.SlitFinishTime)),
            record.MillNo.ToString(CultureInfo.InvariantCulture),
            CsvEscape(record.NdtShortLengthPipe),
            CsvEscape(record.RejectedShortLengthPipe),
            CsvEscape(ndtBatchNo ?? string.Empty));

    public static string FormatSapDateTime(DateTime? value) =>
        value.HasValue
            ? value.Value.ToString(SapSlitDateTimeFormat, CultureInfo.InvariantCulture)
            : string.Empty;

    public static string CsvEscape(string? value)
    {
        var s = value ?? string.Empty;
        if (s.IndexOfAny(new[] { ',', '"', '\r', '\n' }) < 0)
            return s;
        return "\"" + s.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
    }

    public static string ToCsvText(IEnumerable<(InputSlitRecord Record, string NdtBatchNo)> rows)
    {
        var sb = new StringBuilder();
        foreach (var line in BuildLines(rows))
            sb.AppendLine(line);
        return sb.ToString();
    }
}
