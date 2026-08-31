using System.Globalization;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// CSV line written for a closed bundle. Shared by <see cref="CsvBundleOutputWriter"/> and lastRecord tests.
/// </summary>
public static class BundleCloseCsv
{
    public const string Header =
        "PO Number,Slit No,NDT Pipes,Rejected P,Slit Start Time,Slit Finish Time,Mill No,NDT Short Length Pipe,Rejected Short Length Pipe,NDT Batch No";

    public static string FormatLine(InputSlitRecord context, int totalNdtPcs, string ndtBatchNoFormatted) =>
        string.Join(",",
            Escape(context.PoNumber),
            Escape(context.SlitNo),
            totalNdtPcs.ToString(CultureInfo.InvariantCulture),
            context.RejectedPipes.ToString(CultureInfo.InvariantCulture),
            Escape(context.SlitStartTime?.ToString("O") ?? string.Empty),
            Escape(context.SlitFinishTime?.ToString("O") ?? string.Empty),
            context.MillNo.ToString(CultureInfo.InvariantCulture),
            Escape(context.NdtShortLengthPipe),
            Escape(context.RejectedShortLengthPipe),
            ndtBatchNoFormatted);

    public static string Escape(string value)
    {
        if (value.IndexOfAny(new[] { ',', '"', '\r', '\n' }) >= 0)
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        return value;
    }
}
