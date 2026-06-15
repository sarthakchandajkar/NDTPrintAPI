using NdtBundleService.Models;

namespace NdtBundleService.Services;

internal static class SlitOutputCsvFileNaming
{
    /// <summary>SlitNumber_yyyyMMdd_PONumber.csv using the first data row with a PO.</summary>
    internal static string BuildFileName(string inputFilePath, IEnumerable<InputSlitRecord?> rows)
    {
        var first = rows.FirstOrDefault(r => r is not null && !string.IsNullOrWhiteSpace(r.PoNumber));
        var po = first?.PoNumber ?? "NA";
        var slit = string.IsNullOrWhiteSpace(first?.SlitNo) ? "NoSlit" : first!.SlitNo.Trim();
        var date = (first?.SlitStartTime ?? first?.SlitFinishTime)?.Date
            ?? File.GetLastWriteTime(inputFilePath).Date;
        var safeSlit = CsvOutputFileNaming.SanitizeToken(slit, "NoSlit");
        var safePo = CsvOutputFileNaming.SanitizeToken(po, "NA");
        return $"{safeSlit}_{date:yyyyMMdd}_{safePo}.csv";
    }
}
