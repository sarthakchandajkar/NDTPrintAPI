using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using NdtBundleService.Configuration;
using NdtBundleService.Controllers;

namespace NdtBundleService.Services;

/// <summary>
/// When the running PO comes from TM bundle filenames, the PO plan merge may not include that PO.
/// Reads the latest matching <c>WIP_MM_…</c> CSV in Bundle / Bundle Accepted folders and fills missing WIP columns on <see cref="TestController.WipByMillRowDto"/>.
/// </summary>
public static class WipBundleWipCsvEnricher
{
    private static readonly Regex ReFull = new(
        @"^WIP_(\d{2})_(\d+)_(\d+)_(\d{6})_(\d{6})(\.csv)?$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex ReShort = new(
        @"^WIP_(\d{2})_(\d+)_(\d{6})_(\d{6})\.csv$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    public static async Task TryEnrichRowAsync(
        TestController.WipByMillRowDto row,
        MillSlitLiveOptions live,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(row.PoNumber) || row.MillNo is < 1 or > 4)
            return;

        if (!string.IsNullOrWhiteSpace(row.PipeSize))
            return;

        var bundle = (live.WipBundleFolder ?? string.Empty).Trim();
        var accepted = (live.WipBundleAcceptedFolder ?? string.Empty).Trim();
        var files = new List<FileInfo>();
        foreach (var folder in new[] { bundle, accepted })
        {
            if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
                continue;
            foreach (var path in Directory.EnumerateFiles(folder))
            {
                var name = Path.GetFileName(path);
                if (!name.StartsWith("WIP_", StringComparison.OrdinalIgnoreCase))
                    continue;
                var meta = ParseName(name);
                if (meta is null)
                    continue;
                if (!int.TryParse(meta.MillDigits, NumberStyles.Integer, CultureInfo.InvariantCulture, out var mm) || mm != row.MillNo)
                    continue;
                if (!InputSlitCsvParsing.PoEquals(meta.PoNumber, row.PoNumber))
                    continue;
                try
                {
                    files.Add(new FileInfo(path));
                }
                catch
                {
                    /* ignore */
                }
            }
        }

        if (files.Count == 0)
            return;

        foreach (var fi in files.OrderByDescending(f => f.LastWriteTimeUtc).ThenByDescending(f => f.FullName, StringComparer.OrdinalIgnoreCase))
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                if (await TryApplyFirstMatchingRowAsync(fi.FullName, row, cancellationToken).ConfigureAwait(false))
                {
                    logger.LogDebug("WIP bundle CSV enriched row for mill {Mill} PO {Po} from {File}.", row.MillNo, row.PoNumber, fi.Name);
                    return;
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed reading bundle WIP CSV {File} for enrichment.", fi.FullName);
            }
        }
    }

    private static async Task<bool> TryApplyFirstMatchingRowAsync(string filePath, TestController.WipByMillRowDto row, CancellationToken cancellationToken)
    {
        await using var stream = File.OpenRead(filePath);
        using var reader = new StreamReader(stream);
        var headerRaw = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
        if (headerRaw is null)
            return false;

        var headers = headerRaw.Split(',');
        static int HeaderIndex(IReadOnlyList<string> hdrs, params string[] names)
        {
            foreach (var name in names)
            {
                for (var i = 0; i < hdrs.Count; i++)
                {
                    if (string.Equals(hdrs[i].Trim(), name, StringComparison.OrdinalIgnoreCase))
                        return i;
                }
            }

            return -1;
        }

        var poIdx = HeaderIndex(headers, "PO_No", "PO Number", "PO No");
        var millIdx = HeaderIndex(headers, "Mill Number", "Mill No");
        if (millIdx < 0 || poIdx < 0)
            return false;

        int Idx(string columnName) => HeaderIndex(headers, columnName);

        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;
            var cols = line.Split(',');
            if (cols.Length <= Math.Max(millIdx, poIdx))
                continue;
            var millRaw = cols[millIdx].Trim();
            if (!InputSlitCsvParsing.TryParseMillNo(millRaw, out var millNo) || millNo != row.MillNo)
                continue;
            var po = poIdx >= 0 && poIdx < cols.Length ? cols[poIdx].Trim() : string.Empty;
            if (string.IsNullOrWhiteSpace(po) || !InputSlitCsvParsing.PoEquals(po, row.PoNumber))
                continue;

            string Cell(int idx) => idx >= 0 && idx < cols.Length ? cols[idx].Trim() : string.Empty;

            if (string.IsNullOrWhiteSpace(row.PlannedMonth))
                row.PlannedMonth = Cell(Idx("Planned Month"));
            if (string.IsNullOrWhiteSpace(row.PipeGrade))
                row.PipeGrade = Cell(Idx("Pipe Grade"));
            if (string.IsNullOrWhiteSpace(row.PipeSize))
                row.PipeSize = Cell(Idx("Pipe Size"));
            if (string.IsNullOrWhiteSpace(row.PipeLength))
                row.PipeLength = Cell(Idx("Pipe Length"));
            if (string.IsNullOrWhiteSpace(row.PiecesPerBundle))
                row.PiecesPerBundle = Cell(Idx("Pieces Per Bundle"));
            if (string.IsNullOrWhiteSpace(row.TotalPieces))
                row.TotalPieces = Cell(Idx("Total Pieces"));

            return !string.IsNullOrWhiteSpace(row.PipeSize)
                   || !string.IsNullOrWhiteSpace(row.PipeGrade)
                   || !string.IsNullOrWhiteSpace(row.PlannedMonth);
        }

        return false;
    }

    private sealed record WipMeta(string MillDigits, string PoNumber);

    private static WipMeta? ParseName(string fileName)
    {
        var m = ReFull.Match(fileName);
        if (m.Success)
            return new WipMeta(m.Groups[1].Value, m.Groups[2].Value);

        m = ReShort.Match(fileName);
        if (m.Success)
            return new WipMeta(m.Groups[1].Value, m.Groups[2].Value);

        return null;
    }
}
