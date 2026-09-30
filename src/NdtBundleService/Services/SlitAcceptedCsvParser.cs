using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Parses Slitting Slit Accepted CSVs into full audit replicas (every Excel column)
/// plus normalized lookup rows for upload Slit Width.
/// Source files are opened read-only and are never modified, moved, or deleted.
/// </summary>
internal static class SlitAcceptedCsvParser
{
    public static async Task<CsvFolderFileParseResult<SlitAcceptedRow>> ParseFileAsync(
        string filePath,
        CancellationToken cancellationToken)
    {
        var lookup = new List<SlitAcceptedRow>();
        var audit = new List<CsvFolderAuditRow>();

        await using var stream = CsvFolderReadOnlyIO.OpenRead(filePath);
        using var reader = new StreamReader(stream);
        var headerRaw = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
        if (headerRaw is null)
            return new CsvFolderFileParseResult<SlitAcceptedRow>();

        var headerLine = InputSlitCsvParsing.StripBom(headerRaw);
        var headers = InputSlitCsvParsing.SplitCsvFields(headerLine);
        var widthIdx = InputSlitCsvParsing.HeaderIndex(headers, "Slit Width", "Width");
        if (widthIdx < 0)
            widthIdx = FindPairedWidthColumnIndex(headers);

        var slitIdx = FindSlitMatchColumnIndex(headers);
        var thickIdx = InputSlitCsvParsing.HeaderIndex(headers, "Slit Thick", "Thickness", "Slit Thickness");
        var poIdx = InputSlitCsvParsing.HeaderIndex(headers, "PO_No", "PO Number", "PO No", "PO_NO");
        var millIdx = InputSlitCsvParsing.HeaderIndex(headers, "Mill Number", "Mill No", "Mill Line No");
        var nssIdx = InputSlitCsvParsing.HeaderIndex(headers, "NSS", "Non Standard Slit", "Non-Standard Slit");
        var hrcIdx = InputSlitCsvParsing.HeaderIndex(headers, "HRC Number", "HRC", "HRC_Number");
        var paired = FindAllPairedSlitWidthColumns(headers);

        var sourceRow = 1;
        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            sourceRow++;
            if (string.IsNullOrWhiteSpace(line))
                continue;

            var cols = InputSlitCsvParsing.SplitCsvFields(line);
            audit.Add(CsvFolderAuditRowFactory.Create(headerLine, headers, line, cols, sourceRow));

            string Cell(int idx) => idx >= 0 && idx < cols.Length ? cols[idx].Trim() : string.Empty;

            var po = Cell(poIdx);
            int? mill = null;
            if (millIdx >= 0 && InputSlitCsvParsing.TryParseMillNo(Cell(millIdx), out var millNo))
                mill = millNo;
            var thick = Cell(thickIdx);
            var nss = Cell(nssIdx);

            if (slitIdx >= 0 && widthIdx >= 0)
            {
                var slit = Cell(slitIdx);
                var width = Cell(widthIdx);
                if (!string.IsNullOrWhiteSpace(slit) && !string.IsNullOrWhiteSpace(width))
                {
                    lookup.Add(new SlitAcceptedRow
                    {
                        SlitNo = slit,
                        SlitWidth = width,
                        HrcNumber = !string.IsNullOrWhiteSpace(Cell(hrcIdx)) ? Cell(hrcIdx) : ExtractHrc(slit),
                        SlitThick = thick,
                        PoNumber = po,
                        MillNo = mill,
                        Nss = nss,
                        SourceRowNumber = sourceRow
                    });
                }
            }

            foreach (var (pairSlitIdx, pairWidthIdx) in paired)
            {
                var slit = Cell(pairSlitIdx);
                var width = Cell(pairWidthIdx);
                if (string.IsNullOrWhiteSpace(slit) || string.IsNullOrWhiteSpace(width))
                    continue;
                if (lookup.Any(r =>
                        r.SourceRowNumber == sourceRow
                        && string.Equals(r.SlitNo, slit, StringComparison.OrdinalIgnoreCase)))
                    continue;

                lookup.Add(new SlitAcceptedRow
                {
                    SlitNo = slit,
                    SlitWidth = width,
                    HrcNumber = ExtractHrc(slit),
                    SlitThick = thick,
                    PoNumber = po,
                    MillNo = mill,
                    Nss = nss,
                    SourceRowNumber = sourceRow
                });
            }
        }

        return new CsvFolderFileParseResult<SlitAcceptedRow>
        {
            HeaderLine = headerLine,
            AuditRows = audit,
            LookupRows = lookup
        };
    }

    public static List<string> ResolveEligibleFiles(NdtBundleOptions options)
    {
        var folder = (options.SlitAcceptedFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return new List<string>();

        var minUtc = SourceFileEligibility.ParseMinUtcFromRaw(options.SlitAcceptedImportMinLastWriteUtc)
                     ?? SourceFileEligibility.ParseMinUtc(options);
        return Directory.EnumerateFiles(folder, "*.csv")
            .Select(f => new FileInfo(f))
            .Where(f => SourceFileEligibility.IncludeFileUtc(f.LastWriteTimeUtc, minUtc))
            .OrderBy(f => f.LastWriteTimeUtc)
            .ThenBy(f => f.FullName, StringComparer.OrdinalIgnoreCase)
            .Select(f => f.FullName)
            .ToList();
    }

    private static string ExtractHrc(string slitNo)
    {
        var idx = slitNo.IndexOf('_');
        return idx > 0 ? slitNo[..idx].Trim() : string.Empty;
    }

    private static int FindSlitMatchColumnIndex(string[] headers)
    {
        var direct = InputSlitCsvParsing.HeaderIndex(
            headers,
            "Slit_No",
            "Slit No",
            "Slit Number",
            "Batch_No",
            "Batch No",
            "Slit Batch No");
        if (direct >= 0)
            return direct;

        for (var i = 0; i < headers.Length; i++)
        {
            var h = headers[i].Trim();
            if (h.EndsWith("Batch_No", StringComparison.OrdinalIgnoreCase) ||
                h.EndsWith("Batch No", StringComparison.OrdinalIgnoreCase))
                return i;
        }

        return -1;
    }

    private static int FindPairedWidthColumnIndex(string[] headers)
    {
        for (var i = 0; i < headers.Length; i++)
        {
            var h = headers[i].Trim();
            if (!h.EndsWith("Batch_No", StringComparison.OrdinalIgnoreCase))
                continue;
            var widthHeader = h.Replace("Batch_No", "Width", StringComparison.OrdinalIgnoreCase);
            var widthIndex = Array.FindIndex(
                headers,
                x => string.Equals(x.Trim(), widthHeader, StringComparison.OrdinalIgnoreCase));
            if (widthIndex >= 0)
                return widthIndex;
        }

        return -1;
    }

    private static List<(int SlitIdx, int WidthIdx)> FindAllPairedSlitWidthColumns(string[] headers)
    {
        var pairs = new List<(int, int)>();
        for (var i = 0; i < headers.Length; i++)
        {
            var h = headers[i].Trim();
            if (!h.EndsWith("Batch_No", StringComparison.OrdinalIgnoreCase)
                && !h.EndsWith("Batch No", StringComparison.OrdinalIgnoreCase))
                continue;

            var widthHeader = h.Contains("Batch_No", StringComparison.OrdinalIgnoreCase)
                ? h.Replace("Batch_No", "Width", StringComparison.OrdinalIgnoreCase)
                : h.Replace("Batch No", "Width", StringComparison.OrdinalIgnoreCase);
            var widthIndex = Array.FindIndex(
                headers,
                x => string.Equals(x.Trim(), widthHeader, StringComparison.OrdinalIgnoreCase));
            if (widthIndex >= 0)
                pairs.Add((i, widthIndex));
        }

        return pairs;
    }
}
