using System.Globalization;
using System.Text.RegularExpressions;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Parses TM <c>FG_*.csv</c> into full audit replicas (every Excel column) plus normalized lookup rows.
/// Source files are opened read-only and are never modified, moved, or deleted.
/// </summary>
internal static class FgBundleCsvParser
{
    private static readonly Regex FgFileName = new(
        @"^FG_(\d{1,2})_(\d+)_",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    public static async Task<CsvFolderFileParseResult<FgBundleRow>> ParseFileAsync(
        string filePath,
        CancellationToken cancellationToken)
    {
        var lookup = new List<FgBundleRow>();
        var audit = new List<CsvFolderAuditRow>();

        await using var stream = CsvFolderReadOnlyIO.OpenRead(filePath);
        using var reader = new StreamReader(stream);
        var headerRaw = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
        if (headerRaw is null)
            return new CsvFolderFileParseResult<FgBundleRow>();

        var headerLine = InputSlitCsvParsing.StripBom(headerRaw);
        var headers = InputSlitCsvParsing.SplitCsvFields(headerLine);
        int Idx(params string[] names) => InputSlitCsvParsing.HeaderIndex(headers, names);

        var poIdx = Idx("PO Number", "PO_No", "PO No", "PO_NO");
        var millIdx = Idx("Mill Line No", "Mill Number", "Mill No");
        var gradeIdx = Idx("Pipe Grade", "Slit Grade", "Grade");
        var sizeIdx = Idx("Pipe Size", "Size");
        var thickIdx = Idx("Pipe Thickness", "Thickness");
        var lengthIdx = Idx("Pipe Length", "Length");
        var weightIdx = Idx("Pipe Wt/mtr", "Pipe Weight Per Meter", "Pipe Weight", "Weight Per Meter");
        var typeIdx = Idx("Pipe Type", "Type");
        var actPcsIdx = Idx("Act Pcs. in Bundle", "Act Pcs in Bundle", "NumOfPipes");
        var bundleIdx = Idx("Bundle No", "Bundle Number", "Bundle_No");
        var statusIdx = Idx("Bundle Status");
        var s1 = Idx("Slit 1 Num", "Slit1 Num");
        var s1Ok = Idx("Slit 1 OK Pcs", "Slit 1 OK");
        var s2 = Idx("Slit 2 Num", "Slit2 Num");
        var s2Ok = Idx("Slit 2 OK Pcs", "Slit 2 OK");
        var s3 = Idx("Slit 3 Num", "Slit3 Num");
        var s3Ok = Idx("Slit 3 OK Pcs", "Slit 3 OK");
        var s4 = Idx("Slit 4 Num", "Slit4 Num");
        var s4Ok = Idx("Slit 4 OK Pcs", "Slit 4 OK");
        var wtIdx = Idx("Bundle Wt.", "Bundle Wt", "Bundle Weight");
        var startIdx = Idx("Bundle Start");
        var endIdx = Idx("Bundle End");
        var opIdx = Idx("Operator Done");
        var nssIdx = Idx("Non Standard Slit", "Non-Standard Slit", "NSS");
        var specIdx = Idx("PO_Specification", "PO Specification");
        var fileMeta = ParseFgFileName(Path.GetFileName(filePath));

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
            if (string.IsNullOrWhiteSpace(po) && fileMeta is not null)
                po = fileMeta.PoNumber;
            if (string.IsNullOrWhiteSpace(po))
                continue;

            int? mill = null;
            if (millIdx >= 0 && InputSlitCsvParsing.TryParseMillNo(Cell(millIdx), out var millNo))
                mill = millNo;
            else if (fileMeta is not null)
                mill = fileMeta.MillNo;

            lookup.Add(new FgBundleRow
            {
                PoNumber = po,
                MillNo = mill,
                PipeGrade = Cell(gradeIdx),
                PipeSize = Cell(sizeIdx),
                PipeThickness = Cell(thickIdx),
                PipeLength = Cell(lengthIdx),
                PipeWeightPerMeter = Cell(weightIdx),
                PipeType = Cell(typeIdx),
                ActPcsInBundle = Cell(actPcsIdx),
                BundleNo = Cell(bundleIdx),
                BundleStatus = Cell(statusIdx),
                Slit1Num = Cell(s1),
                Slit1OkPcs = Cell(s1Ok),
                Slit2Num = Cell(s2),
                Slit2OkPcs = Cell(s2Ok),
                Slit3Num = Cell(s3),
                Slit3OkPcs = Cell(s3Ok),
                Slit4Num = Cell(s4),
                Slit4OkPcs = Cell(s4Ok),
                BundleWt = Cell(wtIdx),
                BundleStart = Cell(startIdx),
                BundleEnd = Cell(endIdx),
                OperatorDone = Cell(opIdx),
                NonStandardSlit = Cell(nssIdx),
                PoSpecification = Cell(specIdx)
            });
        }

        return new CsvFolderFileParseResult<FgBundleRow>
        {
            HeaderLine = headerLine,
            AuditRows = audit,
            LookupRows = lookup
        };
    }

    public static List<string> ResolveEligibleFiles(NdtBundleOptions options)
    {
        var minUtc = SourceFileEligibility.ParseMinUtcFromRaw(options.FgBundleImportMinLastWriteUtc)
                     ?? SourceFileEligibility.ParseMinUtc(options);
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var files = new List<FileInfo>();

        foreach (var folder in EnumerateFolders(options))
        {
            if (!Directory.Exists(folder))
                continue;
            foreach (var path in Directory.EnumerateFiles(folder, "FG_*.csv"))
            {
                if (!seen.Add(path))
                    continue;
                var info = new FileInfo(path);
                if (!SourceFileEligibility.IncludeFileUtc(info.LastWriteTimeUtc, minUtc))
                    continue;
                files.Add(info);
            }
        }

        return files
            .OrderBy(f => f.LastWriteTimeUtc)
            .ThenBy(f => f.FullName, StringComparer.OrdinalIgnoreCase)
            .Select(f => f.FullName)
            .ToList();
    }

    private static IEnumerable<string> EnumerateFolders(NdtBundleOptions options)
    {
        var live = options.MillSlitLive ?? new MillSlitLiveOptions();
        foreach (var path in new[]
                 {
                     options.FgBundleFolder,
                     options.FgBundleAcceptedFolder,
                     live.WipBundleFolder,
                     live.WipBundleAcceptedFolder
                 })
        {
            var p = (path ?? string.Empty).Trim();
            if (p.Length > 0)
                yield return p;
        }
    }

    private static FgMeta? ParseFgFileName(string fileName)
    {
        var m = FgFileName.Match(fileName);
        if (!m.Success)
            return null;
        if (!int.TryParse(m.Groups[1].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var mill))
            return null;
        if (mill is < 1 or > 4)
            return null;
        return new FgMeta(mill, m.Groups[2].Value);
    }

    private sealed record FgMeta(int MillNo, string PoNumber);
}
