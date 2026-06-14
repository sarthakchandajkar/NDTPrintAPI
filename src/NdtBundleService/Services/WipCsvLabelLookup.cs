using Microsoft.Extensions.Logging;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Resolves pipe grade/size/length/weight/type for NDT tags from PO plan and TM WIP bundle CSVs.
/// </summary>
public static class WipCsvLabelLookup
{
    public static async Task<WipLabelInfo?> ResolveAsync(
        NdtBundleOptions options,
        string? currentPoPlanPath,
        string poNumber,
        int millNo,
        ILogger logger,
        CancellationToken cancellationToken,
        WipLabelInfo? seedFromSql = null,
        bool skipPoPlanFolderScan = false)
    {
        if (string.IsNullOrWhiteSpace(poNumber) || millNo is < 1 or > 4)
            return null;

        var merged = new MutableWipLabelInfo();
        if (seedFromSql is not null)
            merged.Merge(
                seedFromSql.PipeGrade,
                seedFromSql.PipeSize,
                seedFromSql.PipeThickness,
                seedFromSql.PipeLength,
                seedFromSql.PipeWeightPerMeter,
                seedFromSql.PipeType);

        var sawAny = seedFromSql is not null;
        if (merged.HasPrimaryFields())
            return merged.ToModel();

        if (!skipPoPlanFolderScan)
        {
            foreach (var path in EnumeratePoPlanPaths(options, currentPoPlanPath))
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (await TryMergeFromCsvFileAsync(path, poNumber, millNo, merged, cancellationToken).ConfigureAwait(false))
                {
                    sawAny = true;
                    if (merged.HasPrimaryFields())
                        return merged.ToModel();
                }
            }
        }

        foreach (var path in EnumerateWipBundleCsvPaths(options, poNumber, millNo))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (await TryMergeFromCsvFileAsync(path, poNumber, millNo, merged, cancellationToken).ConfigureAwait(false))
            {
                sawAny = true;
                if (merged.HasPrimaryFields())
                    return merged.ToModel();
            }
        }

        if (!sawAny)
        {
            logger.LogWarning(
                "No WIP label row found for PO {PoNumber} mill {MillNo} in PO plan or WIP bundle folders.",
                poNumber,
                millNo);
            return null;
        }

        if (!merged.HasPrimaryFields())
        {
            logger.LogWarning(
                "WIP label row for PO {PoNumber} mill {MillNo} is missing grade/size/length/weight (PO plan or WIP bundle CSV columns may be empty).",
                poNumber,
                millNo);
        }

        return merged.ToModel();
    }

    private static IEnumerable<string> EnumeratePoPlanPaths(NdtBundleOptions options, string? currentPoPlanPath)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (!string.IsNullOrWhiteSpace(options.PoPlanCsvPath))
        {
            var path = options.PoPlanCsvPath.Trim();
            if (File.Exists(path) && seen.Add(path))
                yield return path;
        }

        if (!string.IsNullOrWhiteSpace(currentPoPlanPath) && File.Exists(currentPoPlanPath) && seen.Add(currentPoPlanPath))
            yield return currentPoPlanPath;

        var folder = (options.PoPlanFolder ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(folder) && Directory.Exists(folder))
        {
            foreach (var path in Directory.EnumerateFiles(folder, "*.csv")
                         .Where(p => SourceFileEligibility.IncludePoPlanFolderFileUtc(File.GetLastWriteTimeUtc(p), options))
                         .OrderByDescending(File.GetLastWriteTimeUtc)
                         .ThenBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase))
            {
                if (seen.Add(path))
                    yield return path;
            }
        }
    }

    private static IEnumerable<string> EnumerateWipBundleCsvPaths(NdtBundleOptions options, string poNumber, int millNo)
    {
        var live = options.MillSlitLive ?? new MillSlitLiveOptions();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var folder in new[]
                 {
                     live.WipBundleFolder,
                     live.WipBundleAcceptedFolder,
                     options.FgBundleFolder,
                     options.FgBundleAcceptedFolder
                 })
        {
            var root = (folder ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(root) || !Directory.Exists(root))
                continue;

            foreach (var path in Directory.EnumerateFiles(root)
                         .OrderByDescending(File.GetLastWriteTimeUtc)
                         .ThenBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase))
            {
                if (!seen.Add(path))
                    continue;

                var name = Path.GetFileName(path);
                if (!name.StartsWith("WIP_", StringComparison.OrdinalIgnoreCase))
                    continue;

                var meta = WipBundleFileName.TryParse(name);
                if (meta is null || meta.MillNo != millNo)
                    continue;
                if (!InputSlitCsvParsing.PoEquals(meta.PoNumber, poNumber))
                    continue;

                yield return path;
            }
        }
    }

    private static async Task<bool> TryMergeFromCsvFileAsync(
        string filePath,
        string poNumber,
        int millNo,
        MutableWipLabelInfo merged,
        CancellationToken cancellationToken)
    {
        if (!File.Exists(filePath))
            return false;

        await using var stream = File.OpenRead(filePath);
        using var reader = new StreamReader(stream);
        var headerRaw = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
        if (headerRaw is null)
            return false;

        var headers = InputSlitCsvParsing.SplitDataFields(InputSlitCsvParsing.StripBom(headerRaw));
        var poIdx = InputSlitCsvParsing.HeaderIndex(headers, "PO_No", "PO Number", "PO No");
        var millIdx = InputSlitCsvParsing.HeaderIndex(headers, "Mill Number", "Mill No", "Mill Line No");
        var gradeIdx = InputSlitCsvParsing.HeaderIndex(headers, "Pipe Grade", "Grade");
        var sizeIdx = InputSlitCsvParsing.HeaderIndex(headers, "Pipe Size", "Size");
        var thicknessIdx = InputSlitCsvParsing.HeaderIndex(headers, "Pipe Thickness", "Thickness");
        var lengthIdx = InputSlitCsvParsing.HeaderIndex(headers, "Pipe Length", "Length");
        var weightIdx = InputSlitCsvParsing.HeaderIndex(
            headers,
            "Pipe Weight Per Meter",
            "Weight Per Meter",
            "Pipe Weight",
            "Pipe Wt/mtr",
            "Pipe Wt/m");
        var typeIdx = InputSlitCsvParsing.HeaderIndex(headers, "Pipe Type", "Type");

        if (poIdx < 0)
            return false;

        var matched = false;
        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;

            var cols = InputSlitCsvParsing.SplitDataFields(line);
            if (poIdx >= cols.Length)
                continue;

            var rowPo = cols[poIdx].Trim();
            if (!InputSlitCsvParsing.PoEquals(rowPo, poNumber))
                continue;

            if (millIdx >= 0)
            {
                if (millIdx >= cols.Length)
                    continue;
                if (!InputSlitCsvParsing.TryParseMillNo(cols[millIdx], out var rowMill) || rowMill != millNo)
                    continue;
            }

            string Cell(int idx) => idx >= 0 && idx < cols.Length ? cols[idx].Trim() : string.Empty;
            merged.Merge(Cell(gradeIdx), Cell(sizeIdx), Cell(thicknessIdx), Cell(lengthIdx), Cell(weightIdx), Cell(typeIdx));
            matched = true;
        }

        return matched;
    }

    private sealed class MutableWipLabelInfo
    {
        public string PipeGrade { get; private set; } = string.Empty;
        public string PipeSize { get; private set; } = string.Empty;
        public string PipeThickness { get; private set; } = string.Empty;
        public string PipeLength { get; private set; } = string.Empty;
        public string PipeWeightPerMeter { get; private set; } = string.Empty;
        public string PipeType { get; private set; } = string.Empty;

        public void Merge(string grade, string size, string thickness, string length, string weight, string type)
        {
            if (string.IsNullOrWhiteSpace(PipeGrade) && !string.IsNullOrWhiteSpace(grade))
                PipeGrade = grade.Trim();
            if (string.IsNullOrWhiteSpace(PipeSize) && !string.IsNullOrWhiteSpace(size))
                PipeSize = size.Trim();
            if (string.IsNullOrWhiteSpace(PipeThickness) && !string.IsNullOrWhiteSpace(thickness))
                PipeThickness = thickness.Trim();
            if (string.IsNullOrWhiteSpace(PipeLength) && !string.IsNullOrWhiteSpace(length))
                PipeLength = length.Trim();
            if (string.IsNullOrWhiteSpace(PipeWeightPerMeter) && !string.IsNullOrWhiteSpace(weight))
                PipeWeightPerMeter = weight.Trim();
            if (string.IsNullOrWhiteSpace(PipeType) && !string.IsNullOrWhiteSpace(type))
                PipeType = type.Trim();
        }

        public bool HasPrimaryFields() =>
            !string.IsNullOrWhiteSpace(PipeSize)
            || !string.IsNullOrWhiteSpace(PipeThickness)
            || !string.IsNullOrWhiteSpace(PipeLength)
            || !string.IsNullOrWhiteSpace(PipeWeightPerMeter)
            || !string.IsNullOrWhiteSpace(PipeGrade);

        public WipLabelInfo ToModel() => new()
        {
            PipeGrade = PipeGrade,
            PipeSize = PipeSize,
            PipeThickness = PipeThickness,
            PipeLength = PipeLength,
            PipeWeightPerMeter = PipeWeightPerMeter,
            PipeType = PipeType
        };
    }
}
