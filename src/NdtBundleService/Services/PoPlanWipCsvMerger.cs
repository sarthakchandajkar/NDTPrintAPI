using Microsoft.Extensions.Logging;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>Merges WIP rows from PO plan CSV files into per-mill and per-PO lookups.</summary>
internal static class PoPlanWipCsvMerger
{
    internal sealed class MergeResult
    {
        public Dictionary<int, PoPlanWipRow> ByMill { get; } = new();
        public Dictionary<string, PoPlanWipRow> ByPo { get; } = new(StringComparer.OrdinalIgnoreCase);
    }

    internal static async Task<bool> MergeFileAsync(
        string filePath,
        MergeResult result,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        await using var stream = File.OpenRead(filePath);
        using var reader = new StreamReader(stream);

        var headerLine = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
        if (headerLine is null)
        {
            logger.LogWarning("WIP file has no header line: {File}", filePath);
            return false;
        }

        var headers = headerLine.Split(',');

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
        {
            logger.LogWarning(
                "Skipping WIP file (missing PO or mill column): {File}. Expected Mill Number/Mill No and PO_No/PO Number/PO No.",
                filePath);
            return false;
        }

        int Idx(string columnName) => HeaderIndex(headers, columnName);

        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;
            var cols = line.Split(',');
            if (cols.Length <= millIdx)
                continue;
            var millRaw = cols[millIdx].Trim();
            if (!InputSlitCsvParsing.TryParseMillNo(millRaw, out var millNo))
                continue;

            string Cell(int idx) => idx >= 0 && idx < cols.Length ? cols[idx].Trim() : string.Empty;

            var po = Cell(poIdx);
            if (string.IsNullOrWhiteSpace(po))
                continue;

            var row = new PoPlanWipRow
            {
                MillNo = millNo,
                PoNumber = po,
                PlannedMonth = Cell(Idx("Planned Month")),
                PipeGrade = Cell(Idx("Pipe Grade")),
                PipeSize = Cell(Idx("Pipe Size")),
                PipeType = Cell(Idx("Pipe Type")),
                PipeLength = Cell(Idx("Pipe Length")),
                PiecesPerBundle = Cell(Idx("Pieces Per Bundle")),
                TotalPieces = Cell(Idx("Total Pieces")),
            };
            result.ByMill[millNo] = row;
            result.ByPo[InputSlitCsvParsing.NormalizePo(po)] = row;
        }

        return true;
    }

    internal static List<string> ResolveEligiblePoPlanFiles(NdtBundleOptions options)
    {
        var planFolder = (options.PoPlanFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(planFolder) || !Directory.Exists(planFolder))
            return new List<string>();

        return Directory.EnumerateFiles(planFolder, "*.csv")
            .Select(f => new FileInfo(f))
            .Where(f => SourceFileEligibility.IncludePoPlanFolderFileUtc(f.LastWriteTimeUtc, options))
            .OrderBy(f => f.LastWriteTimeUtc)
            .ThenBy(f => f.FullName, StringComparer.OrdinalIgnoreCase)
            .Select(f => f.FullName)
            .ToList();
    }

    internal static List<string> ResolveEligiblePoPlanImportFiles(NdtBundleOptions options)
    {
        var planFolder = (options.PoPlanFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(planFolder) || !Directory.Exists(planFolder))
            return new List<string>();

        var minUtc = PoPlanWipImportSettings.GetImportMinUtc(options);
        return Directory.EnumerateFiles(planFolder, "*.csv")
            .Select(f => new FileInfo(f))
            .Where(f => SourceFileEligibility.IncludeFileUtc(f.LastWriteTimeUtc, minUtc))
            .OrderBy(f => f.LastWriteTimeUtc)
            .ThenBy(f => f.FullName, StringComparer.OrdinalIgnoreCase)
            .Select(f => f.FullName)
            .ToList();
    }

    internal static string BuildPoPlanFilesSignature(IReadOnlyList<string> paths)
    {
        if (paths.Count == 0)
            return string.Empty;

        return string.Join(';', paths.Select(path => $"{path}|{File.GetLastWriteTimeUtc(path).Ticks}"));
    }
}
