using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Resolves <c>Pipe Grade</c> from TM FG bundle CSVs (<c>FG_{mill}_{po}_…</c>) for upload bundle Slit Grade.
/// </summary>
public static class FgBundleCsvLookup
{
    private static readonly Regex FgFileName = new(
        @"^FG_(\d{1,2})_(\d+)_",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    public static async Task<string> ResolvePipeGradeAsync(
        NdtBundleOptions options,
        string poNumber,
        int? millNo,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(poNumber))
            return string.Empty;

        foreach (var folder in EnumerateFgBundleFolders(options))
        {
            if (!Directory.Exists(folder))
                continue;

            foreach (var path in Directory.EnumerateFiles(folder, "FG_*.csv")
                         .OrderByDescending(File.GetLastWriteTimeUtc)
                         .ThenBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase))
            {
                cancellationToken.ThrowIfCancellationRequested();
                var name = Path.GetFileName(path);
                var meta = ParseFgFileName(name);
                if (meta is null)
                    continue;
                if (!InputSlitCsvParsing.PoEquals(meta.PoNumber, poNumber))
                    continue;
                if (millNo is >= 1 and <= 4 && meta.MillNo != millNo.Value)
                    continue;

                var grade = await ReadPipeGradeFromFileAsync(path, poNumber, millNo, cancellationToken)
                    .ConfigureAwait(false);
                if (!string.IsNullOrWhiteSpace(grade))
                    return grade.Trim();
            }
        }

        logger.LogDebug(
            "No FG bundle Pipe Grade found for PO {PoNumber} mill {MillNo} in TM Bundle folders.",
            poNumber,
            millNo);
        return string.Empty;
    }

    private static IEnumerable<string> EnumerateFgBundleFolders(NdtBundleOptions options)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var live = options.MillSlitLive ?? new MillSlitLiveOptions();
        foreach (var path in new[]
                 {
                     options.FgBundleFolder,
                     options.FgBundleAcceptedFolder,
                     live.WipBundleFolder,
                     live.WipBundleAcceptedFolder,
                 })
        {
            var p = (path ?? string.Empty).Trim();
            if (p.Length > 0 && seen.Add(p))
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
        var po = m.Groups[2].Value;
        return new FgMeta(mill, po);
    }

    private static async Task<string> ReadPipeGradeFromFileAsync(
        string filePath,
        string poNumber,
        int? millNo,
        CancellationToken cancellationToken)
    {
        await using var stream = new FileStream(
            filePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete);
        using var reader = new StreamReader(stream);
        var headerRaw = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
        if (headerRaw is null)
            return string.Empty;

        var headers = InputSlitCsvParsing.SplitCsvFields(InputSlitCsvParsing.StripBom(headerRaw));
        var gradeIdx = InputSlitCsvParsing.HeaderIndex(headers, "Pipe Grade", "Slit Grade", "Grade");
        if (gradeIdx < 0)
            return string.Empty;

        var poIdx = InputSlitCsvParsing.HeaderIndex(headers, "PO_No", "PO Number", "PO No", "PO_NO");
        var millIdx = InputSlitCsvParsing.HeaderIndex(headers, "Mill Number", "Mill No");

        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;

            var cols = InputSlitCsvParsing.SplitCsvFields(line);
            if (poIdx >= 0)
            {
                if (poIdx >= cols.Length)
                    continue;
                if (!InputSlitCsvParsing.PoEquals(cols[poIdx].Trim(), poNumber))
                    continue;
            }

            if (millNo is >= 1 and <= 4 && millIdx >= 0)
            {
                if (millIdx >= cols.Length)
                    continue;
                if (!InputSlitCsvParsing.TryParseMillNo(cols[millIdx], out var rowMill) || rowMill != millNo.Value)
                    continue;
            }

            if (gradeIdx < cols.Length && !string.IsNullOrWhiteSpace(cols[gradeIdx]))
                return cols[gradeIdx].Trim();
        }

        return string.Empty;
    }

    private sealed record FgMeta(int MillNo, string PoNumber);
}
