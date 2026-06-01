using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Resolves slit width from slitting accepted CSVs (<c>Z:\To SAP\TM\Slitting\Slit Accepted</c>).
/// </summary>
public static class SlitAcceptedCsvLookup
{
    public static async Task<string> ResolveSlitWidthAsync(
        NdtBundleOptions options,
        string slitNo,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(slitNo))
            return string.Empty;

        var folder = (options.SlitAcceptedFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return string.Empty;

        var target = slitNo.Trim();
        foreach (var path in Directory.EnumerateFiles(folder, "*.csv")
                     .OrderByDescending(File.GetLastWriteTimeUtc)
                     .ThenBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var width = await TryReadSlitWidthFromFileAsync(path, target, cancellationToken).ConfigureAwait(false);
            if (!string.IsNullOrWhiteSpace(width))
                return width.Trim();
        }

        return string.Empty;
    }

    private static async Task<string> TryReadSlitWidthFromFileAsync(
        string path,
        string slitNo,
        CancellationToken cancellationToken)
    {
        try
        {
            await using var stream = new FileStream(
                path,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete);
            using var reader = new StreamReader(stream);
            var headerRaw = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            if (headerRaw is null)
                return string.Empty;

            var headers = InputSlitCsvParsing.SplitCsvFields(InputSlitCsvParsing.StripBom(headerRaw));
            var widthIdx = InputSlitCsvParsing.HeaderIndex(headers, "Slit Width", "Width");
            if (widthIdx < 0)
                widthIdx = FindPairedWidthColumnIndex(headers);

            if (widthIdx < 0)
                return string.Empty;

            var slitIdx = FindSlitMatchColumnIndex(headers);
            if (slitIdx < 0)
                return string.Empty;

            while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                var cols = InputSlitCsvParsing.SplitCsvFields(line);
                if (slitIdx >= cols.Length)
                    continue;
                if (!SlitNoMatches(cols[slitIdx].Trim(), slitNo))
                    continue;
                if (widthIdx < cols.Length && !string.IsNullOrWhiteSpace(cols[widthIdx]))
                    return cols[widthIdx].Trim();
            }
        }
        catch
        {
            // Ignore unreadable slit accepted files.
        }

        return string.Empty;
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

    private static bool SlitNoMatches(string cell, string target) =>
        string.Equals(cell, target, StringComparison.OrdinalIgnoreCase);
}
