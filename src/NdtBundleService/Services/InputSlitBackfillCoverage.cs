using Microsoft.Extensions.Logging;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Scans NDT Bundles / NDT Input Slit output folders for coverage of a backfilled Input Slit file (F-5.2).
/// </summary>
public static class InputSlitBackfillCoverage
{
    /// <summary>
    /// Evaluates whether existing on-disk NDT outputs already cover <paramref name="sourceFileFullPath"/> rows.
    /// </summary>
    public static BackfillCoverageKind Evaluate(
        string sourceFileFullPath,
        IReadOnlyList<InputSlitRecord> eligibleRows,
        NdtBundleOptions options,
        ILogger? logger = null)
    {
        if (eligibleRows.Count == 0)
            return BackfillCoverageKind.None;

        var outputFolder = (options.OutputBundleFolder ?? string.Empty).Trim();
        var summaryFolder = NdtBundleOutputPaths.ResolveBundleArtifactsFolder(options);

        // Exact: same basename already written under NDT Input Slit with batch numbers.
        if (!string.IsNullOrWhiteSpace(outputFolder))
        {
            var existingOut = Path.Combine(outputFolder, Path.GetFileName(sourceFileFullPath));
            if (File.Exists(existingOut) && PerSlitOutputHasBatchNumbers(existingOut, logger))
                return BackfillCoverageKind.ExactMatch;
        }

        var poMills = eligibleRows
            .Where(r => !string.IsNullOrWhiteSpace(r.PoNumber) && r.MillNo is >= 1 and <= 4)
            .Select(r => (Po: InputSlitCsvParsing.NormalizePo(r.PoNumber), r.MillNo))
            .Distinct()
            .ToList();

        if (poMills.Count == 0)
            return BackfillCoverageKind.None;

        if (string.IsNullOrWhiteSpace(summaryFolder) || !Directory.Exists(summaryFolder))
            return BackfillCoverageKind.None;

        var anyBundleForPoMill = false;
        try
        {
            foreach (var path in Directory.EnumerateFiles(summaryFolder, "NDT_Bundle_*.csv"))
            {
                if (TryReadSummaryPoMill(path, out var po, out var mill)
                    && poMills.Any(pm => pm.MillNo == mill && InputSlitCsvParsing.PoEquals(pm.Po, po)))
                {
                    anyBundleForPoMill = true;
                    break;
                }
            }
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "Backfill coverage: failed scanning NDT Bundles folder {Folder}.", summaryFolder);
        }

        return anyBundleForPoMill ? BackfillCoverageKind.Ambiguous : BackfillCoverageKind.None;
    }

    private static bool PerSlitOutputHasBatchNumbers(string path, ILogger? logger)
    {
        try
        {
            using var reader = new StreamReader(File.OpenRead(path));
            var headerRaw = reader.ReadLine();
            if (headerRaw is null)
                return false;

            var headers = InputSlitCsvParsing.SplitCsvFields(InputSlitCsvParsing.StripBom(headerRaw));
            var batchIdx = InputSlitCsvParsing.HeaderIndex(headers, "NDT Batch No");
            if (batchIdx < 0)
                return false;

            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                var cols = InputSlitCsvParsing.SplitCsvFields(line);
                if (batchIdx < cols.Length && !string.IsNullOrWhiteSpace(cols[batchIdx]))
                    return true;
            }
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "Backfill coverage: could not read per-slit output {Path}.", path);
        }

        return false;
    }

    private static bool TryReadSummaryPoMill(string path, out string po, out int mill)
    {
        po = string.Empty;
        mill = 0;
        try
        {
            using var reader = new StreamReader(File.OpenRead(path));
            var headerRaw = reader.ReadLine();
            var data = reader.ReadLine();
            if (headerRaw is null || data is null)
                return false;

            var headers = InputSlitCsvParsing.SplitCsvFields(InputSlitCsvParsing.StripBom(headerRaw));
            var cols = InputSlitCsvParsing.SplitCsvFields(data);
            var idxPo = InputSlitCsvParsing.HeaderIndex(headers, "PO Number", "PO_No", "PO No");
            var idxMill = InputSlitCsvParsing.HeaderIndex(headers, "Mill No", "Mill Number");
            if (idxPo < 0 || idxMill < 0 || idxPo >= cols.Length || idxMill >= cols.Length)
                return false;

            po = InputSlitCsvParsing.NormalizePo(cols[idxPo].Trim());
            return InputSlitCsvParsing.TryParseMillNo(cols[idxMill].Trim(), out mill) && mill is >= 1 and <= 4;
        }
        catch
        {
            return false;
        }
    }
}
