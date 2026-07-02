using System.Globalization;
using Microsoft.Extensions.Logging;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>Updates consolidated NDT process CSV files when bundle totals or manual stations are reconciled.</summary>
internal static class NdtProcessCsvReconcileHelper
{
    private const int ColPo = 0;
    private const int ColBatch = 1;
    private const int ColNdtPcs = 2;
    private const int ColOk = 3;
    private const int ColVisualReject = 4;
    private const int ColHydroReject = 5;
    private const int ColRevisualReject = 6;
    private const int MinColumns = 4;

    /// <summary>Updates OK (and optional reject columns) on the newest NDT process CSV row for the batch. Returns path when updated.</summary>
    public static async Task<string?> TryUpdateOkForBatchAsync(
        NdtBundleOptions options,
        string ndtBatchNo,
        int okPcs,
        int? visualReject,
        int? hydroReject,
        int? revisualReject,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        var folder = (options.NdtProcessOutputFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder) || string.IsNullOrWhiteSpace(ndtBatchNo))
            return null;

        var batch = ndtBatchNo.Trim();
        var targetPath = FindLatestNdtProcessFileForBatch(folder, batch);
        if (targetPath is null)
            return null;

        try
        {
            var lines = await File.ReadAllLinesAsync(targetPath, cancellationToken).ConfigureAwait(false);
            if (lines.Length < 2)
                return null;

            var dataIndex = -1;
            for (var i = lines.Length - 1; i >= 1; i--)
            {
                if (string.IsNullOrWhiteSpace(lines[i]))
                    continue;
                var cols = ReconcileCsvParsing.SplitCsvLine(lines[i]);
                if (cols.Count >= MinColumns && cols[ColBatch].Trim().Equals(batch, StringComparison.OrdinalIgnoreCase))
                {
                    dataIndex = i;
                    break;
                }
            }

            if (dataIndex < 0)
                return null;

            var rowCols = ReconcileCsvParsing.SplitCsvLine(lines[dataIndex]);
            while (rowCols.Count < 7)
                rowCols.Add("0");

            rowCols[ColOk] = okPcs.ToString(CultureInfo.InvariantCulture);
            if (visualReject.HasValue)
                rowCols[ColVisualReject] = visualReject.Value.ToString(CultureInfo.InvariantCulture);
            if (hydroReject.HasValue)
                rowCols[ColHydroReject] = hydroReject.Value.ToString(CultureInfo.InvariantCulture);
            if (revisualReject.HasValue)
                rowCols[ColRevisualReject] = revisualReject.Value.ToString(CultureInfo.InvariantCulture);

            lines[dataIndex] = string.Join(", ", rowCols);
            await File.WriteAllLinesAsync(targetPath, lines, cancellationToken).ConfigureAwait(false);
            logger.LogInformation(
                "Updated NDT process CSV {Path} for batch {BatchNo}: OK={OkPcs}.",
                targetPath,
                batch,
                okPcs);
            return targetPath;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to update NDT process CSV {Path} for batch {BatchNo}.", targetPath, batch);
            return null;
        }
    }

    public static string? FindLatestNdtProcessFileForBatch(string folder, string ndtBatchNo)
    {
        var batch = ndtBatchNo.Trim();
        string? latestPath = null;
        var latestTime = DateTime.MinValue;

        foreach (var path in Directory.EnumerateFiles(folder, "*.csv"))
        {
            var fileName = Path.GetFileName(path);
            if (!fileName.StartsWith("NDT_process_", StringComparison.OrdinalIgnoreCase) &&
                !fileName.StartsWith("NDT process_", StringComparison.OrdinalIgnoreCase))
                continue;

            try
            {
                var lines = File.ReadAllLines(path);
                var hasBatch = false;
                for (var i = 1; i < lines.Length; i++)
                {
                    if (string.IsNullOrWhiteSpace(lines[i]))
                        continue;
                    var cols = ReconcileCsvParsing.SplitCsvLine(lines[i]);
                    if (cols.Count >= MinColumns && cols[ColBatch].Trim().Equals(batch, StringComparison.OrdinalIgnoreCase))
                    {
                        hasBatch = true;
                        break;
                    }
                }

                if (!hasBatch)
                    continue;

                var writeTime = File.GetLastWriteTimeUtc(path);
                if (writeTime >= latestTime)
                {
                    latestTime = writeTime;
                    latestPath = path;
                }
            }
            catch
            {
                // skip unreadable files
            }
        }

        return latestPath;
    }

    /// <summary>Reads consolidated metrics from the newest NDT process CSV for the batch, if any.</summary>
    public static (int Ok, int VisualReject, int HydroReject, int RevisualReject, int NdtPcs, string? Po)? TryReadMetricsForBatch(
        NdtBundleOptions options,
        string ndtBatchNo)
    {
        var folder = (options.NdtProcessOutputFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return null;

        var path = FindLatestNdtProcessFileForBatch(folder, ndtBatchNo.Trim());
        if (path is null)
            return null;

        try
        {
            var lines = File.ReadAllLines(path);
            for (var i = lines.Length - 1; i >= 1; i--)
            {
                if (string.IsNullOrWhiteSpace(lines[i]))
                    continue;
                var cols = ReconcileCsvParsing.SplitCsvLine(lines[i]);
                if (cols.Count < MinColumns || !cols[ColBatch].Trim().Equals(ndtBatchNo.Trim(), StringComparison.OrdinalIgnoreCase))
                    continue;

                static int ParseAt(List<string> c, int index) =>
                    index < c.Count && int.TryParse(c[index].Trim(), out var v) ? v : 0;

                return (
                    ParseAt(cols, ColOk),
                    ParseAt(cols, ColVisualReject),
                    ParseAt(cols, ColHydroReject),
                    ParseAt(cols, ColRevisualReject),
                    ParseAt(cols, ColNdtPcs),
                    cols[ColPo].Trim());
            }
        }
        catch
        {
            // ignored
        }

        return null;
    }
}
