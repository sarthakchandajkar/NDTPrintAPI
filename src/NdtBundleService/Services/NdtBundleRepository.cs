using System.Globalization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Persists and queries NDT bundles. When ConnectionString is set uses SQL Server; otherwise reads from output CSVs.
/// Always updates output CSV files on reconciliation.
/// </summary>
public sealed class NdtBundleRepository : INdtBundleRepository
{
    private const int ColNdtPipes = 2;
    private const int ColNdtBatchNo = 9;
    private const int ColSlitNo = 1;
    private const int MinColumns = 10;

    private readonly NdtBundleOptions _options;
    private readonly ILogger<NdtBundleRepository> _logger;

    public NdtBundleRepository(IOptions<NdtBundleOptions> options, ILogger<NdtBundleRepository> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task RecordBundleAsync(NdtBundleRecord record, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
            return;

        try
        {
            await using var conn = new Microsoft.Data.SqlClient.SqlConnection(_options.ConnectionString);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            const string sql = @"
                INSERT INTO dbo.NDT_Bundle (PO_Number, Mill_No, Bundle_No, Total_NDT_Pcs, Context_Slit_No, Slit_Start_Time, Slit_Finish_Time, Rejected_P, NDT_Short_Length_Pipe, Rejected_Short_Length_Pipe, IsReprint)
                VALUES (@PoNumber, @MillNo, @BundleNo, @TotalNdtPcs, @SlitNo, @SlitStartTime, @SlitFinishTime, @RejectedPipes, @NdtShortLengthPipe, @RejectedShortLengthPipe, 0)";
            await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@PoNumber", record.PoNumber);
            cmd.Parameters.AddWithValue("@MillNo", record.MillNo);
            cmd.Parameters.AddWithValue("@BundleNo", record.BundleNo);
            cmd.Parameters.AddWithValue("@TotalNdtPcs", record.TotalNdtPcs);
            cmd.Parameters.AddWithValue("@SlitNo", (object?)record.SlitNo ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@SlitStartTime", (object?)record.SlitStartTime ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@SlitFinishTime", (object?)record.SlitFinishTime ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@RejectedPipes", record.RejectedPipes);
            cmd.Parameters.AddWithValue("@NdtShortLengthPipe", (object?)record.NdtShortLengthPipe ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@RejectedShortLengthPipe", (object?)record.RejectedShortLengthPipe ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            _logger.LogDebug("Recorded bundle {BundleNo} in database.", record.BundleNo);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to record bundle {BundleNo} in database.", record.BundleNo);
        }
    }

    public async Task<IReadOnlyList<NdtBundleRecord>> GetBundlesAsync(CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(_options.ConnectionString))
        {
            try
            {
                await using var conn = new Microsoft.Data.SqlClient.SqlConnection(_options.ConnectionString);
                await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
                const string sql = "SELECT Bundle_No AS BundleNo, PO_Number AS PoNumber, Mill_No AS MillNo, Total_NDT_Pcs AS TotalNdtPcs, Context_Slit_No AS SlitNo, Slit_Start_Time AS SlitStartTime, Slit_Finish_Time AS SlitFinishTime, Rejected_P AS RejectedPipes, NDT_Short_Length_Pipe AS NdtShortLengthPipe, Rejected_Short_Length_Pipe AS RejectedShortLengthPipe FROM dbo.NDT_Bundle ORDER BY PrintedAt DESC";
                await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
                var list = new List<NdtBundleRecord>();
                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    list.Add(ReadBundleFromReader(reader));
                }
                return list;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to get bundles from database; falling back to CSV scan.");
            }
        }

        return await GetBundlesFromCsvAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<NdtBundleRecord?> GetByBatchNoAsync(string batchNo, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(batchNo))
            return null;

        if (!string.IsNullOrWhiteSpace(_options.ConnectionString))
        {
            try
            {
                await using var conn = new Microsoft.Data.SqlClient.SqlConnection(_options.ConnectionString);
                await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
                const string sql = "SELECT TOP 1 Bundle_No AS BundleNo, PO_Number AS PoNumber, Mill_No AS MillNo, Total_NDT_Pcs AS TotalNdtPcs, Context_Slit_No AS SlitNo, Slit_Start_Time AS SlitStartTime, Slit_Finish_Time AS SlitFinishTime, Rejected_P AS RejectedPipes, NDT_Short_Length_Pipe AS NdtShortLengthPipe, Rejected_Short_Length_Pipe AS RejectedShortLengthPipe FROM dbo.NDT_Bundle WHERE Bundle_No = @BatchNo ORDER BY PrintedAt DESC";
                await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@BatchNo", batchNo.Trim());
                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    return ReadBundleFromReader(reader);
                return null;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to get bundle by BatchNo from database.");
            }
        }

        var bundles = await GetBundlesFromCsvAsync(cancellationToken).ConfigureAwait(false);
        return bundles.FirstOrDefault(b => b.BundleNo.Equals(batchNo.Trim(), StringComparison.OrdinalIgnoreCase));
    }

    public async Task UpdateBundlePipesAsync(string batchNo, int newPipes, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(batchNo))
            return;

        if (!string.IsNullOrWhiteSpace(_options.ConnectionString))
        {
            try
            {
                await using var conn = new Microsoft.Data.SqlClient.SqlConnection(_options.ConnectionString);
                await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
                const string sql = "UPDATE dbo.NDT_Bundle SET Total_NDT_Pcs = @NewPipes WHERE Bundle_No = @BatchNo";
                await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@NewPipes", newPipes);
                cmd.Parameters.AddWithValue("@BatchNo", batchNo.Trim());
                await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                _logger.LogInformation("Updated bundle {BatchNo} Total_NDT_Pcs to {NewPipes} in database.", batchNo, newPipes);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to update bundle {BatchNo} in database.", batchNo);
                throw;
            }
        }

        await UpdateOutputCsvFilesForBundleAsync(batchNo, newPipes, cancellationToken).ConfigureAwait(false);
    }

    public async Task<int> UpdateOutputCsvFilesForBundleAsync(string batchNo, int newPipes, CancellationToken cancellationToken)
    {
        var folder = _options.OutputBundleFolder;
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return 0;

        var batchNoTrimmed = batchNo.Trim();
        var filesOrdered = Directory.EnumerateFiles(folder, "*.csv").OrderBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase).ToList();

        // Find the last file (by name order) that contains this NDT Batch No
        string? lastFileWithBatch = null;
        foreach (var path in filesOrdered)
        {
            try
            {
                var lines = await File.ReadAllLinesAsync(path, cancellationToken).ConfigureAwait(false);
                for (var i = 1; i < lines.Length; i++)
                {
                    var cols = SplitCsvLine(lines[i]);
                    if (cols.Count >= MinColumns && cols[ColNdtBatchNo].Trim().Equals(batchNoTrimmed, StringComparison.OrdinalIgnoreCase))
                    {
                        lastFileWithBatch = path;
                        break;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Skip scan for {Path}.", path);
            }
        }

        if (string.IsNullOrEmpty(lastFileWithBatch))
        {
            _logger.LogWarning("No CSV file found containing bundle {BatchNo}.", batchNo);
            return 0;
        }

        // Sum NDT Pipes for this batch from all files except the last file (so we only change the last file)
        int sumFromOtherRows = 0;
        foreach (var path in filesOrdered)
        {
            if (path.Equals(lastFileWithBatch, StringComparison.OrdinalIgnoreCase))
                break;
            try
            {
                var lines = await File.ReadAllLinesAsync(path, cancellationToken).ConfigureAwait(false);
                for (var i = 1; i < lines.Length; i++)
                {
                    var cols = SplitCsvLine(lines[i]);
                    if (cols.Count >= MinColumns && cols[ColNdtBatchNo].Trim().Equals(batchNoTrimmed, StringComparison.OrdinalIgnoreCase))
                        sumFromOtherRows += int.TryParse(cols[ColNdtPipes].Trim(), out var p) ? p : 0;
                }
            }
            catch { /* ignore */ }
        }

        // In the last file, set the last row for this batch so that grand total = newPipes
        try
        {
            var lines = await File.ReadAllLinesAsync(lastFileWithBatch, cancellationToken).ConfigureAwait(false);
            var lastRowIndex = -1;
            var currentLastFileSum = 0;
            for (var i = 1; i < lines.Length; i++)
            {
                var cols = SplitCsvLine(lines[i]);
                if (cols.Count >= MinColumns && cols[ColNdtBatchNo].Trim().Equals(batchNoTrimmed, StringComparison.OrdinalIgnoreCase))
                {
                    lastRowIndex = i;
                    currentLastFileSum += int.TryParse(cols[ColNdtPipes].Trim(), out var p) ? p : 0;
                }
            }
            if (lastRowIndex < 0)
                return 0;

            var lastRowCols = SplitCsvLine(lines[lastRowIndex]);
            var lastRowCurrentValue = int.TryParse(lastRowCols[ColNdtPipes].Trim(), out var v) ? v : 0;
            var otherRowsInLastFileSum = currentLastFileSum - lastRowCurrentValue;
            var newLastRowValue = newPipes - sumFromOtherRows - otherRowsInLastFileSum;
            if (newLastRowValue < 0)
                newLastRowValue = 0;

            lastRowCols[ColNdtPipes] = newLastRowValue.ToString();
            lines[lastRowIndex] = string.Join(",", lastRowCols);
            await File.WriteAllLinesAsync(lastFileWithBatch, lines, cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("Updated NDT Pipes for bundle {BatchNo} in last file {Path}: last row set to {NewRowValue} so total = {NewPipes}.", batchNo, lastFileWithBatch, newLastRowValue, newPipes);
            return 1;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to update file {Path} for bundle {BatchNo}.", lastFileWithBatch, batchNo);
        }

        return 0;
    }

    public async Task<IReadOnlyList<(string SlitNo, int NdtPipes)>> GetSlitsForBatchAsync(string batchNo, CancellationToken cancellationToken)
    {
        var folder = _options.OutputBundleFolder;
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder) || string.IsNullOrWhiteSpace(batchNo))
            return Array.Empty<(string, int)>();

        var batchNoTrimmed = batchNo.Trim();
        var bySlit = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        // Use only per-slit output files (same name as input slit CSV). Exclude bundle summary files.
        var files = Directory.EnumerateFiles(folder, "*.csv")
            .Where(p => !Path.GetFileName(p).StartsWith("NDT_Bundle_", StringComparison.OrdinalIgnoreCase))
            .OrderBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        foreach (var path in files)
        {
            try
            {
                var lines = await File.ReadAllLinesAsync(path, cancellationToken).ConfigureAwait(false);
                for (var i = 1; i < lines.Length; i++)
                {
                    var cols = SplitCsvLine(lines[i]);
                    if (cols.Count < MinColumns)
                        continue;
                    if (!cols[ColNdtBatchNo].Trim().Equals(batchNoTrimmed, StringComparison.OrdinalIgnoreCase))
                        continue;

                    var slit = cols[ColSlitNo].Trim();
                    if (string.IsNullOrEmpty(slit))
                        slit = "—";
                    var pipes = int.TryParse(cols[ColNdtPipes].Trim(), out var p) ? p : 0;
                    bySlit[slit] = bySlit.TryGetValue(slit, out var existing) ? existing + pipes : pipes;
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Skip slit scan for {Path}.", path);
            }
        }

        return bySlit
            .OrderBy(kv => kv.Key, StringComparer.OrdinalIgnoreCase)
            .Select(kv => (kv.Key, kv.Value))
            .ToList();
    }

    public async Task<int> UpdateOutputCsvFilesForSlitAsync(string batchNo, string slitNo, int newPipes, CancellationToken cancellationToken)
    {
        var folder = _options.OutputBundleFolder;
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return 0;
        if (string.IsNullOrWhiteSpace(batchNo) || string.IsNullOrWhiteSpace(slitNo))
            return 0;

        var batchNoTrimmed = batchNo.Trim();
        var slitNoTrimmed = slitNo.Trim();

        // Update per-slit output files only; do not touch NDT_Bundle_*.csv here.
        var files = Directory.EnumerateFiles(folder, "*.csv")
            .Where(p => !Path.GetFileName(p).StartsWith("NDT_Bundle_", StringComparison.OrdinalIgnoreCase))
            .OrderBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var filesUpdated = 0;
        foreach (var path in files)
        {
            try
            {
                var lines = await File.ReadAllLinesAsync(path, cancellationToken).ConfigureAwait(false);
                var changed = false;
                for (var i = 1; i < lines.Length; i++)
                {
                    var cols = SplitCsvLine(lines[i]);
                    if (cols.Count < MinColumns)
                        continue;

                    if (!cols[ColNdtBatchNo].Trim().Equals(batchNoTrimmed, StringComparison.OrdinalIgnoreCase))
                        continue;
                    if (!cols[ColSlitNo].Trim().Equals(slitNoTrimmed, StringComparison.OrdinalIgnoreCase))
                        continue;

                    cols[ColNdtPipes] = newPipes.ToString(CultureInfo.InvariantCulture);
                    lines[i] = string.Join(",", cols);
                    changed = true;
                }

                if (changed)
                {
                    await File.WriteAllLinesAsync(path, lines, cancellationToken).ConfigureAwait(false);
                    filesUpdated++;
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Failed slit update for file {Path}.", path);
            }
        }

        return filesUpdated;
    }

    public async Task UpdateBundleTotalInDatabaseAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(batchNo) || string.IsNullOrWhiteSpace(_options.ConnectionString))
            return;

        try
        {
            await using var conn = new Microsoft.Data.SqlClient.SqlConnection(_options.ConnectionString);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            const string sql = "UPDATE dbo.NDT_Bundle SET Total_NDT_Pcs = @NewPipes WHERE Bundle_No = @BatchNo";
            await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@NewPipes", newTotalPipes);
            cmd.Parameters.AddWithValue("@BatchNo", batchNo.Trim());
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to update bundle total in DB for {BatchNo}.", batchNo);
            throw;
        }
    }

    public async Task<bool> UpdateBundleSummaryCsvAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken)
    {
        var folder = GetBundleSummaryFolder();
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder) || string.IsNullOrWhiteSpace(batchNo))
            return false;

        var fileName = $"NDT_Bundle_{batchNo.Trim()}.csv";
        var path = Path.Combine(folder, fileName);
        if (!File.Exists(path))
            return false;

        try
        {
            var lines = await File.ReadAllLinesAsync(path, cancellationToken).ConfigureAwait(false);
            if (lines.Length < 2)
                return false;

            var cols = SplitCsvLine(lines[1]);
            if (cols.Count < MinColumns)
                return false;

            cols[ColNdtPipes] = newTotalPipes.ToString(CultureInfo.InvariantCulture);
            lines[1] = string.Join(",", cols);
            await File.WriteAllLinesAsync(path, lines, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to update bundle summary CSV {Path}.", path);
            return false;
        }
    }

    private static NdtBundleRecord ReadBundleFromReader(Microsoft.Data.SqlClient.SqlDataReader reader)
    {
        return new NdtBundleRecord
        {
            BundleNo = reader.GetString(0),
            PoNumber = reader.GetString(1),
            MillNo = reader.GetInt32(2),
            TotalNdtPcs = reader.GetInt32(3),
            SlitNo = reader.IsDBNull(4) ? "" : reader.GetString(4),
            SlitStartTime = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
            SlitFinishTime = reader.IsDBNull(6) ? null : reader.GetDateTime(6),
            RejectedPipes = reader.GetInt32(7),
            NdtShortLengthPipe = reader.IsDBNull(8) ? "" : reader.GetString(8),
            RejectedShortLengthPipe = reader.IsDBNull(9) ? "" : reader.GetString(9)
        };
    }

    /// <summary>
    /// Scans output CSVs and builds one record per unique NDT Batch No.
    /// Prefers NDT_Bundle_*.csv files (one row per bundle with actual total); otherwise aggregates per-slit rows.
    /// </summary>
    private async Task<List<NdtBundleRecord>> GetBundlesFromCsvAsync(CancellationToken cancellationToken)
    {
        var perSlitFolder = _options.OutputBundleFolder;
        var summaryFolder = GetBundleSummaryFolder();

        var hasPerSlit = !string.IsNullOrWhiteSpace(perSlitFolder) && Directory.Exists(perSlitFolder);
        var hasSummary = !string.IsNullOrWhiteSpace(summaryFolder) && Directory.Exists(summaryFolder);
        if (!hasPerSlit && !hasSummary)
            return new List<NdtBundleRecord>();

        var byBundle = new Dictionary<string, (int TotalNdtPcs, string SlitNo, string PoNumber, int MillNo, string NdtShortLengthPipe, string RejectedShortLengthPipe)>(StringComparer.OrdinalIgnoreCase);
        var bundleNosFromSummary = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var bundleFiles = hasSummary
            ? Directory.EnumerateFiles(summaryFolder!, "NDT_Bundle_*.csv")
                .OrderBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase)
                .ToList()
            : new List<string>();

        var otherFiles = hasPerSlit
            ? Directory.EnumerateFiles(perSlitFolder!, "*.csv")
                .Where(p => !Path.GetFileName(p).StartsWith("NDT_Bundle_", StringComparison.OrdinalIgnoreCase))
                .OrderBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase)
                .ToList()
            : new List<string>();

        // First pass: NDT_Bundle_*.csv = one row per bundle with actual total (e.g. 11 pipes)
        foreach (var path in bundleFiles)
        {
            try
            {
                await using var stream = File.OpenRead(path);
                using var reader = new StreamReader(stream);
                var headerLine = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
                if (headerLine is null) continue;
                if (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is not { } line)
                    continue;
                var cols = SplitCsvLine(line);
                if (cols.Count < MinColumns) continue;
                var bundleNo = cols[ColNdtBatchNo].Trim();
                if (string.IsNullOrEmpty(bundleNo)) continue;
                if (!int.TryParse(cols[ColNdtPipes].Trim(), out var ndtPipes)) ndtPipes = 0;
                var po = cols[0].Trim();
                var slitNo = cols[1].Trim();
                if (!int.TryParse(cols[6].Trim(), out var millNo)) millNo = 0;
                var ndtShort = cols.Count > 7 ? cols[7].Trim() : "";
                var rejShort = cols.Count > 8 ? cols[8].Trim() : "";
                byBundle[bundleNo] = (ndtPipes, slitNo, po, millNo, ndtShort, rejShort);
                bundleNosFromSummary.Add(bundleNo);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Skip bundle file {Path}.", path);
            }
        }

        // Second pass: per-slit files; only add/sum if this bundle not already from NDT_Bundle_ (avoid double-count)
        foreach (var path in otherFiles)
        {
            try
            {
                await using var stream = File.OpenRead(path);
                using var reader = new StreamReader(stream);
                var headerLine = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
                if (headerLine is null) continue;

                while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    var cols = SplitCsvLine(line);
                    if (cols.Count < MinColumns) continue;
                    var bundleNo = cols[ColNdtBatchNo].Trim();
                    if (string.IsNullOrEmpty(bundleNo)) continue;
                    if (bundleNosFromSummary.Contains(bundleNo)) continue; // already have authoritative total from NDT_Bundle_*.csv
                    if (!int.TryParse(cols[ColNdtPipes].Trim(), out var ndtPipes)) ndtPipes = 0;
                    var po = cols[0].Trim();
                    var slitNo = cols[1].Trim();
                    if (!int.TryParse(cols[6].Trim(), out var millNo)) millNo = 0;
                    var ndtShort = cols.Count > 7 ? cols[7].Trim() : "";
                    var rejShort = cols.Count > 8 ? cols[8].Trim() : "";

                    if (byBundle.TryGetValue(bundleNo, out var existing))
                    {
                        byBundle[bundleNo] = (
                            existing.TotalNdtPcs + ndtPipes,
                            slitNo,
                            po,
                            millNo,
                            ndtShort,
                            rejShort
                        );
                    }
                    else
                    {
                        byBundle[bundleNo] = (ndtPipes, slitNo, po, millNo, ndtShort, rejShort);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Skip bundle scan for {Path}.", path);
            }
        }

        var list = byBundle.Select(kv => new NdtBundleRecord
        {
            BundleNo = kv.Key,
            PoNumber = kv.Value.PoNumber,
            MillNo = kv.Value.MillNo,
            TotalNdtPcs = kv.Value.TotalNdtPcs,
            SlitNo = kv.Value.SlitNo,
            SlitStartTime = null,
            SlitFinishTime = null,
            RejectedPipes = 0,
            NdtShortLengthPipe = kv.Value.NdtShortLengthPipe,
            RejectedShortLengthPipe = kv.Value.RejectedShortLengthPipe
        }).OrderByDescending(b => b.BundleNo).ToList();

        return list;
    }

    /// <summary>Simple CSV line split (no quoted comma handling). Sufficient for output files we write.</summary>
    private static List<string> SplitCsvLine(string line)
    {
        return line.Split(',').Select(s => s.Trim()).ToList();
    }

    private string GetBundleSummaryFolder()
    {
        var configured = (_options.BundleSummaryOutputFolder ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(configured))
            return configured;
        return (_options.OutputBundleFolder ?? string.Empty).Trim();
    }
}
