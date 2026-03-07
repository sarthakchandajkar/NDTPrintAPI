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
        var filesUpdated = 0;

        foreach (var path in Directory.EnumerateFiles(folder, "*.csv"))
        {
            try
            {
                var lines = await File.ReadAllLinesAsync(path, cancellationToken).ConfigureAwait(false);
                if (lines.Length < 2)
                    continue;

                var modified = false;
                for (var i = 1; i < lines.Length; i++)
                {
                    var cols = SplitCsvLine(lines[i]);
                    if (cols.Count >= MinColumns && cols[ColNdtBatchNo].Trim().Equals(batchNoTrimmed, StringComparison.OrdinalIgnoreCase))
                    {
                        cols[ColNdtPipes] = newPipes.ToString();
                        lines[i] = string.Join(",", cols);
                        modified = true;
                    }
                }

                if (modified)
                {
                    await File.WriteAllLinesAsync(path, lines, cancellationToken).ConfigureAwait(false);
                    filesUpdated++;
                    _logger.LogInformation("Updated NDT Pipes to {NewPipes} for bundle {BatchNo} in {Path}.", newPipes, batchNo, path);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to update file {Path} for bundle {BatchNo}.", path, batchNo);
            }
        }

        return filesUpdated;
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
    /// TotalNdtPcs is the sum of NDT Pipes across all rows in that bundle (accurate bundle total).
    /// SlitNo is taken from the last row for that bundle (closing slit).
    /// </summary>
    private async Task<List<NdtBundleRecord>> GetBundlesFromCsvAsync(CancellationToken cancellationToken)
    {
        var folder = _options.OutputBundleFolder;
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return new List<NdtBundleRecord>();

        // Aggregate by bundle: sum NDT Pipes, keep last row's Slit No and PO/Mill for display
        var byBundle = new Dictionary<string, (int TotalNdtPcs, string SlitNo, string PoNumber, int MillNo, string NdtShortLengthPipe, string RejectedShortLengthPipe)>(StringComparer.OrdinalIgnoreCase);

        var files = Directory.EnumerateFiles(folder, "*.csv").OrderBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase).ToList();
        foreach (var path in files)
        {
            try
            {
                await using var stream = File.OpenRead(path);
                using var reader = new StreamReader(stream);
                var headerLine = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
                if (headerLine is null)
                    continue;

                while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
                {
                    if (string.IsNullOrWhiteSpace(line))
                        continue;
                    var cols = SplitCsvLine(line);
                    if (cols.Count < MinColumns)
                        continue;
                    var bundleNo = cols[ColNdtBatchNo].Trim();
                    if (string.IsNullOrEmpty(bundleNo))
                        continue;
                    if (!int.TryParse(cols[ColNdtPipes].Trim(), out var ndtPipes))
                        ndtPipes = 0;
                    var po = cols[0].Trim();
                    var slitNo = cols[1].Trim();
                    if (!int.TryParse(cols[6].Trim(), out var millNo))
                        millNo = 0;
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
}
