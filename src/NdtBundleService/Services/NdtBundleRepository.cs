using System.Globalization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Persists and queries NDT bundles. When <see cref="NdtBundleOptions.UseSqlServerForBundles"/> is true and ConnectionString is set, uses SQL Server; otherwise reads from output CSVs.
/// Always updates output CSV files on reconciliation.
/// </summary>
public sealed class NdtBundleRepository : INdtBundleRepository
{
    private const int ColNdtPipes = 2;
    private const int ColNdtBatchNo = 9;
    private const int ColSlitNo = 1;
    private const int MinColumns = 10;

    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly ISqlTraceabilityWriteTracker _writeTracker;
    private readonly ILogger<NdtBundleRepository> _logger;

    public NdtBundleRepository(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        ISqlTraceabilityWriteTracker writeTracker,
        ILogger<NdtBundleRepository> logger)
    {
        _optionsMonitor = optionsMonitor;
        _writeTracker = writeTracker;
        _logger = logger;
    }

    private NdtBundleOptions Opt => _optionsMonitor.CurrentValue;

    private bool UseDatabase => SqlTraceabilityConnection.IsSqlEnabled(Opt);

    public async Task RecordBundleAsync(NdtBundleRecord record, CancellationToken cancellationToken)
    {
        if (!UseDatabase)
        {
            _logger.LogWarning(
                "NDT_Bundle SQL write skipped for {BundleNo}: set NdtBundle:UseSqlServerForBundles=true and ConnectionString (or SqlServer+SqlDatabase) to JazeeraMES_Prod.",
                record.BundleNo);
            return;
        }

        const int maxAttempts = 3;
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await UpsertBundleAsync(record, cancellationToken).ConfigureAwait(false);
                _writeTracker.RecordSuccess("NDT_Bundle", $"{record.BundleNo} ({record.TotalNdtPcs} pcs)");
                _logger.LogInformation(
                    "Recorded bundle {BundleNo} ({Total} NDT pcs) in JazeeraMES_Prod.dbo.NDT_Bundle.",
                    record.BundleNo,
                    record.TotalNdtPcs);
                return;
            }
            catch (Exception ex) when (attempt < maxAttempts)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to record bundle {BundleNo} in database (attempt {Attempt}/{Max}); retrying.",
                    record.BundleNo,
                    attempt,
                    maxAttempts);
                await Task.Delay(TimeSpan.FromMilliseconds(400 * attempt), cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _writeTracker.RecordFailure("NDT_Bundle", ex.Message, record.BundleNo);
                _logger.LogError(
                    ex,
                    "Failed to record bundle {BundleNo} in JazeeraMES_Prod after {Max} attempts; Printed Tags and Visual incoming count may not match the label.",
                    record.BundleNo,
                    maxAttempts);
            }
        }
    }

    private async Task UpsertBundleAsync(NdtBundleRecord record, CancellationToken cancellationToken)
    {
        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "NDT_Bundle upsert", cancellationToken).ConfigureAwait(false);

        const string checkSql = "SELECT PO_Number FROM dbo.NDT_Bundle WHERE Bundle_No = @BundleNo";
        await using (var checkCmd = new Microsoft.Data.SqlClient.SqlCommand(checkSql, conn))
        {
            checkCmd.Parameters.AddWithValue("@BundleNo", record.BundleNo);
            var existingPo = await checkCmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) as string;
            if (!string.IsNullOrEmpty(existingPo)
                && !InputSlitCsvParsing.PoEquals(existingPo, record.PoNumber))
            {
                throw new InvalidOperationException(
                    $"Bundle_No {record.BundleNo} already exists for PO {InputSlitCsvParsing.NormalizePo(existingPo)}; " +
                    $"refusing to overwrite with PO {InputSlitCsvParsing.NormalizePo(record.PoNumber)}.");
            }
        }

        const string sql = @"
IF EXISTS (SELECT 1 FROM dbo.NDT_Bundle WHERE Bundle_No = @BundleNo)
BEGIN
    UPDATE dbo.NDT_Bundle
    SET PO_Number = @PoNumber,
        Mill_No = @MillNo,
        Total_NDT_Pcs = @TotalNdtPcs,
        Context_Slit_No = @SlitNo,
        Slit_Start_Time = @SlitStartTime,
        Slit_Finish_Time = @SlitFinishTime,
        Rejected_P = @RejectedPipes,
        NDT_Short_Length_Pipe = @NdtShortLengthPipe,
        Rejected_Short_Length_Pipe = @RejectedShortLengthPipe,
        PrintedAt = SYSDATETIME(),
        IsReprint = 0
    WHERE Bundle_No = @BundleNo;
END
ELSE
BEGIN
    INSERT INTO dbo.NDT_Bundle
        (PO_Number, Mill_No, Bundle_No, Total_NDT_Pcs, Context_Slit_No, Slit_Start_Time, Slit_Finish_Time, Rejected_P, NDT_Short_Length_Pipe, Rejected_Short_Length_Pipe, IsReprint)
    VALUES
        (@PoNumber, @MillNo, @BundleNo, @TotalNdtPcs, @SlitNo, @SlitStartTime, @SlitFinishTime, @RejectedPipes, @NdtShortLengthPipe, @RejectedShortLengthPipe, 0);
END";
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
    }

    public async Task<IReadOnlyList<NdtBundleRecord>> GetBundlesAsync(CancellationToken cancellationToken)
    {
        if (UseDatabase && Opt.PreferSqlForReconcileReads)
        {
            try
            {
                var list = await GetBundlesFromSqlAsync(cancellationToken).ConfigureAwait(false);
                if (list.Count > 0 || !Opt.AllowCsvFallbackForBundleReads)
                    return list;

                _logger.LogInformation("NDT_Bundle SQL returned no rows; scanning output CSV folders for bundle list.");
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to get bundles from database.");
                if (!Opt.AllowCsvFallbackForBundleReads)
                    return Array.Empty<NdtBundleRecord>();
            }
        }
        else if (UseDatabase)
        {
            try
            {
                return await GetBundlesFromSqlAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to get bundles from database; falling back to CSV scan.");
            }
        }

        if (!Opt.AllowCsvFallbackForBundleReads)
            return Array.Empty<NdtBundleRecord>();

        return await GetBundlesFromCsvAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<List<NdtBundleRecord>> GetBundlesFromSqlAsync(CancellationToken cancellationToken)
    {
        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        const string sql = @"
WITH Ranked AS (
    SELECT
        Bundle_No,
        PO_Number,
        Mill_No,
        Total_NDT_Pcs,
        Context_Slit_No,
        Slit_Start_Time,
        Slit_Finish_Time,
        Rejected_P,
        NDT_Short_Length_Pipe,
        Rejected_Short_Length_Pipe,
        PrintedAt,
        ROW_NUMBER() OVER (PARTITION BY Bundle_No ORDER BY PrintedAt DESC) AS rn
    FROM dbo.NDT_Bundle
)
SELECT
    Bundle_No AS BundleNo,
    PO_Number AS PoNumber,
    Mill_No AS MillNo,
    Total_NDT_Pcs AS TotalNdtPcs,
    Context_Slit_No AS SlitNo,
    Slit_Start_Time AS SlitStartTime,
    Slit_Finish_Time AS SlitFinishTime,
    Rejected_P AS RejectedPipes,
    NDT_Short_Length_Pipe AS NdtShortLengthPipe,
    Rejected_Short_Length_Pipe AS RejectedShortLengthPipe
FROM Ranked
WHERE rn = 1
ORDER BY PrintedAt DESC";
        await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
        var list = new List<NdtBundleRecord>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            list.Add(ReadBundleFromReader(reader));
        }

        return list;
    }

    public async Task<NdtBundleRecord?> GetByBatchNoAsync(string batchNo, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(batchNo))
            return null;

        if (UseDatabase)
        {
            try
            {
                await using var conn = SqlTraceabilityConnection.Create(Opt);
                await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
                const string sql = "SELECT TOP 1 Bundle_No AS BundleNo, PO_Number AS PoNumber, Mill_No AS MillNo, Total_NDT_Pcs AS TotalNdtPcs, Context_Slit_No AS SlitNo, Slit_Start_Time AS SlitStartTime, Slit_Finish_Time AS SlitFinishTime, Rejected_P AS RejectedPipes, NDT_Short_Length_Pipe AS NdtShortLengthPipe, Rejected_Short_Length_Pipe AS RejectedShortLengthPipe FROM dbo.NDT_Bundle WHERE Bundle_No = @BatchNo ORDER BY PrintedAt DESC";
                await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@BatchNo", batchNo.Trim());
                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    return await ApplyFormedBundleTotalAsync(ReadBundleFromReader(reader), batchNo.Trim(), cancellationToken).ConfigureAwait(false);
                return null;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to get bundle by BatchNo from database.");
            }
        }

        var bundles = await GetBundlesFromCsvAsync(cancellationToken).ConfigureAwait(false);
        var match = bundles.FirstOrDefault(b => b.BundleNo.Equals(batchNo.Trim(), StringComparison.OrdinalIgnoreCase));
        if (match is null)
            return null;
        return await ApplyFormedBundleTotalAsync(match, batchNo.Trim(), cancellationToken).ConfigureAwait(false);
    }

    private async Task<NdtBundleRecord> ApplyFormedBundleTotalAsync(NdtBundleRecord record, string batchNo, CancellationToken cancellationToken)
    {
        var formedTotal = await TryReadBundleSummaryTotalAsync(batchNo, cancellationToken).ConfigureAwait(false);
        if (formedTotal is not > 0 || formedTotal.Value <= record.TotalNdtPcs)
            return record;

        return new NdtBundleRecord
        {
            BundleNo = record.BundleNo,
            PoNumber = record.PoNumber,
            MillNo = record.MillNo,
            TotalNdtPcs = formedTotal.Value,
            SlitNo = record.SlitNo,
            SlitStartTime = record.SlitStartTime,
            SlitFinishTime = record.SlitFinishTime,
            RejectedPipes = record.RejectedPipes,
            NdtShortLengthPipe = record.NdtShortLengthPipe,
            RejectedShortLengthPipe = record.RejectedShortLengthPipe
        };
    }

    private async Task<int?> TryReadBundleSummaryTotalAsync(string batchNo, CancellationToken cancellationToken)
    {
        var folder = GetBundleSummaryFolder();
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return null;

        var path = Path.Combine(folder, NdtBundleOutputPaths.GetBundleCsvFileName(batchNo));
        if (!File.Exists(path))
            return null;

        try
        {
            await using var stream = File.OpenRead(path);
            using var reader = new StreamReader(stream);
            var headerLine = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            if (headerLine is null)
                return null;
            if (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is not { } line)
                return null;

            headerLine = InputSlitCsvParsing.StripBom(headerLine);
            var headers = InputSlitCsvParsing.SplitCsvFields(headerLine);
            var cols = InputSlitCsvParsing.SplitCsvFields(line);
            var idxNd = InputSlitCsvParsing.HeaderIndex(headers, "NDT Pipes");
            if (idxNd < 0)
            {
                if (cols.Length <= ColNdtPipes)
                    return null;
                idxNd = ColNdtPipes;
            }

            if (idxNd >= cols.Length)
                return null;

            return InputSlitCsvParsing.TryParseIntFlexible(cols[idxNd].Trim(), out var ndtPipes)
                ? ndtPipes
                : null;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to read bundle summary total for {BatchNo}.", batchNo);
            return null;
        }
    }

    public async Task UpdateBundlePipesAsync(string batchNo, int newPipes, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(batchNo))
            return;

        if (UseDatabase)
        {
            try
            {
                await using var conn = SqlTraceabilityConnection.Create(Opt);
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
        var folder = Opt.OutputBundleFolder;
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
        if (string.IsNullOrWhiteSpace(batchNo))
            return Array.Empty<(string, int)>();

        var batchNoTrimmed = batchNo.Trim();

        if (UseDatabase)
        {
            try
            {
                var fromSql = await GetSlitsForBatchFromSqlAsync(batchNoTrimmed, cancellationToken).ConfigureAwait(false);
                if (fromSql.Count > 0)
                    return fromSql;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to load slits for batch {BatchNo} from SQL; falling back to per-slit CSV scan.", batchNoTrimmed);
            }
        }

        var folder = Opt.OutputBundleFolder;
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return Array.Empty<(string, int)>();
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

    private async Task<IReadOnlyList<(string SlitNo, int NdtPipes)>> GetSlitsForBatchFromSqlAsync(
        string batchNoTrimmed,
        CancellationToken cancellationToken)
    {
        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        const string sql = @"
SELECT
    COALESCE(NULLIF(LTRIM(RTRIM(Slit_No)), N''), N'—') AS SlitNo,
    SUM(NDT_Pipes) AS NdtPipes
FROM dbo.Output_Slit_Row
WHERE NDT_Batch_No = @BatchNo
GROUP BY COALESCE(NULLIF(LTRIM(RTRIM(Slit_No)), N''), N'—')
ORDER BY SlitNo";

        await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@BatchNo", batchNoTrimmed);

        var list = new List<(string, int)>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var slit = reader.IsDBNull(0) ? "—" : reader.GetString(0);
            var pipes = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
            list.Add((slit, pipes));
        }

        _logger.LogDebug("Loaded {Count} slit row(s) for batch {BatchNo} from Output_Slit_Row.", list.Count, batchNoTrimmed);
        return list;
    }

    public async Task<int> UpdateOutputCsvFilesForSlitAsync(string batchNo, string slitNo, int newPipes, CancellationToken cancellationToken)
    {
        var folder = Opt.OutputBundleFolder;
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

    public async Task<(int RowsRemoved, IReadOnlyList<RemovedSlitRowTraceRef> TraceRefs)> DeletePerSlitOutputRowsForBatchSlitsAsync(
        string batchNo,
        IReadOnlyList<string> slitNos,
        CancellationToken cancellationToken)
    {
        var traceRefs = new List<RemovedSlitRowTraceRef>();
        if (string.IsNullOrWhiteSpace(batchNo) || slitNos.Count == 0)
            return (0, traceRefs);

        var folder = Opt.OutputBundleFolder;
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return (0, traceRefs);

        var batchNoTrimmed = batchNo.Trim();
        var targets = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var sn in slitNos)
        {
            if (string.IsNullOrWhiteSpace(sn))
                targets.Add("—");
            else
                targets.Add(sn.Trim());
        }

        var files = Directory.EnumerateFiles(folder, "*.csv")
            .Where(p => !Path.GetFileName(p).StartsWith("NDT_Bundle_", StringComparison.OrdinalIgnoreCase))
            .OrderBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var rowsRemoved = 0;
        foreach (var path in files)
        {
            string[] rawLines;
            try
            {
                rawLines = await File.ReadAllLinesAsync(path, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Skip delete scan for {Path}.", path);
                continue;
            }

            if (rawLines.Length == 0)
                continue;

            var baseName = Path.GetFileName(path);
            var kept = new List<string> { rawLines[0] };
            var changed = false;

            for (var i = 1; i < rawLines.Length; i++)
            {
                var line = rawLines[i];
                if (string.IsNullOrWhiteSpace(line))
                {
                    kept.Add(line);
                    continue;
                }

                var cols = SplitCsvLine(line);
                if (cols.Count < MinColumns || !cols[ColNdtBatchNo].Trim().Equals(batchNoTrimmed, StringComparison.OrdinalIgnoreCase))
                {
                    kept.Add(line);
                    continue;
                }

                var slitKey = string.IsNullOrWhiteSpace(cols[ColSlitNo]) ? "—" : cols[ColSlitNo].Trim();
                if (!targets.Contains(slitKey))
                {
                    kept.Add(line);
                    continue;
                }

                var poRaw = cols[0].Trim();
                var po = string.IsNullOrWhiteSpace(poRaw) ? string.Empty : InputSlitCsvParsing.NormalizePo(poRaw);
                traceRefs.Add(new RemovedSlitRowTraceRef(baseName, i + 1, po));
                rowsRemoved++;
                changed = true;
            }

            if (!changed)
                continue;

            var hasNonEmptyData = false;
            for (var k = 1; k < kept.Count; k++)
            {
                if (string.IsNullOrWhiteSpace(kept[k]))
                    continue;
                hasNonEmptyData = true;
                break;
            }

            try
            {
                if (!hasNonEmptyData)
                {
                    File.Delete(path);
                    _logger.LogInformation("Deleted per-slit output CSV after slit removal (no data rows left): {Path}", path);
                }
                else
                {
                    await File.WriteAllLinesAsync(path, kept, cancellationToken).ConfigureAwait(false);
                    _logger.LogInformation("Rewrote per-slit output CSV after slit removal: {Path}", path);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to delete or rewrite {Path} after slit removal.", path);
            }
        }

        return (rowsRemoved, traceRefs);
    }

    public async Task UpdateBundleTotalInDatabaseAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(batchNo) || !UseDatabase)
            return;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
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

        var fileName = NdtBundleOutputPaths.GetBundleCsvFileName(batchNo.Trim());
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
        var perSlitFolder = Opt.OutputBundleFolder;
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
                headerLine = InputSlitCsvParsing.StripBom(headerLine);
                var headers = InputSlitCsvParsing.SplitCsvFields(headerLine);
                var cols = InputSlitCsvParsing.SplitCsvFields(line);
                var idxBatch = InputSlitCsvParsing.HeaderIndex(headers, "NDT Batch No");
                var idxNd = InputSlitCsvParsing.HeaderIndex(headers, "NDT Pipes");
                var idxPo = InputSlitCsvParsing.HeaderIndex(headers, "PO Number", "PO_No", "PO No");
                var idxSlit = InputSlitCsvParsing.HeaderIndex(headers, "Slit No");
                var idxMill = InputSlitCsvParsing.HeaderIndex(headers, "Mill No", "Mill Number");
                var idxShort = InputSlitCsvParsing.HeaderIndex(headers, "NDT Short Length Pipe");
                var idxRejShort = InputSlitCsvParsing.HeaderIndex(headers, "Rejected Short Length Pipe");
                if (idxBatch < 0 || idxNd < 0)
                {
                    if (cols.Length < MinColumns) continue;
                    idxBatch = ColNdtBatchNo;
                    idxNd = ColNdtPipes;
                    idxPo = 0;
                    idxSlit = 1;
                    idxMill = 6;
                    idxShort = 7;
                    idxRejShort = 8;
                }
                else if (cols.Length <= Math.Max(idxBatch, idxNd))
                {
                    continue;
                }

                var maxIdx1 = Math.Max(Math.Max(idxBatch, idxNd), Math.Max(Math.Max(idxPo, idxSlit), Math.Max(Math.Max(idxMill, idxShort), idxRejShort)));
                if (maxIdx1 >= cols.Length)
                    continue;

                var bundleNo = cols[idxBatch].Trim();
                if (string.IsNullOrEmpty(bundleNo)) continue;
                if (!InputSlitCsvParsing.TryParseIntFlexible(cols[idxNd].Trim(), out var ndtPipes))
                    ndtPipes = 0;
                var po = idxPo >= 0 && idxPo < cols.Length ? cols[idxPo].Trim() : "";
                var slitNo = idxSlit >= 0 && idxSlit < cols.Length ? cols[idxSlit].Trim() : "";
                var millNo = 0;
                if (idxMill >= 0 && idxMill < cols.Length)
                    _ = int.TryParse(cols[idxMill].Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out millNo);
                var ndtShort = idxShort >= 0 && idxShort < cols.Length ? cols[idxShort].Trim() : "";
                var rejShort = idxRejShort >= 0 && idxRejShort < cols.Length ? cols[idxRejShort].Trim() : "";
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

                var headers = InputSlitCsvParsing.SplitCsvFields(InputSlitCsvParsing.StripBom(headerLine));
                var pIdxBatch = InputSlitCsvParsing.HeaderIndex(headers, "NDT Batch No");
                var pIdxNd = InputSlitCsvParsing.HeaderIndex(headers, "NDT Pipes");
                var pIdxPo = InputSlitCsvParsing.HeaderIndex(headers, "PO Number", "PO_No", "PO No");
                var pIdxSlit = InputSlitCsvParsing.HeaderIndex(headers, "Slit No");
                var pIdxMill = InputSlitCsvParsing.HeaderIndex(headers, "Mill No", "Mill Number");
                var pIdxShort = InputSlitCsvParsing.HeaderIndex(headers, "NDT Short Length Pipe");
                var pIdxRejShort = InputSlitCsvParsing.HeaderIndex(headers, "Rejected Short Length Pipe");
                var useFixed = pIdxBatch < 0 || pIdxNd < 0;

                while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    var cols = InputSlitCsvParsing.SplitCsvFields(line);
                    if (!useFixed && (cols.Length <= Math.Max(pIdxBatch, pIdxNd)))
                        continue;
                    if (useFixed && cols.Length < MinColumns) continue;

                    int idxBatch = useFixed ? ColNdtBatchNo : pIdxBatch;
                    int idxNd = useFixed ? ColNdtPipes : pIdxNd;
                    int idxPo = useFixed ? 0 : pIdxPo;
                    int idxSlit = useFixed ? 1 : pIdxSlit;
                    int idxMill = useFixed ? 6 : pIdxMill;
                    int idxShort = useFixed ? 7 : pIdxShort;
                    int idxRejShort = useFixed ? 8 : pIdxRejShort;

                    var maxIdx = Math.Max(Math.Max(idxBatch, idxNd), Math.Max(Math.Max(idxPo, idxSlit), Math.Max(Math.Max(idxMill, idxShort), idxRejShort)));
                    if (maxIdx >= cols.Length)
                        continue;

                    var bundleNo = cols[idxBatch].Trim();
                    if (string.IsNullOrEmpty(bundleNo)) continue;
                    if (bundleNosFromSummary.Contains(bundleNo)) continue; // already have authoritative total from NDT_Bundle_*.csv
                    if (!InputSlitCsvParsing.TryParseIntFlexible(cols[idxNd].Trim(), out var ndtPipes))
                        ndtPipes = 0;
                    var po = idxPo >= 0 && idxPo < cols.Length ? cols[idxPo].Trim() : "";
                    var slitNo = idxSlit >= 0 && idxSlit < cols.Length ? cols[idxSlit].Trim() : "";
                    var millNo = 0;
                    if (idxMill >= 0 && idxMill < cols.Length)
                        _ = int.TryParse(cols[idxMill].Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out millNo);
                    var ndtShort = idxShort >= 0 && idxShort < cols.Length ? cols[idxShort].Trim() : "";
                    var rejShort = idxRejShort >= 0 && idxRejShort < cols.Length ? cols[idxRejShort].Trim() : "";

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

    private string GetBundleSummaryFolder() =>
        NdtBundleOutputPaths.ResolveBundleArtifactsFolder(Opt) ?? string.Empty;

    public async Task<NdtBundleCsvPurgeResult> PurgeDerivedCsvAndBundlesFromDateAsync(DateTime fromUtc, CancellationToken cancellationToken)
    {
        var summaryDeleted = 0;
        var perSlitDeleted = 0;
        var processDeleted = 0;
        var sqlDeleted = 0;

        var summaryFolder = GetBundleSummaryFolder();
        if (!string.IsNullOrWhiteSpace(summaryFolder) && Directory.Exists(summaryFolder))
        {
            foreach (var path in Directory.EnumerateFiles(summaryFolder, "NDT_Bundle_*.csv", SearchOption.TopDirectoryOnly))
            {
                if (ShouldDeleteBundleSummaryFile(path, fromUtc))
                {
                    File.Delete(path);
                    summaryDeleted++;
                }
            }
        }

        var outputFolder = (Opt.OutputBundleFolder ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(outputFolder) && Directory.Exists(outputFolder))
        {
            foreach (var path in Directory.EnumerateFiles(outputFolder, "*.csv", SearchOption.TopDirectoryOnly))
            {
                if (NdtBundleOutputPaths.IsBundleSummaryFileName(Path.GetFileName(path)))
                    continue;
                if (ShouldDeletePerSlitOutputFile(path, fromUtc))
                {
                    File.Delete(path);
                    perSlitDeleted++;
                }
            }
        }

        var processFolder = (Opt.NdtProcessOutputFolder ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(processFolder) && Directory.Exists(processFolder))
        {
            foreach (var path in Directory.EnumerateFiles(processFolder, "NDT process_*", SearchOption.TopDirectoryOnly))
            {
                try
                {
                    if (File.GetLastWriteTimeUtc(path) >= fromUtc)
                    {
                        File.Delete(path);
                        processDeleted++;
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Could not delete NDT process file {Path}.", path);
                }
            }
        }

        if (UseDatabase)
        {
            try
            {
                await using var conn = SqlTraceabilityConnection.Create(Opt);
                await SqlTraceabilityConnection.OpenAsync(conn, _logger, "NDT_Bundle purge", cancellationToken).ConfigureAwait(false);
                const string sql = "DELETE FROM dbo.NDT_Bundle WHERE PrintedAt >= @FromUtc";
                await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@FromUtc", fromUtc);
                sqlDeleted = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to purge NDT_Bundle rows from {FromUtc:o}.", fromUtc);
            }
        }

        return new NdtBundleCsvPurgeResult
        {
            BundleSummaryFilesDeleted = summaryDeleted,
            PerSlitOutputFilesDeleted = perSlitDeleted,
            NdtProcessFilesDeleted = processDeleted,
            SqlBundlesDeleted = sqlDeleted
        };
    }

    private static bool ShouldDeleteBundleSummaryFile(string path, DateTime fromUtc)
    {
        try
        {
            if (File.GetLastWriteTimeUtc(path) >= fromUtc)
                return true;
        }
        catch
        {
            return false;
        }

        var name = Path.GetFileNameWithoutExtension(path);
        if (!name.StartsWith("NDT_Bundle_", StringComparison.OrdinalIgnoreCase))
            return false;
        var batchNo = name["NDT_Bundle_".Length..];
        for (var mill = 1; mill <= 4; mill++)
        {
            if (NdtBundleSequence.TryParseSequenceForCurrentYear(batchNo, mill, out _))
                return true;
        }

        return false;
    }

    private static bool ShouldDeletePerSlitOutputFile(string path, DateTime fromUtc)
    {
        try
        {
            if (File.GetLastWriteTimeUtc(path) >= fromUtc)
                return true;
        }
        catch
        {
            // fall through to name parse
        }

        var name = Path.GetFileNameWithoutExtension(path);
        var parts = name.Split('_');
        if (parts.Length < 3)
            return false;
        if (parts[1].Length == 8
            && DateTime.TryParseExact(parts[1], "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var fileDate))
        {
            return fileDate.Date >= fromUtc.Date;
        }

        return false;
    }

    public Task<IReadOnlyDictionary<int, int>> GetMaxSequenceByMillForCurrentYearAsync(CancellationToken cancellationToken) =>
        GetMaxSequenceByMillAsync(beforeUtc: null, cancellationToken);

    public Task<IReadOnlyDictionary<int, int>> GetMaxSequenceByMillBeforeUtcAsync(DateTime beforeUtc, CancellationToken cancellationToken) =>
        GetMaxSequenceByMillAsync(beforeUtc, cancellationToken);

    private async Task<IReadOnlyDictionary<int, int>> GetMaxSequenceByMillAsync(DateTime? beforeUtc, CancellationToken cancellationToken)
    {
        var result = new Dictionary<int, int>();

        if (UseDatabase)
        {
            try
            {
                var fromSql = await GetMaxSequenceByMillFromSqlAsync(beforeUtc, cancellationToken).ConfigureAwait(false);
                MergeMaxSequences(result, fromSql);
                if (result.Count > 0)
                    return result;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to read max NDT bundle sequence per mill from SQL.");
            }
        }

        if (Opt.AllowCsvFallbackForBundleReads)
        {
            var fromCsv = await GetMaxSequenceByMillFromCsvAsync(beforeUtc, cancellationToken).ConfigureAwait(false);
            MergeMaxSequences(result, fromCsv);
        }

        return result;
    }

    private async Task<Dictionary<int, int>> GetMaxSequenceByMillFromSqlAsync(DateTime? beforeUtc, CancellationToken cancellationToken)
    {
        var yy = (DateTime.UtcNow.Year % 100).ToString("D2", CultureInfo.InvariantCulture);
        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "max bundle sequence", cancellationToken).ConfigureAwait(false);

        var sql = @"
SELECT Mill_No,
       MAX(TRY_CAST(SUBSTRING(Bundle_No, 6, 5) AS INT)) AS MaxSeq
FROM dbo.NDT_Bundle
WHERE LEN(LTRIM(RTRIM(Bundle_No))) = 10
  AND LEFT(Bundle_No, 2) = '12'
  AND SUBSTRING(Bundle_No, 3, 2) = @Yy
  AND TRY_CAST(SUBSTRING(Bundle_No, 5, 1) AS INT) BETWEEN 1 AND 4
  AND TRY_CAST(SUBSTRING(Bundle_No, 6, 5) AS INT) IS NOT NULL";

        if (beforeUtc.HasValue)
            sql += " AND PrintedAt < @BeforeUtc";

        sql += " GROUP BY Mill_No";

        await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@Yy", yy);
        if (beforeUtc.HasValue)
            cmd.Parameters.AddWithValue("@BeforeUtc", beforeUtc.Value);

        var byMill = new Dictionary<int, int>();
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var mill = reader.GetInt32(0);
            if (reader.IsDBNull(1))
                continue;
            var seq = reader.GetInt32(1);
            if (mill is >= 1 and <= 4 && seq > 0)
                byMill[mill] = seq;
        }

        return byMill;
    }

    private async Task<Dictionary<int, int>> GetMaxSequenceByMillFromCsvAsync(DateTime? beforeUtc, CancellationToken cancellationToken)
    {
        var byMill = new Dictionary<int, int>();
        var bundles = await GetBundlesFromCsvAsync(cancellationToken).ConfigureAwait(false);
        foreach (var bundle in bundles)
        {
            if (bundle.MillNo is < 1 or > 4)
                continue;
            if (!NdtBundleSequence.TryParseSequenceForCurrentYear(bundle.BundleNo, bundle.MillNo, out var seq))
                continue;
            if (beforeUtc.HasValue)
            {
                // CSV scan has no PrintedAt; use file mtime on summary CSV when available.
                var summaryPath = Path.Combine(GetBundleSummaryFolder(), NdtBundleOutputPaths.GetBundleCsvFileName(bundle.BundleNo));
                if (File.Exists(summaryPath))
                {
                    try
                    {
                        if (File.GetLastWriteTimeUtc(summaryPath) >= beforeUtc.Value)
                            continue;
                    }
                    catch
                    {
                        // include row when mtime unknown
                    }
                }
            }

            if (!byMill.TryGetValue(bundle.MillNo, out var max) || seq > max)
                byMill[bundle.MillNo] = seq;
        }

        return byMill;
    }

    private static void MergeMaxSequences(Dictionary<int, int> target, IReadOnlyDictionary<int, int> source)
    {
        foreach (var (mill, seq) in source)
        {
            if (mill is < 1 or > 4 || seq <= 0)
                continue;
            if (!target.TryGetValue(mill, out var existing) || seq > existing)
                target[mill] = seq;
        }
    }

    public async Task<IReadOnlyDictionary<int, int>> GetMaxSequenceByMillForPoNumbersAsync(
        IReadOnlySet<string> poNumbers,
        CancellationToken cancellationToken)
    {
        if (poNumbers.Count == 0)
            return new Dictionary<int, int>();

        var normalized = new HashSet<string>(poNumbers.Select(InputSlitCsvParsing.NormalizePo), StringComparer.OrdinalIgnoreCase);
        var bundles = await GetBundlesAsync(cancellationToken).ConfigureAwait(false);
        var byMill = new Dictionary<int, int>();
        foreach (var bundle in bundles)
        {
            if (!normalized.Contains(InputSlitCsvParsing.NormalizePo(bundle.PoNumber)))
                continue;
            if (bundle.MillNo is < 1 or > 4)
                continue;
            if (!NdtBundleSequence.TryParseSequenceForCurrentYear(bundle.BundleNo, bundle.MillNo, out var seq))
                continue;
            if (!byMill.TryGetValue(bundle.MillNo, out var max) || seq > max)
                byMill[bundle.MillNo] = seq;
        }

        return byMill;
    }

    public async Task<NdtBundleCsvPurgeResult> PurgeDerivedForPoNumbersAsync(
        IReadOnlySet<string> poNumbers,
        DateTime? alsoFromUtc,
        CancellationToken cancellationToken)
    {
        var summaryDeleted = 0;
        var perSlitDeleted = 0;
        var processDeleted = 0;
        var sqlDeleted = 0;

        if (poNumbers.Count == 0)
        {
            return new NdtBundleCsvPurgeResult();
        }

        var normalized = poNumbers.Select(InputSlitCsvParsing.NormalizePo).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        var safeTokens = normalized
            .Select(p => CsvOutputFileNaming.SanitizeToken(p, "NA"))
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var summaryFolder = GetBundleSummaryFolder();
        if (!string.IsNullOrWhiteSpace(summaryFolder) && Directory.Exists(summaryFolder))
        {
            foreach (var path in Directory.EnumerateFiles(summaryFolder, "NDT_Bundle_*.csv", SearchOption.TopDirectoryOnly))
            {
                try
                {
                    if (alsoFromUtc.HasValue && File.GetLastWriteTimeUtc(path) < alsoFromUtc.Value)
                        continue;
                }
                catch
                {
                    // continue with PO match
                }

                if (await FileMatchesAnyPoAsync(path, normalized, safeTokens, cancellationToken).ConfigureAwait(false))
                {
                    File.Delete(path);
                    summaryDeleted++;
                }
            }
        }

        var outputFolder = (Opt.OutputBundleFolder ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(outputFolder) && Directory.Exists(outputFolder))
        {
            foreach (var path in Directory.EnumerateFiles(outputFolder, "*.csv", SearchOption.TopDirectoryOnly))
            {
                if (NdtBundleOutputPaths.IsBundleSummaryFileName(Path.GetFileName(path)))
                    continue;

                var name = Path.GetFileNameWithoutExtension(path);
                if (!safeTokens.Any(token => name.Contains(token, StringComparison.OrdinalIgnoreCase)))
                    continue;

                if (alsoFromUtc.HasValue)
                {
                    try
                    {
                        if (File.GetLastWriteTimeUtc(path) < alsoFromUtc.Value)
                            continue;
                    }
                    catch
                    {
                        // delete when PO matches
                    }
                }

                File.Delete(path);
                perSlitDeleted++;
            }
        }

        var processFolder = (Opt.NdtProcessOutputFolder ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(processFolder) && Directory.Exists(processFolder))
        {
            foreach (var path in Directory.EnumerateFiles(processFolder, "NDT process_*", SearchOption.TopDirectoryOnly))
            {
                if (!safeTokens.Any(token => Path.GetFileName(path).Contains(token, StringComparison.OrdinalIgnoreCase)))
                    continue;
                try
                {
                    if (alsoFromUtc.HasValue && File.GetLastWriteTimeUtc(path) < alsoFromUtc.Value)
                        continue;
                    File.Delete(path);
                    processDeleted++;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Could not delete NDT process file {Path}.", path);
                }
            }
        }

        if (UseDatabase)
        {
            try
            {
                await using var conn = SqlTraceabilityConnection.Create(Opt);
                await SqlTraceabilityConnection.OpenAsync(conn, _logger, "NDT_Bundle purge by PO", cancellationToken).ConfigureAwait(false);
                var inClause = string.Join(", ", normalized.Select((_, i) => $"@Po{i}"));
                var sql = $"DELETE FROM dbo.NDT_Bundle WHERE PO_Number IN ({inClause})";
                await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
                for (var i = 0; i < normalized.Count; i++)
                    cmd.Parameters.AddWithValue($"@Po{i}", normalized[i]);
                sqlDeleted = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to purge NDT_Bundle rows for planned-month PO list.");
            }
        }

        return new NdtBundleCsvPurgeResult
        {
            BundleSummaryFilesDeleted = summaryDeleted,
            PerSlitOutputFilesDeleted = perSlitDeleted,
            NdtProcessFilesDeleted = processDeleted,
            SqlBundlesDeleted = sqlDeleted
        };
    }

    private static async Task<bool> FileMatchesAnyPoAsync(
        string path,
        IReadOnlyList<string> normalizedPos,
        IReadOnlySet<string> safeTokens,
        CancellationToken cancellationToken)
    {
        var fileName = Path.GetFileNameWithoutExtension(path);
        if (safeTokens.Any(token => fileName.Contains(token, StringComparison.OrdinalIgnoreCase)))
            return true;

        try
        {
            await using var stream = File.OpenRead(path);
            using var reader = new StreamReader(stream);
            var header = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            if (header is null)
                return false;
            var headers = InputSlitCsvParsing.SplitCsvFields(InputSlitCsvParsing.StripBom(header));
            var poIdx = InputSlitCsvParsing.HeaderIndex(headers, "PO Number", "PO_No", "PO No");
            if (poIdx < 0)
                return false;
            if (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is not { } line)
                return false;
            var cols = InputSlitCsvParsing.SplitCsvFields(line);
            if (poIdx >= cols.Length)
                return false;
            var po = InputSlitCsvParsing.NormalizePo(cols[poIdx].Trim());
            return normalizedPos.Any(p => InputSlitCsvParsing.PoEquals(p, po));
        }
        catch
        {
            return false;
        }
    }
}
