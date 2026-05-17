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
        if (UseDatabase)
        {
            try
            {
                await using var conn = SqlTraceabilityConnection.Create(Opt);
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
        var folder = Opt.OutputBundleFolder;
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
}
