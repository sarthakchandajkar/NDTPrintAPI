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

    private static string TruncatePrintError(string? error) =>
        string.IsNullOrEmpty(error) ? string.Empty : error.Length <= 500 ? error : error[..500];

    public async Task RecordBundlePendingPrintAsync(NdtBundleRecord record, CancellationToken cancellationToken)
    {
        if (!UseDatabase)
        {
            _logger.LogWarning(
                "NDT_Bundle pending-print SQL write skipped for {BundleNo}: SQL not configured.",
                record.BundleNo);
            return;
        }

        const int maxAttempts = 3;
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await UpsertBundlePendingPrintAsync(record, cancellationToken).ConfigureAwait(false);
                _writeTracker.RecordSuccess("NDT_Bundle", $"{record.BundleNo} pending print");
                _logger.LogInformation(
                    "Recorded bundle {BundleNo} ({Total} NDT pcs) in NDT_Bundle with Print_Status=Pending.",
                    record.BundleNo,
                    record.TotalNdtPcs);
                return;
            }
            catch (Exception ex) when (attempt < maxAttempts)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to record pending-print bundle {BundleNo} (attempt {Attempt}/{Max}); retrying.",
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
                    "Failed to record pending-print bundle {BundleNo} after {Max} attempts.",
                    record.BundleNo,
                    maxAttempts);
            }
        }
    }

    public async Task UpdateBundlePrintStatusAsync(
        string bundleNo,
        string printStatus,
        string? printError,
        CancellationToken cancellationToken)
    {
        if (!UseDatabase || string.IsNullOrWhiteSpace(bundleNo))
            return;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await SqlTraceabilityConnection.OpenAsync(conn, _logger, "NDT_Bundle print status", cancellationToken).ConfigureAwait(false);
            const string sql = @"
UPDATE dbo.NDT_Bundle
SET Print_Status = @Status,
    Print_Error = @Error,
    Print_Attempted_At = COALESCE(Print_Attempted_At, SYSDATETIME())
WHERE Bundle_No = @BundleNo";
            await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@BundleNo", bundleNo.Trim());
            cmd.Parameters.AddWithValue("@Status", printStatus);
            cmd.Parameters.AddWithValue("@Error", (object?)TruncatePrintError(printError) ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to update print status for bundle {BundleNo} to {Status}.", bundleNo, printStatus);
        }
    }

    public async Task<IReadOnlyList<NdtBundleRecord>> GetStuckPrintsAsync(TimeSpan olderThan, CancellationToken cancellationToken)
    {
        if (!UseDatabase)
            return Array.Empty<NdtBundleRecord>();

        var cutoffUtc = DateTime.UtcNow - olderThan;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            const string sql = @"
SELECT
    Bundle_No AS BundleNo,
    PO_Number AS PoNumber,
    Mill_No AS MillNo,
    Total_NDT_Pcs AS TotalNdtPcs,
    Context_Slit_No AS SlitNo,
    Slit_Start_Time AS SlitStartTime,
    Slit_Finish_Time AS SlitFinishTime,
    PrintedAt,
    Rejected_P AS RejectedPipes,
    NDT_Short_Length_Pipe AS NdtShortLengthPipe,
    Rejected_Short_Length_Pipe AS RejectedShortLengthPipe,
    Print_Status AS PrintStatus,
    Print_Attempted_At AS PrintAttemptedAt,
    Print_Error AS PrintError
FROM dbo.NDT_Bundle
WHERE Print_Status IN ('Pending', 'PrintFailed')
  AND COALESCE(Print_Attempted_At, PrintedAt) < @CutoffUtc
ORDER BY COALESCE(Print_Attempted_At, PrintedAt)";
            await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@CutoffUtc", cutoffUtc);
            var list = new List<NdtBundleRecord>();
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                list.Add(ReadBundleFromReader(reader));
            return list;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get stuck prints from database.");
            return Array.Empty<NdtBundleRecord>();
        }
    }

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
        AddBundleUpsertParameters(cmd, record);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task UpsertBundlePendingPrintAsync(NdtBundleRecord record, CancellationToken cancellationToken)
    {
        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "NDT_Bundle pending-print upsert", cancellationToken).ConfigureAwait(false);
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
        IsReprint = 0,
        Print_Status = 'Pending',
        Print_Attempted_At = SYSDATETIME(),
        Print_Error = NULL
    WHERE Bundle_No = @BundleNo;
END
ELSE
BEGIN
    INSERT INTO dbo.NDT_Bundle
        (PO_Number, Mill_No, Bundle_No, Total_NDT_Pcs, Context_Slit_No, Slit_Start_Time, Slit_Finish_Time, Rejected_P, NDT_Short_Length_Pipe, Rejected_Short_Length_Pipe, IsReprint, Print_Status, Print_Attempted_At, Print_Error)
    VALUES
        (@PoNumber, @MillNo, @BundleNo, @TotalNdtPcs, @SlitNo, @SlitStartTime, @SlitFinishTime, @RejectedPipes, @NdtShortLengthPipe, @RejectedShortLengthPipe, 0, 'Pending', SYSDATETIME(), NULL);
END";
        await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
        AddBundleUpsertParameters(cmd, record);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static void AddBundleUpsertParameters(Microsoft.Data.SqlClient.SqlCommand cmd, NdtBundleRecord record)
    {
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
        PrintedAt,
        Rejected_P,
        NDT_Short_Length_Pipe,
        Rejected_Short_Length_Pipe,
        Print_Status,
        Print_Attempted_At,
        Print_Error,
        ROW_NUMBER() OVER (PARTITION BY Bundle_No ORDER BY PrintedAt DESC) AS rn
    FROM dbo.NDT_Bundle
),
SlitSum AS (
    SELECT NDT_Batch_No, SUM(NDT_Pipes) AS SlitTotal
    FROM dbo.Output_Slit_Row
    GROUP BY NDT_Batch_No
)
SELECT
    r.Bundle_No AS BundleNo,
    r.PO_Number AS PoNumber,
    r.Mill_No AS MillNo,
    CASE
        WHEN r.Total_NDT_Pcs > 0 THEN r.Total_NDT_Pcs
        ELSE COALESCE(s.SlitTotal, r.Total_NDT_Pcs)
    END AS TotalNdtPcs,
    r.Context_Slit_No AS SlitNo,
    r.Slit_Start_Time AS SlitStartTime,
    r.Slit_Finish_Time AS SlitFinishTime,
    r.PrintedAt,
    r.Rejected_P AS RejectedPipes,
    r.NDT_Short_Length_Pipe AS NdtShortLengthPipe,
    r.Rejected_Short_Length_Pipe AS RejectedShortLengthPipe,
    r.Print_Status AS PrintStatus,
    r.Print_Attempted_At AS PrintAttemptedAt,
    r.Print_Error AS PrintError
FROM Ranked r
LEFT JOIN SlitSum s ON s.NDT_Batch_No = r.Bundle_No
WHERE r.rn = 1
ORDER BY r.PrintedAt DESC";
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
                const string sql = @"
SELECT TOP 1
    Bundle_No AS BundleNo,
    PO_Number AS PoNumber,
    Mill_No AS MillNo,
    Total_NDT_Pcs AS TotalNdtPcs,
    Context_Slit_No AS SlitNo,
    Slit_Start_Time AS SlitStartTime,
    Slit_Finish_Time AS SlitFinishTime,
    PrintedAt,
    Rejected_P AS RejectedPipes,
    NDT_Short_Length_Pipe AS NdtShortLengthPipe,
    Rejected_Short_Length_Pipe AS RejectedShortLengthPipe,
    Print_Status AS PrintStatus,
    Print_Attempted_At AS PrintAttemptedAt,
    Print_Error AS PrintError,
    Close_Source AS CloseSource,
    Awaiting_Csv_Recon AS AwaitingCsvRecon,
    Count_Discrepancy AS CountDiscrepancy,
    Manual_Recon AS ManualRecon,
    Manual_Recon_By AS ManualReconBy,
    Manual_Recon_At AS ManualReconAt,
    Manual_Recon_Reason AS ManualReconReason,
    Manual_Recon_Original_Total AS ManualReconOriginalTotal,
    Post_Recon_Csv_Sum AS PostReconCsvSum
FROM dbo.NDT_Bundle WHERE Bundle_No = @BatchNo ORDER BY PrintedAt DESC";
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

    public async Task<NdtBundleRecord?> GetLatestPrintedBundleForMillAsync(int millNo, CancellationToken cancellationToken)
    {
        if (millNo is < 1 or > 4 || !UseDatabase)
            return null;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            const string sql = @"
SELECT TOP 1
    Bundle_No AS BundleNo,
    PO_Number AS PoNumber,
    Mill_No AS MillNo,
    Total_NDT_Pcs AS TotalNdtPcs,
    Context_Slit_No AS SlitNo,
    Slit_Start_Time AS SlitStartTime,
    Slit_Finish_Time AS SlitFinishTime,
    PrintedAt,
    Rejected_P AS RejectedPipes,
    NDT_Short_Length_Pipe AS NdtShortLengthPipe,
    Rejected_Short_Length_Pipe AS RejectedShortLengthPipe,
    Print_Status AS PrintStatus,
    Print_Attempted_At AS PrintAttemptedAt,
    Print_Error AS PrintError
FROM dbo.NDT_Bundle
WHERE Mill_No = @MillNo AND Total_NDT_Pcs > 0
ORDER BY PrintedAt DESC";
            await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@MillNo", millNo);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                return ReadBundleFromReader(reader);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to get latest printed bundle for Mill {MillNo} from database.", millNo);
        }

        return null;
    }

    public async Task<bool> HasPrintedBundleForPoAsync(int millNo, string poNumber, CancellationToken cancellationToken)
    {
        if (millNo is < 1 or > 4 || string.IsNullOrWhiteSpace(poNumber) || !UseDatabase)
            return false;

        var normalized = InputSlitCsvParsing.NormalizePo(poNumber);
        var requested = poNumber.Trim();

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            const string sql = @"
SELECT TOP 1 1
FROM dbo.NDT_Bundle
WHERE Mill_No = @MillNo
  AND Total_NDT_Pcs > 0
  AND (PO_Number = @Po OR PO_Number = @PoNormalized)";
            await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@MillNo", millNo);
            cmd.Parameters.AddWithValue("@Po", requested);
            cmd.Parameters.AddWithValue("@PoNormalized", normalized);
            var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return result is not null;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to check printed bundle for Mill {MillNo} PO {Po} in database.", millNo, poNumber);
            return false;
        }
    }

    public async Task<int> MarkManualReviewAsync(string poNumber, int millNo, CancellationToken cancellationToken)
    {
        if (millNo is < 1 or > 4 || string.IsNullOrWhiteSpace(poNumber) || !UseDatabase)
            return 0;

        var normalized = InputSlitCsvParsing.NormalizePo(poNumber);
        var requested = poNumber.Trim();

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            const string sql = @"
UPDATE dbo.NDT_Bundle
SET Manual_Review = 1
WHERE Mill_No = @MillNo
  AND (PO_Number = @Po OR PO_Number = @PoNormalized);";
            await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@MillNo", millNo);
            cmd.Parameters.AddWithValue("@Po", requested);
            cmd.Parameters.AddWithValue("@PoNormalized", normalized);
            var updated = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            if (updated > 0)
            {
                _logger.LogWarning(
                    "Marked Manual_Review=1 on {Count} NDT_Bundle row(s) for PO {PO} Mill {Mill}.",
                    updated,
                    normalized,
                    millNo);
            }

            return updated;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to mark Manual_Review for PO {PO} Mill {Mill} (run docs/NDT_Bundle_Alter_ManualReview.sql if column is missing).",
                normalized,
                millNo);
            return 0;
        }
    }

    public async Task TrySetPlcCloseMetadataAsync(int engineBatchSequence, int millNo, CancellationToken cancellationToken)
    {
        if (!UseDatabase || engineBatchSequence <= 0 || millNo is < 1 or > 4)
            return;

        var yy = (DateTime.Now.Year % 100).ToString("D2", System.Globalization.CultureInfo.InvariantCulture);
        var bundleNo = "12" + yy + millNo.ToString(System.Globalization.CultureInfo.InvariantCulture) +
                       engineBatchSequence.ToString("D5", System.Globalization.CultureInfo.InvariantCulture);

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            const string sql = @"
UPDATE dbo.NDT_Bundle
SET Close_Source = N'Plc',
    Awaiting_Csv_Recon = 1
WHERE Bundle_No = @BundleNo;";
            await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@BundleNo", bundleNo);
            var updated = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            if (updated > 0)
            {
                _logger.LogInformation(
                    "Set Close_Source=Plc Awaiting_Csv_Recon=1 on bundle {BundleNo}.",
                    bundleNo);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to set PLC close metadata for {BundleNo} (run docs/NDT_Bundle_Alter_CloseSource.sql if columns are missing).",
                bundleNo);
        }
    }

    public async Task<(string BundleNo, int EngineSequence, int PlcTotal)?> TryGetAwaitingPlcReconBatchAsync(
        string poNumber,
        int millNo,
        CancellationToken cancellationToken)
    {
        if (!UseDatabase || millNo is < 1 or > 4)
            return null;

        var normalized = InputSlitCsvParsing.NormalizePo(poNumber);
        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            const string findSql = @"
SELECT TOP 1 Bundle_No, Total_NDT_Pcs
FROM dbo.NDT_Bundle
WHERE Mill_No = @MillNo
  AND Awaiting_Csv_Recon = 1
  AND Close_Source = N'Plc'
  AND Manual_Recon = 0
  AND (PO_Number = @Po OR PO_Number = @PoNormalized)
ORDER BY PrintedAt ASC;";
            await using var find = new Microsoft.Data.SqlClient.SqlCommand(findSql, conn);
            find.Parameters.AddWithValue("@MillNo", millNo);
            find.Parameters.AddWithValue("@Po", poNumber.Trim());
            find.Parameters.AddWithValue("@PoNormalized", normalized);
            await using var reader = await find.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                return null;

            var bundleNo = reader.GetString(0);
            var plcTotal = reader.GetInt32(1);
            if (!TryParseEngineSequenceFromBundleNo(bundleNo, out var seq))
                return null;
            return (bundleNo, seq, plcTotal);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(
                ex,
                "TryGetAwaitingPlcReconBatch failed for PO {PO} Mill {Mill} (columns may be missing).",
                normalized,
                millNo);
            return null;
        }
    }

    public async Task<IReadOnlyList<PlcCsvReconAwaitingBundle>> ListAwaitingPlcReconBatchesAsync(
        string poNumber,
        int millNo,
        CancellationToken cancellationToken)
    {
        if (!UseDatabase || millNo is < 1 or > 4)
            return Array.Empty<PlcCsvReconAwaitingBundle>();

        var normalized = InputSlitCsvParsing.NormalizePo(poNumber);
        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            const string findSql = @"
SELECT Bundle_No, Total_NDT_Pcs, PrintedAt
FROM dbo.NDT_Bundle
WHERE Mill_No = @MillNo
  AND Awaiting_Csv_Recon = 1
  AND Close_Source = N'Plc'
  AND Manual_Recon = 0
  AND (PO_Number = @Po OR PO_Number = @PoNormalized)
ORDER BY PrintedAt ASC;";
            await using var find = new Microsoft.Data.SqlClient.SqlCommand(findSql, conn);
            find.Parameters.AddWithValue("@MillNo", millNo);
            find.Parameters.AddWithValue("@Po", poNumber.Trim());
            find.Parameters.AddWithValue("@PoNormalized", normalized);
            await using var reader = await find.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

            var rows = new List<(string BundleNo, int PlcTotal, DateTime PrintedAtUtc)>();
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                rows.Add((
                    reader.GetString(0),
                    reader.GetInt32(1),
                    reader.GetDateTime(2)));
            }

            await reader.CloseAsync().ConfigureAwait(false);

            var result = new List<PlcCsvReconAwaitingBundle>(rows.Count);
            foreach (var (bundleNo, plcTotal, printedAtUtc) in rows)
            {
                if (!TryParseEngineSequenceFromBundleNo(bundleNo, out var seq))
                    continue;

                var slits = await GetSlitsForBatchAsync(bundleNo, cancellationToken).ConfigureAwait(false);
                var slitSum = slits.Sum(s => s.NdtPipes);
                result.Add(new PlcCsvReconAwaitingBundle(
                    bundleNo,
                    seq,
                    plcTotal,
                    slitSum,
                    printedAtUtc));
            }

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(
                ex,
                "ListAwaitingPlcReconBatches failed for PO {PO} Mill {Mill} (columns may be missing).",
                normalized,
                millNo);
            return Array.Empty<PlcCsvReconAwaitingBundle>();
        }
    }

    public async Task<PlcCsvReconResult?> TryFinalizePlcReconBundleAsync(
        string bundleNo,
        int slitSum,
        int reconWindowMinutes,
        DateTime utcNow,
        bool force,
        CancellationToken cancellationToken)
    {
        if (!UseDatabase || slitSum < 0 || string.IsNullOrWhiteSpace(bundleNo))
            return null;

        var bundleNoTrimmed = bundleNo.Trim();
        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            const string findSql = @"
SELECT Total_NDT_Pcs, PrintedAt
FROM dbo.NDT_Bundle
WHERE Bundle_No = @BundleNo
  AND Awaiting_Csv_Recon = 1
  AND Close_Source = N'Plc';";
            int plcTotal;
            DateTime printedAtUtc;
            await using (var find = new Microsoft.Data.SqlClient.SqlCommand(findSql, conn))
            {
                find.Parameters.AddWithValue("@BundleNo", bundleNoTrimmed);
                await using var reader = await find.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    return null;
                plcTotal = reader.GetInt32(0);
                printedAtUtc = reader.GetDateTime(1);
            }

            var applied = PlcCsvReconSemantics.EvaluateFinalize(
                bundleNoTrimmed,
                plcTotal,
                slitSum,
                printedAtUtc,
                reconWindowMinutes,
                utcNow,
                force);

            if (!applied.ClearsAwaitingCsvRecon)
                return null;

            const string updateSql = @"
UPDATE dbo.NDT_Bundle
SET Awaiting_Csv_Recon = 0,
    Count_Discrepancy = @Discrepancy
WHERE Bundle_No = @BundleNo;";
            await using var upd = new Microsoft.Data.SqlClient.SqlCommand(updateSql, conn);
            upd.Parameters.AddWithValue("@BundleNo", bundleNoTrimmed);
            upd.Parameters.AddWithValue("@Discrepancy", applied.CountDiscrepancy ? 1 : 0);
            await upd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            if (applied.CountDiscrepancy)
            {
                _logger.LogWarning(
                    "PLC vs CSV count discrepancy for bundle {BundleNo}: plc={PlcTotal} slitSum={SlitSum}.",
                    bundleNoTrimmed,
                    plcTotal,
                    slitSum);
            }
            else
            {
                _logger.LogInformation(
                    "PLC closed bundle {BundleNo} reconciled with slit sum {SlitSum}.",
                    bundleNoTrimmed,
                    slitSum);
            }

            return new PlcCsvReconResult
            {
                BundleNo = applied.BundleNo,
                PlcTotal = applied.PlcTotal,
                SlitSum = applied.SlitSum
            };
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "PLC CSV recon finalize failed for bundle {BundleNo} (run docs/NDT_Bundle_Alter_CloseSource.sql if columns are missing).",
                bundleNoTrimmed);
            return null;
        }
    }

    public async Task<IReadOnlyList<PlcCsvReconResult>> TryFinalizeReadyPlcReconBundlesAsync(
        string poNumber,
        int millNo,
        int reconWindowMinutes,
        DateTime utcNow,
        CancellationToken cancellationToken)
    {
        var awaiting = await ListAwaitingPlcReconBatchesAsync(poNumber, millNo, cancellationToken)
            .ConfigureAwait(false);
        if (awaiting.Count == 0)
            return Array.Empty<PlcCsvReconResult>();

        var finalized = new List<PlcCsvReconResult>();
        foreach (var bundle in awaiting)
        {
            if (!PlcCsvReconFifo.ShouldFinalize(
                    bundle.PlcTotal,
                    bundle.CurrentSlitSum,
                    bundle.PrintedAtUtc,
                    reconWindowMinutes,
                    utcNow))
            {
                continue;
            }

            var result = await TryFinalizePlcReconBundleAsync(
                    bundle.BundleNo,
                    bundle.CurrentSlitSum,
                    reconWindowMinutes,
                    utcNow,
                    force: false,
                    cancellationToken)
                .ConfigureAwait(false);
            if (result is not null)
                finalized.Add(result);
        }

        return finalized;
    }

    public async Task<PlcCsvReconResult?> TryReconcilePlcClosedBundleAsync(
        string poNumber,
        int millNo,
        int slitSum,
        CancellationToken cancellationToken)
    {
        var oldest = await TryGetAwaitingPlcReconBatchAsync(poNumber, millNo, cancellationToken)
            .ConfigureAwait(false);
        if (oldest is null)
            return null;

        return await TryFinalizePlcReconBundleAsync(
                oldest.Value.BundleNo,
                slitSum,
                reconWindowMinutes: 1,
                DateTime.UtcNow,
                force: true,
                cancellationToken)
            .ConfigureAwait(false);
    }

    public async Task<PlcCsvReconResult?> TryForceFinalizeAwaitingReconOnReopenAsync(
        string poNumber,
        int millNo,
        CancellationToken cancellationToken)
    {
        var awaiting = await ListAwaitingPlcReconBatchesAsync(poNumber, millNo, cancellationToken)
            .ConfigureAwait(false);
        if (awaiting.Count == 0)
            return null;

        PlcCsvReconResult? last = null;
        foreach (var bundle in awaiting)
        {
            var result = await TryFinalizePlcReconBundleAsync(
                    bundle.BundleNo,
                    bundle.CurrentSlitSum,
                    reconWindowMinutes: 1,
                    DateTime.UtcNow,
                    force: true,
                    cancellationToken)
                .ConfigureAwait(false);

            if (result is null)
                continue;

            last = result;
            if (result.CountDiscrepancy)
            {
                _logger.LogWarning(
                    "PO reopen force-finalized awaiting recon for bundle {BundleNo}: plc={PlcTotal} slitSum={SlitSum} (rows received so far).",
                    result.BundleNo,
                    result.PlcTotal,
                    result.SlitSum);
            }
            else
            {
                _logger.LogInformation(
                    "PO reopen force-finalized awaiting recon for bundle {BundleNo} with slit sum {SlitSum}.",
                    result.BundleNo,
                    result.SlitSum);
            }
        }

        return last;
    }

    public async Task<bool> IsManualReconLockedAsync(string batchNo, CancellationToken cancellationToken)
    {
        if (!UseDatabase || string.IsNullOrWhiteSpace(batchNo))
            return false;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            const string sql = @"
SELECT TOP 1 Manual_Recon
FROM dbo.NDT_Bundle
WHERE Bundle_No = @BatchNo
ORDER BY PrintedAt DESC;";
            await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@BatchNo", batchNo.Trim());
            var scalar = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return scalar is bool b && b || scalar is 1 or (byte)1;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not read Manual_Recon for {BatchNo} (column may be missing).", batchNo);
            return false;
        }
    }

    public async Task<ManualBundleReconcileResult?> ManualReconcileBundleAsync(
        string batchNo,
        int correctedTotal,
        string reason,
        string reconciledBy,
        CancellationToken cancellationToken)
    {
        if (!UseDatabase || string.IsNullOrWhiteSpace(batchNo) || correctedTotal < 0)
            return null;

        var batchNoTrimmed = batchNo.Trim();
        var reasonTrimmed = TruncateManualReconReason(reason);
        var byTrimmed = TruncateManualReconBy(reconciledBy);
        var slits = await GetSlitsForBatchAsync(batchNoTrimmed, cancellationToken).ConfigureAwait(false);
        var slitSum = slits.Sum(s => s.NdtPipes);
        var reconciledAtUtc = DateTime.UtcNow;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var tx = await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

            int plcTotal;
            bool awaitingCsvRecon;
            string? closeSource;
            int? existingOriginalTotal;
            await using (var find = new Microsoft.Data.SqlClient.SqlCommand(@"
SELECT Total_NDT_Pcs, Awaiting_Csv_Recon, Close_Source, Manual_Recon_Original_Total
FROM dbo.NDT_Bundle
WHERE Bundle_No = @BundleNo;", conn, (Microsoft.Data.SqlClient.SqlTransaction)tx))
            {
                find.Parameters.AddWithValue("@BundleNo", batchNoTrimmed);
                await using var reader = await find.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    await tx.RollbackAsync(cancellationToken).ConfigureAwait(false);
                    return null;
                }

                plcTotal = reader.GetInt32(0);
                awaitingCsvRecon = !reader.IsDBNull(1) && reader.GetBoolean(1);
                closeSource = reader.IsDBNull(2) ? null : reader.GetString(2);
                existingOriginalTotal = reader.IsDBNull(3) ? null : reader.GetInt32(3);
            }

            var originalTotal = existingOriginalTotal ?? plcTotal;
            var finalize = ManualBundleReconcileSemantics.EvaluateForceFinalize(
                awaitingCsvRecon,
                closeSource,
                plcTotal,
                slitSum);

            const string updateSql = @"
UPDATE dbo.NDT_Bundle
SET Awaiting_Csv_Recon = CASE WHEN @ForceFinalized = 1 THEN 0 ELSE Awaiting_Csv_Recon END,
    Count_Discrepancy = CASE WHEN @ForceFinalized = 1 THEN @Discrepancy ELSE Count_Discrepancy END,
    Manual_Recon = 1,
    Manual_Recon_By = @By,
    Manual_Recon_At = @At,
    Manual_Recon_Reason = @Reason,
    Manual_Recon_Original_Total = COALESCE(Manual_Recon_Original_Total, @OriginalTotal),
    Total_NDT_Pcs = @CorrectedTotal
WHERE Bundle_No = @BundleNo;";
            await using (var upd = new Microsoft.Data.SqlClient.SqlCommand(updateSql, conn, (Microsoft.Data.SqlClient.SqlTransaction)tx))
            {
                upd.Parameters.AddWithValue("@ForceFinalized", finalize.ForceFinalized ? 1 : 0);
                upd.Parameters.AddWithValue("@Discrepancy", finalize.CountDiscrepancy ? 1 : 0);
                upd.Parameters.AddWithValue("@By", byTrimmed);
                upd.Parameters.AddWithValue("@At", reconciledAtUtc);
                upd.Parameters.AddWithValue("@Reason", reasonTrimmed);
                upd.Parameters.AddWithValue("@OriginalTotal", originalTotal);
                upd.Parameters.AddWithValue("@CorrectedTotal", correctedTotal);
                upd.Parameters.AddWithValue("@BundleNo", batchNoTrimmed);
                var rows = await upd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                if (rows == 0)
                {
                    await tx.RollbackAsync(cancellationToken).ConfigureAwait(false);
                    return null;
                }
            }

            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);

            if (finalize.ForceFinalized && finalize.CountDiscrepancy)
            {
                _logger.LogWarning(
                    "Manual reconcile force-finalized awaiting recon for bundle {BundleNo}: plc={PlcTotal} slitSum={SlitSum} (rows received so far).",
                    batchNoTrimmed,
                    finalize.PlcTotalAtFinalize,
                    finalize.SlitSumAtFinalize);
            }
            else if (finalize.ForceFinalized)
            {
                _logger.LogInformation(
                    "Manual reconcile force-finalized awaiting recon for bundle {BundleNo} with slit sum {SlitSum}.",
                    batchNoTrimmed,
                    finalize.SlitSumAtFinalize);
            }

            _logger.LogInformation(
                "Manual bundle reconcile for {BatchNo}: original={Original} corrected={Corrected} by={By}.",
                batchNoTrimmed,
                originalTotal,
                correctedTotal,
                byTrimmed);

            var bundle = await GetByBatchNoAsync(batchNoTrimmed, cancellationToken).ConfigureAwait(false);
            if (bundle is null)
                return null;

            return new ManualBundleReconcileResult
            {
                Bundle = bundle,
                OriginalTotal = originalTotal,
                CorrectedTotal = correctedTotal,
                ForceFinalized = finalize.ForceFinalized,
                CountDiscrepancyLogged = finalize.ForceFinalized && finalize.CountDiscrepancy,
                SlitSumAtFinalize = slitSum
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Manual bundle reconcile failed for {BatchNo} (run docs/NDT_Bundle_Alter_ManualRecon.sql if columns are missing).",
                batchNoTrimmed);
            throw;
        }
    }

    public async Task<int> TryUpdatePostReconCsvSumAsync(string batchNo, CancellationToken cancellationToken)
    {
        if (!UseDatabase || string.IsNullOrWhiteSpace(batchNo))
            return 0;

        var batchNoTrimmed = batchNo.Trim();
        if (!await IsManualReconLockedAsync(batchNoTrimmed, cancellationToken).ConfigureAwait(false))
            return 0;

        var slits = await GetSlitsForBatchAsync(batchNoTrimmed, cancellationToken).ConfigureAwait(false);
        var slitSum = slits.Sum(s => s.NdtPipes);

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            const string sql = @"
UPDATE dbo.NDT_Bundle
SET Post_Recon_Csv_Sum = @SlitSum
WHERE Bundle_No = @BatchNo
  AND Manual_Recon = 1;";
            await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@SlitSum", slitSum);
            cmd.Parameters.AddWithValue("@BatchNo", batchNoTrimmed);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            return slitSum;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not update Post_Recon_Csv_Sum for {BatchNo}.", batchNoTrimmed);
            return 0;
        }
    }

    private static string TruncateManualReconReason(string reason)
    {
        var trimmed = reason.Trim();
        return trimmed.Length <= 500 ? trimmed : trimmed[..500];
    }

    private static string TruncateManualReconBy(string reconciledBy)
    {
        var trimmed = reconciledBy.Trim();
        return trimmed.Length <= 100 ? trimmed : trimmed[..100];
    }

    private static bool TryParseEngineSequenceFromBundleNo(string bundleNo, out int sequence)
    {
        sequence = 0;
        if (string.IsNullOrWhiteSpace(bundleNo) || bundleNo.Length < 5)
            return false;
        return int.TryParse(
            bundleNo.AsSpan(bundleNo.Length - 5),
            System.Globalization.NumberStyles.None,
            System.Globalization.CultureInfo.InvariantCulture,
            out sequence);
    }

    private async Task<NdtBundleRecord> ApplyFormedBundleTotalAsync(NdtBundleRecord record, string batchNo, CancellationToken cancellationToken)
    {
        var formedTotal = await TryReadBundleSummaryTotalAsync(batchNo, cancellationToken).ConfigureAwait(false);
        if (formedTotal is > 0 && formedTotal.Value > record.TotalNdtPcs)
        {
            return new NdtBundleRecord
            {
                BundleNo = record.BundleNo,
                PoNumber = record.PoNumber,
                MillNo = record.MillNo,
                TotalNdtPcs = formedTotal.Value,
                SlitNo = record.SlitNo,
                SlitStartTime = record.SlitStartTime,
                SlitFinishTime = record.SlitFinishTime,
                PrintedAt = record.PrintedAt,
                PrintStatus = record.PrintStatus,
                PrintAttemptedAt = record.PrintAttemptedAt,
                PrintError = record.PrintError,
                RejectedPipes = record.RejectedPipes,
                NdtShortLengthPipe = record.NdtShortLengthPipe,
                RejectedShortLengthPipe = record.RejectedShortLengthPipe
            };
        }

        if (record.TotalNdtPcs > 0)
            return record;

        var slits = await GetSlitsForBatchAsync(batchNo, cancellationToken).ConfigureAwait(false);
        var slitSum = slits.Sum(s => s.NdtPipes);
        if (slitSum <= 0)
            return record;

        return new NdtBundleRecord
        {
            BundleNo = record.BundleNo,
            PoNumber = record.PoNumber,
            MillNo = record.MillNo,
            TotalNdtPcs = slitSum,
            SlitNo = record.SlitNo,
            SlitStartTime = record.SlitStartTime,
            SlitFinishTime = record.SlitFinishTime,
            PrintedAt = record.PrintedAt,
            PrintStatus = record.PrintStatus,
            PrintAttemptedAt = record.PrintAttemptedAt,
            PrintError = record.PrintError,
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
        // UNC per-slit folders can hold thousands of CSVs; bundle reconcile must not honor HTTP cancellation.
        var ioToken = CancellationToken.None;
        var filesOrdered = await ResolvePerSlitCsvPathsForBundleUpdateAsync(batchNoTrimmed, folder).ConfigureAwait(false);

        // Find the last file (by name order) that contains this NDT Batch No
        string? lastFileWithBatch = null;
        foreach (var path in filesOrdered)
        {
            try
            {
                var lines = await File.ReadAllLinesAsync(path, ioToken).ConfigureAwait(false);
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
                var lines = await File.ReadAllLinesAsync(path, ioToken).ConfigureAwait(false);
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
            var lines = await File.ReadAllLinesAsync(lastFileWithBatch, ioToken).ConfigureAwait(false);
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
            await File.WriteAllLinesAsync(lastFileWithBatch, lines, ioToken).ConfigureAwait(false);
            _logger.LogInformation("Updated NDT Pipes for bundle {BatchNo} in last file {Path}: last row set to {NewRowValue} so total = {NewPipes}.", batchNo, lastFileWithBatch, newLastRowValue, newPipes);
            return 1;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to update file {Path} for bundle {BatchNo}.", lastFileWithBatch, batchNo);
        }

        return 0;
    }

    private async Task<List<string>> ResolvePerSlitCsvPathsForBundleUpdateAsync(string batchNoTrimmed, string folder)
    {
        if (UseDatabase)
        {
            try
            {
                var fromSql = await GetPerSlitSourceFilePathsForBatchFromSqlAsync(batchNoTrimmed, folder).ConfigureAwait(false);
                if (fromSql.Count > 0)
                {
                    _logger.LogDebug(
                        "Bundle CSV update narrowed to {Count} per-slit file(s) from Output_Slit_Row for batch {BatchNo}.",
                        fromSql.Count,
                        batchNoTrimmed);
                    return fromSql;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Could not resolve per-slit source files from SQL for batch {BatchNo}; falling back to full folder scan.",
                    batchNoTrimmed);
            }
        }

        return Directory.EnumerateFiles(folder, "*.csv")
            .Where(p => !Path.GetFileName(p).StartsWith("NDT_Bundle_", StringComparison.OrdinalIgnoreCase))
            .OrderBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private async Task<List<string>> GetPerSlitSourceFilePathsForBatchFromSqlAsync(string batchNo, string folder)
    {
        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await conn.OpenAsync(CancellationToken.None).ConfigureAwait(false);

        const string sql = @"
SELECT DISTINCT Source_File
FROM dbo.Output_Slit_Row
WHERE NDT_Batch_No = @BatchNo
  AND Source_File IS NOT NULL
  AND LTRIM(RTRIM(Source_File)) <> N''";

        var paths = new List<string>();
        await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@BatchNo", batchNo);
        await using var reader = await cmd.ExecuteReaderAsync(CancellationToken.None).ConfigureAwait(false);
        while (await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false))
        {
            if (reader.IsDBNull(0))
                continue;
            var baseName = reader.GetString(0).Trim();
            if (baseName.Length == 0)
                continue;
            var path = Path.Combine(folder, baseName);
            if (File.Exists(path))
                paths.Add(path);
        }

        return paths.OrderBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase).ToList();
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
        var files = TryListPerSlitOutputCsvFiles();

        foreach (var path in files)
        {
            try
            {
                var lines = await File.ReadAllLinesAsync(path, cancellationToken).ConfigureAwait(false);
                if (lines.Length == 0)
                    continue;

                var columns = ReconcileCsvParsing.ResolveOutputCsvColumns(lines[0]);
                for (var i = 1; i < lines.Length; i++)
                {
                    if (string.IsNullOrWhiteSpace(lines[i]))
                        continue;

                    var cols = ReconcileCsvParsing.SplitCsvLine(lines[i]);
                    if (cols.Count < columns.MinColumns)
                        continue;
                    if (!columns.TryGetField(cols, columns.NdtBatchNo, out var batchCell)
                        || !batchCell.Equals(batchNoTrimmed, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    if (!columns.TryGetField(cols, columns.SlitNo, out var slitCell))
                        slitCell = string.Empty;
                    var slit = ReconcileCsvParsing.NormalizeSlitKey(slitCell);
                    if (!columns.TryGetField(cols, columns.NdtPipes, out var ndtRaw))
                        ndtRaw = "0";
                    var pipes = int.TryParse(ndtRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var p) ? p : 0;
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
            var pipes = ReadSqlInt32(reader, 1);
            list.Add((slit, pipes));
        }

        _logger.LogDebug("Loaded {Count} slit row(s) for batch {BatchNo} from Output_Slit_Row.", list.Count, batchNoTrimmed);
        return list;
    }

    public async Task<int> UpdateOutputCsvFilesForSlitAsync(string batchNo, string slitNo, int newPipes, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(batchNo))
            return 0;

        var batchNoTrimmed = batchNo.Trim();
        var slitNoNormalized = ReconcileCsvParsing.NormalizeSlitKey(slitNo);

        var files = FilterPerSlitOutputCsvFilesForReconcile(slitNoNormalized);
        if (files.Count == 0)
            return 0;

        // UNC per-slit folders can hold thousands of CSVs; reconcile must not honor HTTP request cancellation.
        var ioToken = CancellationToken.None;

        var filesUpdated = 0;
        foreach (var path in files)
        {
            try
            {
                var lines = await File.ReadAllLinesAsync(path, ioToken).ConfigureAwait(false);
                if (lines.Length == 0)
                    continue;

                var columns = ReconcileCsvParsing.ResolveOutputCsvColumns(lines[0]);
                var changed = false;
                for (var i = 1; i < lines.Length; i++)
                {
                    if (string.IsNullOrWhiteSpace(lines[i]))
                        continue;

                    var cols = ReconcileCsvParsing.SplitCsvLine(lines[i]);
                    if (cols.Count < columns.MinColumns)
                        continue;

                    if (!columns.TryGetField(cols, columns.NdtBatchNo, out var batchCell)
                        || !batchCell.Equals(batchNoTrimmed, StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    if (!columns.TryGetField(cols, columns.SlitNo, out var slitCell)
                        || !ReconcileCsvParsing.SlitKeysMatch(slitCell, slitNoNormalized))
                    {
                        continue;
                    }

                    lines[i] = ReconcileCsvParsing.ReplaceFieldAtIndex(
                        lines[i],
                        columns.NdtPipes,
                        newPipes.ToString(CultureInfo.InvariantCulture));
                    changed = true;
                }

                if (changed)
                {
                    await File.WriteAllLinesAsync(path, lines, ioToken).ConfigureAwait(false);
                    filesUpdated++;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed slit reconcile update for file {Path}.", path);
            }
        }

        return filesUpdated;
    }

    private List<string> FilterPerSlitOutputCsvFilesForReconcile(string slitNoNormalized)
    {
        var all = TryListPerSlitOutputCsvFiles();
        if (all.Count == 0 || slitNoNormalized == "—")
            return all;

        var filtered = all
            .Where(p => ReconcileCsvParsing.PerSlitOutputFileNameMatchesSlit(Path.GetFileName(p), slitNoNormalized))
            .ToList();

        if (filtered.Count > 0)
        {
            _logger.LogDebug(
                "Slit reconcile narrowed per-slit CSV scan from {Total} to {Filtered} file(s) for slit {SlitNo}.",
                all.Count,
                filtered.Count,
                slitNoNormalized);
            return filtered;
        }

        _logger.LogWarning(
            "Slit reconcile found no per-slit CSV filename match for slit {SlitNo}; scanning all {Total} file(s).",
            slitNoNormalized,
            all.Count);
        return all;
    }

    private List<string> TryListPerSlitOutputCsvFiles()
    {
        var folder = Opt.OutputBundleFolder;
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return new List<string>();

        try
        {
            return Directory.EnumerateFiles(folder, "*.csv")
                .Where(p => !Path.GetFileName(p).StartsWith("NDT_Bundle_", StringComparison.OrdinalIgnoreCase))
                .OrderBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to list per-slit output CSV files in {Folder}.", folder);
            return new List<string>();
        }
    }

    private static int ReadSqlInt32(Microsoft.Data.SqlClient.SqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
            return 0;

        return Convert.ToInt32(reader.GetValue(ordinal), CultureInfo.InvariantCulture);
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

    public async Task<int> TrySyncBundleTotalFromSlitsAsync(string batchNo, bool forceFromSlits, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(batchNo))
            return 0;

        var batchNoTrimmed = batchNo.Trim();

        if (await IsManualReconLockedAsync(batchNoTrimmed, cancellationToken).ConfigureAwait(false))
        {
            var lockedTotal = await TryGetStoredBundleTotalAsync(batchNoTrimmed, cancellationToken).ConfigureAwait(false);
            _logger.LogDebug(
                "Skipping slit-sum sync for manually reconciled bundle {BatchNo} (stored total {Total}).",
                batchNoTrimmed,
                lockedTotal);
            return lockedTotal;
        }

        var slits = await GetSlitsForBatchAsync(batchNoTrimmed, cancellationToken).ConfigureAwait(false);
        var slitSum = slits.Sum(s => s.NdtPipes);
        if (slitSum <= 0)
            return 0;

        var storedTotal = await TryGetStoredBundleTotalAsync(batchNoTrimmed, cancellationToken).ConfigureAwait(false);
        if (!forceFromSlits && storedTotal > 0)
            return storedTotal;

        if (storedTotal == slitSum)
            return slitSum;

        if (UseDatabase)
        {
            try
            {
                await UpdateBundleTotalInDatabaseAsync(batchNoTrimmed, slitSum, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Could not update NDT_Bundle total for {BatchNo} to slit sum {SlitSum}; slit reconcile data is still saved.",
                    batchNoTrimmed,
                    slitSum);
            }
        }

        await UpdateBundleSummaryCsvAsync(batchNoTrimmed, slitSum, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "Synced bundle total for {BatchNo} from slit sum: {OldTotal} → {NewTotal} (force={Force}).",
            batchNoTrimmed,
            storedTotal,
            slitSum,
            forceFromSlits);

        return slitSum;
    }

    private async Task<int> TryGetStoredBundleTotalAsync(string batchNo, CancellationToken cancellationToken)
    {
        if (UseDatabase)
        {
            try
            {
                await using var conn = SqlTraceabilityConnection.Create(Opt);
                await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
                const string sql = @"
SELECT TOP 1 Total_NDT_Pcs
FROM dbo.NDT_Bundle
WHERE Bundle_No = @BatchNo
ORDER BY PrintedAt DESC";
                await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@BatchNo", batchNo);
                var scalar = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                if (scalar is int i)
                    return i;
                if (scalar is not null && scalar != DBNull.Value && int.TryParse(scalar.ToString(), out var parsed))
                    return parsed;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Could not read stored bundle total for {BatchNo}.", batchNo);
            }
        }

        var summaryTotal = await TryReadBundleSummaryTotalAsync(batchNo, cancellationToken).ConfigureAwait(false);
        return summaryTotal ?? 0;
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
            BundleNo = reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
            PoNumber = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
            MillNo = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
            TotalNdtPcs = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
            SlitNo = reader.IsDBNull(4) ? "" : reader.GetString(4),
            SlitStartTime = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
            SlitFinishTime = reader.IsDBNull(6) ? null : reader.GetDateTime(6),
            PrintedAt = reader.IsDBNull(7) ? null : reader.GetDateTime(7),
            RejectedPipes = reader.IsDBNull(8) ? 0 : reader.GetInt32(8),
            NdtShortLengthPipe = reader.IsDBNull(9) ? "" : reader.GetString(9),
            RejectedShortLengthPipe = reader.IsDBNull(10) ? "" : reader.GetString(10),
            PrintStatus = reader.FieldCount > 11 && !reader.IsDBNull(11)
                ? reader.GetString(11)
                : BundlePrintStatus.Pending,
            PrintAttemptedAt = reader.FieldCount > 12 && !reader.IsDBNull(12)
                ? reader.GetDateTime(12)
                : null,
            PrintError = reader.FieldCount > 13 && !reader.IsDBNull(13)
                ? reader.GetString(13)
                : null,
            CloseSource = reader.FieldCount > 14 && !reader.IsDBNull(14) ? reader.GetString(14) : null,
            AwaitingCsvRecon = reader.FieldCount > 15 && !reader.IsDBNull(15) && reader.GetBoolean(15),
            CountDiscrepancy = reader.FieldCount > 16 && !reader.IsDBNull(16) && reader.GetBoolean(16),
            ManualRecon = reader.FieldCount > 17 && !reader.IsDBNull(17) && reader.GetBoolean(17),
            ManualReconBy = reader.FieldCount > 18 && !reader.IsDBNull(18) ? reader.GetString(18) : null,
            ManualReconAt = reader.FieldCount > 19 && !reader.IsDBNull(19) ? reader.GetDateTime(19) : null,
            ManualReconReason = reader.FieldCount > 20 && !reader.IsDBNull(20) ? reader.GetString(20) : null,
            ManualReconOriginalTotal = reader.FieldCount > 21 && !reader.IsDBNull(21) ? reader.GetInt32(21) : null,
            PostReconCsvSum = reader.FieldCount > 22 && !reader.IsDBNull(22) ? reader.GetInt32(22) : null
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
