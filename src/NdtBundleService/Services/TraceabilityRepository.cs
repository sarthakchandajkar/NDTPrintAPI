using System.Globalization;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

public interface ITraceabilityRepository
{
    Task RecordInputSlitRowsAsync(string sourceFile, IReadOnlyList<(InputSlitRecord Record, int SourceRowNumber)> rows, CancellationToken cancellationToken);
    Task RecordOutputSlitRowsAsync(string sourceFile, IReadOnlyList<(InputSlitRecord Record, string NdtBatchNo, int SourceRowNumber)> rows, CancellationToken cancellationToken);
    Task RecordManualStationRunAsync(
        string poNumber,
        string ndtBatchNo,
        int ndtPcs,
        int okPcs,
        int rejectPcs,
        string workStation,
        DateTime start,
        DateTime end,
        string? hydrotestingType,
        string sourceFile,
        CancellationToken cancellationToken);

    /// <summary>One row per completed NDT process CSV (after Revisual), matching the consolidated export columns.</summary>
    Task RecordNdtProcessConsolidatedAsync(
        string poNumber,
        string ndtBatchNo,
        int ndtPcs,
        int okPcs,
        int visualReject,
        int hydrotestReject,
        int revisualReject,
        DateTime bundleStart,
        DateTime bundleEnd,
        string outputFilePath,
        CancellationToken cancellationToken);

    /// <summary>Upserts label fields for (PO, Mill) when a bundle tag is formed or printed.</summary>
    Task RecordBundleLabelAsync(
        string poNumber,
        int millNo,
        string? specification,
        string? type,
        string? pipeSize,
        string? length,
        CancellationToken cancellationToken);

    Task RecordUploadBundleRowsAsync(string generatedFile, IReadOnlyList<UploadBundleRow> rows, CancellationToken cancellationToken);

    /// <summary>
    /// Deletes Output_Slit_Row rows that correspond to removed per-slit output CSV lines (same source file basename and row number as when the worker imported the file). Input_Slit_Row is not modified.
    /// </summary>
    Task DeleteOutputSlitRowsForRemovedOutputLinesAsync(
        string ndtBatchNo,
        IReadOnlyList<RemovedSlitRowTraceRef> refs,
        CancellationToken cancellationToken);
}

public sealed class UploadBundleRow
{
    public string PoNo { get; init; } = string.Empty;
    public string SlitNo { get; init; } = string.Empty;
    public string HrcNumber { get; init; } = string.Empty;
    public string SlitWidth { get; init; } = string.Empty;
    public string SlitThick { get; init; } = string.Empty;
    public string Nss { get; init; } = string.Empty;
    public string SlitGrade { get; init; } = string.Empty;
    public string BundleNumber { get; init; } = string.Empty;
    public int NumOfPipes { get; init; }
    public string TotalBundleWt { get; init; } = string.Empty;
    public string LenPerPipe { get; init; } = string.Empty;
    public bool? IsFullBundle { get; init; }
}

public sealed class TraceabilityRepository : ITraceabilityRepository
{
    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly ILogger<TraceabilityRepository> _logger;

    public TraceabilityRepository(IOptionsMonitor<NdtBundleOptions> optionsMonitor, ILogger<TraceabilityRepository> logger)
    {
        _optionsMonitor = optionsMonitor;
        _logger = logger;
    }

    private NdtBundleOptions Opt => _optionsMonitor.CurrentValue;

    private bool Enabled =>
        Opt.UseSqlServerForBundles && !string.IsNullOrWhiteSpace(Opt.ConnectionString);

    private SqlConnection CreateConnection() => SqlTraceabilityConnection.Create(Opt.ConnectionString);

    private Task OpenConnectionAsync(SqlConnection connection, string operation, CancellationToken cancellationToken) =>
        SqlTraceabilityConnection.OpenAsync(connection, _logger, operation, cancellationToken);

    public async Task RecordInputSlitRowsAsync(
        string sourceFile,
        IReadOnlyList<(InputSlitRecord Record, int SourceRowNumber)> rows,
        CancellationToken cancellationToken)
    {
        if (!Enabled || rows.Count == 0)
            return;

        try
        {
            await using var conn = CreateConnection();
            await OpenConnectionAsync(conn, "Input_Slit_Row insert", cancellationToken).ConfigureAwait(false);

            const string sql = @"
INSERT INTO dbo.Input_Slit_Row
    (PO_Number, Slit_No, NDT_Pipes, Rejected_P, Slit_Start_Time, Slit_Finish_Time, Mill_No, NDT_Short_Length_Pipe, Rejected_Short_Length_Pipe, Source_File, Source_Row_Number)
VALUES
    (@PoNumber, @SlitNo, @NdtPipes, @RejectedP, @StartTime, @FinishTime, @MillNo, @NdtShort, @RejShort, @SourceFile, @SourceRowNumber);";

            foreach (var (r, rowNo) in rows)
            {
                await using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@PoNumber", r.PoNumber);
                cmd.Parameters.AddWithValue("@SlitNo", (object?)NullIfEmpty(r.SlitNo) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@NdtPipes", r.NdtPipes);
                cmd.Parameters.AddWithValue("@RejectedP", r.RejectedPipes);
                cmd.Parameters.AddWithValue("@StartTime", (object?)r.SlitStartTime ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@FinishTime", (object?)r.SlitFinishTime ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@MillNo", r.MillNo == 0 ? (object)DBNull.Value : r.MillNo);
                cmd.Parameters.AddWithValue("@NdtShort", (object?)NullIfEmpty(r.NdtShortLengthPipe) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@RejShort", (object?)NullIfEmpty(r.RejectedShortLengthPipe) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@SourceFile", (object?)NullIfEmpty(sourceFile) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@SourceRowNumber", rowNo);
                await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            _logger.LogInformation(
                "Recorded {Count} Input_Slit_Row row(s) for file {File} in JazeeraMES_Prod.",
                rows.Count,
                sourceFile);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to record Input_Slit_Row for file {File} in JazeeraMES_Prod.", sourceFile);
        }
    }

    public async Task RecordOutputSlitRowsAsync(
        string sourceFile,
        IReadOnlyList<(InputSlitRecord Record, string NdtBatchNo, int SourceRowNumber)> rows,
        CancellationToken cancellationToken)
    {
        if (!Enabled || rows.Count == 0)
            return;

        try
        {
            await using var conn = CreateConnection();
            await OpenConnectionAsync(conn, "Output_Slit_Row insert", cancellationToken).ConfigureAwait(false);

            const string sql = @"
INSERT INTO dbo.Output_Slit_Row
    (PO_Number, Slit_No, NDT_Pipes, Rejected_P, Slit_Start_Time, Slit_Finish_Time, Mill_No, NDT_Short_Length_Pipe, Rejected_Short_Length_Pipe, NDT_Batch_No, Source_File, Source_Row_Number)
VALUES
    (@PoNumber, @SlitNo, @NdtPipes, @RejectedP, @StartTime, @FinishTime, @MillNo, @NdtShort, @RejShort, @BatchNo, @SourceFile, @SourceRowNumber);";

            foreach (var (r, batchNo, rowNo) in rows)
            {
                // If Output_Slit_Row has an FK to NDT_Bundle(Bundle_No), create a stub bundle row early.
                await EnsureBundleRowExistsAsync(conn, r, batchNo, cancellationToken).ConfigureAwait(false);

                await using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@PoNumber", r.PoNumber);
                cmd.Parameters.AddWithValue("@SlitNo", (object?)NullIfEmpty(r.SlitNo) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@NdtPipes", r.NdtPipes);
                cmd.Parameters.AddWithValue("@RejectedP", r.RejectedPipes);
                cmd.Parameters.AddWithValue("@StartTime", (object?)r.SlitStartTime ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@FinishTime", (object?)r.SlitFinishTime ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@MillNo", r.MillNo == 0 ? (object)DBNull.Value : r.MillNo);
                cmd.Parameters.AddWithValue("@NdtShort", (object?)NullIfEmpty(r.NdtShortLengthPipe) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@RejShort", (object?)NullIfEmpty(r.RejectedShortLengthPipe) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@BatchNo", batchNo);
                cmd.Parameters.AddWithValue("@SourceFile", (object?)NullIfEmpty(sourceFile) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@SourceRowNumber", rowNo);
                await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            _logger.LogInformation(
                "Recorded {Count} Output_Slit_Row row(s) for file {File} in JazeeraMES_Prod.",
                rows.Count,
                sourceFile);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to record Output_Slit_Row for file {File} in JazeeraMES_Prod.", sourceFile);
        }
    }

    public async Task DeleteOutputSlitRowsForRemovedOutputLinesAsync(
        string ndtBatchNo,
        IReadOnlyList<RemovedSlitRowTraceRef> refs,
        CancellationToken cancellationToken)
    {
        if (!Enabled || refs.Count == 0)
            return;

        try
        {
            await using var conn = CreateConnection();
            await OpenConnectionAsync(conn, "Output_Slit_Row delete", cancellationToken).ConfigureAwait(false);

            const string delOutput = @"
DELETE FROM dbo.Output_Slit_Row
WHERE NDT_Batch_No = @BatchNo
  AND PO_Number = @PoNumber
  AND Source_Row_Number = @RowNo
  AND (Source_File LIKE @LikeWin OR Source_File LIKE @LikeUnix);";

            foreach (var r in refs)
            {
                var esc = SqlLikeEscape(r.FileBaseName);
                var likeWin = "%\\" + esc;
                var likeUnix = "%/" + esc;

                await using var cmd = new SqlCommand(delOutput, conn);
                cmd.Parameters.AddWithValue("@BatchNo", ndtBatchNo.Trim());
                cmd.Parameters.AddWithValue("@PoNumber", r.PoNumber);
                cmd.Parameters.AddWithValue("@RowNo", r.SourceRowNumber1Based);
                cmd.Parameters.AddWithValue("@LikeWin", likeWin);
                cmd.Parameters.AddWithValue("@LikeUnix", likeUnix);
                await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete Output_Slit_Row after slit removal for batch {BatchNo}.", ndtBatchNo);
        }
    }

    private static string SqlLikeEscape(string literal) =>
        literal.Replace("[", "[[]", StringComparison.Ordinal).Replace("%", "[%]", StringComparison.Ordinal).Replace("_", "[_]", StringComparison.Ordinal);

    public async Task RecordManualStationRunAsync(
        string poNumber,
        string ndtBatchNo,
        int ndtPcs,
        int okPcs,
        int rejectPcs,
        string workStation,
        DateTime start,
        DateTime end,
        string? hydrotestingType,
        string sourceFile,
        CancellationToken cancellationToken)
    {
        if (!Enabled)
            return;

        try
        {
            await using var conn = CreateConnection();
            await OpenConnectionAsync(conn, "Manual_Station_Run insert", cancellationToken).ConfigureAwait(false);

            const string sql = @"
INSERT INTO dbo.Manual_Station_Run
    (PO_Number, NDT_Batch_No, NDT_Pcs, OK_Pcs, Reject_Pcs, Work_Station, Bundle_Start, Bundle_End, Hydrotesting_Type, Source_File)
VALUES
    (@PoNumber, @BatchNo, @NdtPcs, @Ok, @Reject, @WorkStation, @Start, @End, @HydroType, @SourceFile);";

            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@PoNumber", poNumber);
            cmd.Parameters.AddWithValue("@BatchNo", ndtBatchNo);
            cmd.Parameters.AddWithValue("@NdtPcs", ndtPcs);
            cmd.Parameters.AddWithValue("@Ok", okPcs);
            cmd.Parameters.AddWithValue("@Reject", rejectPcs);
            cmd.Parameters.AddWithValue("@WorkStation", (object?)NullIfEmpty(workStation) ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Start", start);
            cmd.Parameters.AddWithValue("@End", end);
            cmd.Parameters.AddWithValue("@HydroType", (object?)NullIfEmpty(hydrotestingType ?? string.Empty) ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@SourceFile", (object?)NullIfEmpty(sourceFile) ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            _logger.LogInformation(
                "Recorded Manual_Station_Run for batch {BatchNo} station {Station} in JazeeraMES_Prod.",
                ndtBatchNo,
                workStation);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to record Manual_Station_Run for batch {BatchNo} in JazeeraMES_Prod.", ndtBatchNo);
        }
    }

    public async Task RecordNdtProcessConsolidatedAsync(
        string poNumber,
        string ndtBatchNo,
        int ndtPcs,
        int okPcs,
        int visualReject,
        int hydrotestReject,
        int revisualReject,
        DateTime bundleStart,
        DateTime bundleEnd,
        string outputFilePath,
        CancellationToken cancellationToken)
    {
        if (!Enabled)
            return;

        try
        {
            await using var conn = CreateConnection();
            await OpenConnectionAsync(conn, "NDT_Process_Consolidated upsert", cancellationToken).ConfigureAwait(false);

            const string sql = @"
IF EXISTS (SELECT 1 FROM dbo.NDT_Process_Consolidated WHERE NDT_Batch_No = @BatchNo)
BEGIN
    UPDATE dbo.NDT_Process_Consolidated
    SET PO_Number = @PoNumber,
        NDT_Pcs = @NdtPcs,
        OK_Pcs = @Ok,
        Visual_Reject = @VisualRej,
        Hydrotest_Reject = @HydroRej,
        Revisual_Reject = @RevisualRej,
        Bundle_Start = @BundleStart,
        Bundle_End = @BundleEnd,
        Output_File = @OutputFile
    WHERE NDT_Batch_No = @BatchNo;
END
ELSE
BEGIN
    INSERT INTO dbo.NDT_Process_Consolidated
        (PO_Number, NDT_Batch_No, NDT_Pcs, OK_Pcs, Visual_Reject, Hydrotest_Reject, Revisual_Reject, Bundle_Start, Bundle_End, Output_File)
    VALUES
        (@PoNumber, @BatchNo, @NdtPcs, @Ok, @VisualRej, @HydroRej, @RevisualRej, @BundleStart, @BundleEnd, @OutputFile);
END";

            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@PoNumber", poNumber);
            cmd.Parameters.AddWithValue("@BatchNo", ndtBatchNo);
            cmd.Parameters.AddWithValue("@NdtPcs", ndtPcs);
            cmd.Parameters.AddWithValue("@Ok", okPcs);
            cmd.Parameters.AddWithValue("@VisualRej", visualReject);
            cmd.Parameters.AddWithValue("@HydroRej", hydrotestReject);
            cmd.Parameters.AddWithValue("@RevisualRej", revisualReject);
            cmd.Parameters.AddWithValue("@BundleStart", bundleStart);
            cmd.Parameters.AddWithValue("@BundleEnd", bundleEnd);
            cmd.Parameters.AddWithValue("@OutputFile", (object?)NullIfEmpty(outputFilePath) ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            _logger.LogInformation(
                "Recorded NDT_Process_Consolidated for batch {BatchNo} in JazeeraMES_Prod (output {OutputFile}).",
                ndtBatchNo,
                outputFilePath);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to record NDT_Process_Consolidated for batch {BatchNo} in JazeeraMES_Prod. Revisual CSV was still written; fix SQL connectivity.",
                ndtBatchNo);
        }
    }

    public async Task RecordBundleLabelAsync(
        string poNumber,
        int millNo,
        string? specification,
        string? type,
        string? pipeSize,
        string? length,
        CancellationToken cancellationToken)
    {
        if (!Enabled || millNo is < 1 or > 4)
            return;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po))
            return;

        try
        {
            await using var conn = CreateConnection();
            await OpenConnectionAsync(conn, "Bundle_Label upsert", cancellationToken).ConfigureAwait(false);

            const string sql = @"
IF EXISTS (SELECT 1 FROM dbo.Bundle_Label WHERE PO_Number = @PoNumber AND Mill_No = @MillNo)
BEGIN
    UPDATE dbo.Bundle_Label
    SET Specification = @Specification,
        Type = @Type,
        Pipe_Size = @PipeSize,
        Length = @Length
    WHERE PO_Number = @PoNumber AND Mill_No = @MillNo;
END
ELSE
BEGIN
    INSERT INTO dbo.Bundle_Label (PO_Number, Mill_No, Specification, Type, Pipe_Size, Length)
    VALUES (@PoNumber, @MillNo, @Specification, @Type, @PipeSize, @Length);
END";

            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@PoNumber", po);
            cmd.Parameters.AddWithValue("@MillNo", millNo);
            cmd.Parameters.AddWithValue("@Specification", (object?)NullIfEmpty(specification) ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Type", (object?)NullIfEmpty(type) ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@PipeSize", (object?)NullIfEmpty(pipeSize) ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Length", (object?)NullIfEmpty(length) ?? DBNull.Value);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("Recorded Bundle_Label for PO {PoNumber} mill {MillNo} in JazeeraMES_Prod.", po, millNo);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to record Bundle_Label for PO {PoNumber} mill {MillNo} in JazeeraMES_Prod.", po, millNo);
        }
    }

    public async Task RecordUploadBundleRowsAsync(string generatedFile, IReadOnlyList<UploadBundleRow> rows, CancellationToken cancellationToken)
    {
        if (!Enabled || rows.Count == 0)
            return;

        try
        {
            await using var conn = CreateConnection();
            await OpenConnectionAsync(conn, "Upload_Bundle_Row insert", cancellationToken).ConfigureAwait(false);

            const string sql = @"
INSERT INTO dbo.Upload_Bundle_Row
    (PO_NO, Slit_No, HRC_Number, Slit_Width, Slit_Thick, NSS, Slit_Grade, Bundle_Number, NumOfPipes, TotalBundleWt, LenPerPipe, IsFullBundle, Source_File)
VALUES
    (@PoNo, @SlitNo, @Hrc, @Width, @Thick, @Nss, @Grade, @BundleNo, @NumPipes, @Wt, @Len, @IsFull, @SourceFile);";

            foreach (var r in rows)
            {
                await using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@PoNo", r.PoNo);
                cmd.Parameters.AddWithValue("@SlitNo", (object?)NullIfEmpty(r.SlitNo) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@Hrc", (object?)NullIfEmpty(r.HrcNumber) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@Width", (object?)NullIfEmpty(r.SlitWidth) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@Thick", (object?)NullIfEmpty(r.SlitThick) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@Nss", (object?)NullIfEmpty(r.Nss) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@Grade", (object?)NullIfEmpty(r.SlitGrade) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@BundleNo", r.BundleNumber);
                cmd.Parameters.AddWithValue("@NumPipes", r.NumOfPipes);
                cmd.Parameters.AddWithValue("@Wt", (object?)NullIfEmpty(r.TotalBundleWt) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@Len", (object?)NullIfEmpty(r.LenPerPipe) ?? DBNull.Value);
                cmd.Parameters.AddWithValue("@IsFull", r.IsFullBundle.HasValue ? (object)r.IsFullBundle.Value : DBNull.Value);
                cmd.Parameters.AddWithValue("@SourceFile", (object?)NullIfEmpty(generatedFile) ?? DBNull.Value);
                await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to record Upload_Bundle_Row for file {File}.", generatedFile);
        }
    }

    private static string? NullIfEmpty(string? s) =>
        string.IsNullOrWhiteSpace(s) ? null : s.Trim();

    private static async Task EnsureBundleRowExistsAsync(
        SqlConnection conn,
        InputSlitRecord record,
        string batchNo,
        CancellationToken cancellationToken)
    {
        const string sql = @"
IF NOT EXISTS (SELECT 1 FROM dbo.NDT_Bundle WHERE Bundle_No = @BundleNo)
BEGIN
    INSERT INTO dbo.NDT_Bundle
        (PO_Number, Mill_No, Bundle_No, Total_NDT_Pcs, Context_Slit_No, Slit_Start_Time, Slit_Finish_Time, Rejected_P, NDT_Short_Length_Pipe, Rejected_Short_Length_Pipe, IsReprint)
    VALUES
        (@PoNumber, @MillNo, @BundleNo, 0, @SlitNo, @SlitStartTime, @SlitFinishTime, @RejectedPipes, @NdtShortLengthPipe, @RejectedShortLengthPipe, 0);
END";

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@PoNumber", record.PoNumber);
        cmd.Parameters.AddWithValue("@MillNo", record.MillNo);
        cmd.Parameters.AddWithValue("@BundleNo", batchNo);
        cmd.Parameters.AddWithValue("@SlitNo", (object?)NullIfEmpty(record.SlitNo) ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@SlitStartTime", (object?)record.SlitStartTime ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@SlitFinishTime", (object?)record.SlitFinishTime ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@RejectedPipes", record.RejectedPipes);
        cmd.Parameters.AddWithValue("@NdtShortLengthPipe", (object?)NullIfEmpty(record.NdtShortLengthPipe) ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@RejectedShortLengthPipe", (object?)NullIfEmpty(record.RejectedShortLengthPipe) ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }
}

