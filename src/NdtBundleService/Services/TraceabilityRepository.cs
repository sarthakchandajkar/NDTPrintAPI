using System.Globalization;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Outcome of an <c>Output_Slit_Row</c> batch renumber. When <see cref="Succeeded"/> is false the
/// rows in SQL still reference the old batch number and reconcile/SAP totals will be wrong until remediated.
/// </summary>
public sealed record OutputSlitBatchCorrectionResult(
    bool Succeeded,
    int RowsUpdated,
    IReadOnlyList<string> SourceFiles)
{
    public static OutputSlitBatchCorrectionResult NoOp { get; } =
        new(true, 0, Array.Empty<string>());
}

public interface ITraceabilityRepository
{
    Task RecordInputSlitRowsAsync(
        string sourceFile,
        IReadOnlyList<(InputSlitRecord Record, int SourceRowNumber)> rows,
        CancellationToken cancellationToken,
        DateTime? sourceLastWriteTimeUtc = null);

    /// <summary>
    /// True when <c>Input_Slit_Row</c> already has this source path at a version that covers
    /// <paramref name="fileLastWriteTimeUtc"/> (NULL stored write = legacy, any version).
    /// </summary>
    Task<bool> IsInputSlitFileVersionImportedAsync(
        string sourceFileFullPath,
        DateTime fileLastWriteTimeUtc,
        CancellationToken cancellationToken);

    /// <summary>
    /// True when <c>Input_Slit_File_Seen</c> has this path+write (terminal skip, e.g. no configured-mill rows).
    /// </summary>
    Task<bool> IsInputSlitFileSeenAsync(
        string sourceFileFullPath,
        DateTime fileLastWriteTimeUtc,
        CancellationToken cancellationToken);

    /// <summary>Upserts a durable seen marker so reconcile will not re-queue the file version.</summary>
    Task MarkInputSlitFileSeenAsync(
        string sourceFileFullPath,
        DateTime fileLastWriteTimeUtc,
        string reason,
        CancellationToken cancellationToken);

    /// <summary>
    /// Updates <c>Output_Slit_Row.NDT_Batch_No</c> from provisional to final for the PO/mill.
    /// Ensures the target <c>NDT_Bundle</c> parent row exists first (FK safety). The result reports
    /// whether the SQL rename actually succeeded — callers must not claim correction on failure.
    /// </summary>
    Task<OutputSlitBatchCorrectionResult> UpdateOutputSlitBatchNoAsync(
        string poNumber,
        int millNo,
        string oldBatchNo,
        string newBatchNo,
        CancellationToken cancellationToken);

    /// <summary>
    /// Batch number already recorded in <c>Output_Slit_Row</c> for this source file basename
    /// (PO/mill guarded), or <c>null</c> when the file has no rows yet. Re-processed files must
    /// keep their original bundle instead of re-routing to the newest printed one.
    /// </summary>
    Task<string?> TryGetExistingOutputSlitBatchAsync(
        string sourceFileFullPath,
        string poNumber,
        int millNo,
        CancellationToken cancellationToken);

    /// <summary>
    /// Source files whose <c>Output_Slit_Row</c> rows must not be renumbered — SAP Accepted on disk
    /// or in <c>Output_Slit_Sap_Status</c>.
    /// </summary>
    Task<IReadOnlyList<string>> GetSapFrozenSourceFilesAsync(
        IReadOnlyList<string> sourceFiles,
        CancellationToken cancellationToken);

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

    /// <summary>Updates the latest Manual_Station_Run row for (batch, work station), or inserts when none exists.</summary>
    Task UpsertManualStationRunAsync(
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

    /// <summary>Updates NDT_Pipes on all Output_Slit_Row rows for the batch and slit.</summary>
    Task<int> UpdateOutputSlitRowNdtPipesByBatchAndSlitAsync(
        string ndtBatchNo,
        string slitNo,
        int ndtPipes,
        CancellationToken cancellationToken);

    /// <summary>Aligns Output_Slit_Row.NDT_Pipes with per-slit output CSV rows for the batch (by source file + row number).</summary>
    Task SyncOutputSlitRowsFromPerSlitCsvForBatchAsync(string ndtBatchNo, CancellationToken cancellationToken);

    /// <summary>Updates reject counts and OK on NDT_Process_Consolidated when present (partial update after Visual/Hydro reconcile).</summary>
    Task UpdateNdtProcessConsolidatedFromStationsAsync(
        string poNumber,
        string ndtBatchNo,
        int ndtPcs,
        int okPcs,
        int visualReject,
        int hydrotestReject,
        int revisualReject,
        DateTime? bundleStart,
        DateTime? bundleEnd,
        string? outputFilePath,
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
    private readonly ISqlTraceabilityWriteTracker _writeTracker;
    private readonly ILogger<TraceabilityRepository> _logger;

    public TraceabilityRepository(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        ISqlTraceabilityWriteTracker writeTracker,
        ILogger<TraceabilityRepository> logger)
    {
        _optionsMonitor = optionsMonitor;
        _writeTracker = writeTracker;
        _logger = logger;
    }

    private NdtBundleOptions Opt => _optionsMonitor.CurrentValue;

    private bool Enabled => SqlTraceabilityConnection.IsSqlEnabled(Opt);

    private Task OpenConnectionAsync(SqlConnection connection, string operation, CancellationToken cancellationToken) =>
        SqlTraceabilityConnection.OpenAsync(connection, _logger, operation, cancellationToken);

    public async Task RecordInputSlitRowsAsync(
        string sourceFile,
        IReadOnlyList<(InputSlitRecord Record, int SourceRowNumber)> rows,
        CancellationToken cancellationToken,
        DateTime? sourceLastWriteTimeUtc = null)
    {
        if (!Enabled || rows.Count == 0)
        {
            if (rows.Count > 0 && !Enabled)
                _logger.LogWarning(
                    "SQL disabled: {Count} Input_Slit_Row row(s) from {File} were not saved to JazeeraMES_Prod.",
                    rows.Count,
                    sourceFile);
            return;
        }

        var inserted = 0;
        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await OpenConnectionAsync(conn, "Input_Slit_Row insert", cancellationToken).ConfigureAwait(false);

            // Prefer insert with Source_LastWriteTimeUtc; fall back if column not yet migrated.
            const string sqlWithLw = @"
INSERT INTO dbo.Input_Slit_Row
    (PO_Number, Slit_No, NDT_Pipes, Rejected_P, Slit_Start_Time, Slit_Finish_Time, Mill_No, NDT_Short_Length_Pipe, Rejected_Short_Length_Pipe, Source_File, Source_Row_Number, Source_LastWriteTimeUtc)
VALUES
    (@PoNumber, @SlitNo, @NdtPipes, @RejectedP, @StartTime, @FinishTime, @MillNo, @NdtShort, @RejShort, @SourceFile, @SourceRowNumber, @SourceLastWrite);";

            const string sqlLegacy = @"
INSERT INTO dbo.Input_Slit_Row
    (PO_Number, Slit_No, NDT_Pipes, Rejected_P, Slit_Start_Time, Slit_Finish_Time, Mill_No, NDT_Short_Length_Pipe, Rejected_Short_Length_Pipe, Source_File, Source_Row_Number)
VALUES
    (@PoNumber, @SlitNo, @NdtPipes, @RejectedP, @StartTime, @FinishTime, @MillNo, @NdtShort, @RejShort, @SourceFile, @SourceRowNumber);";

            var useLegacyInsert = false;
            foreach (var (r, rowNo) in rows)
            {
                if (string.IsNullOrWhiteSpace(r.PoNumber))
                    continue;

                try
                {
                    await using var cmd = new SqlCommand(useLegacyInsert ? sqlLegacy : sqlWithLw, conn);
                    cmd.Parameters.AddWithValue("@PoNumber", InputSlitCsvParsing.NormalizePo(r.PoNumber));
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
                    if (!useLegacyInsert)
                    {
                        cmd.Parameters.AddWithValue(
                            "@SourceLastWrite",
                            sourceLastWriteTimeUtc.HasValue ? sourceLastWriteTimeUtc.Value : DBNull.Value);
                    }

                    await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    inserted++;
                }
                catch (SqlException ex) when (!useLegacyInsert && IsMissingSourceLastWriteColumn(ex))
                {
                    _logger.LogWarning(
                        "Input_Slit_Row.Source_LastWriteTimeUtc missing — falling back to legacy insert. Run docs/Input_Slit_Row_Alter_SourceLastWrite.sql.");
                    useLegacyInsert = true;
                    // Retry this row with legacy SQL
                    await using var cmd = new SqlCommand(sqlLegacy, conn);
                    cmd.Parameters.AddWithValue("@PoNumber", InputSlitCsvParsing.NormalizePo(r.PoNumber));
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
                    inserted++;
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Failed Input_Slit_Row insert for PO {Po} row {RowNo} file {File}.",
                        r.PoNumber,
                        rowNo,
                        sourceFile);
                }
            }

            if (inserted > 0)
            {
                _writeTracker.RecordSuccess("Input_Slit_Row", $"{inserted} row(s) from {Path.GetFileName(sourceFile)}");
                _logger.LogInformation(
                    "Recorded {Count} Input_Slit_Row row(s) for file {File} in JazeeraMES_Prod.",
                    inserted,
                    sourceFile);
            }
            else
            {
                _writeTracker.RecordFailure("Input_Slit_Row", "No rows inserted.", sourceFile);
            }
        }
        catch (Exception ex)
        {
            _writeTracker.RecordFailure("Input_Slit_Row", ex.Message, sourceFile);
            _logger.LogError(ex, "Failed to record Input_Slit_Row for file {File} in JazeeraMES_Prod.", sourceFile);
        }
    }

    public async Task<bool> IsInputSlitFileVersionImportedAsync(
        string sourceFileFullPath,
        DateTime fileLastWriteTimeUtc,
        CancellationToken cancellationToken)
    {
        if (!Enabled || string.IsNullOrWhiteSpace(sourceFileFullPath))
            return false;

        var full = Path.GetFullPath(sourceFileFullPath);
        var baseName = Path.GetFileName(full);
        if (string.IsNullOrEmpty(baseName))
            return false;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await OpenConnectionAsync(conn, "Input_Slit_Row version check", cancellationToken).ConfigureAwait(false);

            var esc = SqlLikeEscape(baseName);
            var likeWin = "%\\" + esc;
            var likeUnix = "%/" + esc;

            // Prefer version-aware check; fall back to path-only when column missing.
            const string sqlVersioned = @"
SELECT CASE
         WHEN EXISTS (
             SELECT 1
             FROM dbo.Input_Slit_Row
             WHERE (Source_File = @FullPath OR Source_File LIKE @LikeWin OR Source_File LIKE @LikeUnix)
               AND (Source_LastWriteTimeUtc IS NULL OR Source_LastWriteTimeUtc >= @FileLw)
         ) THEN 1 ELSE 0
       END;";

            try
            {
                await using var cmd = new SqlCommand(sqlVersioned, conn);
                cmd.Parameters.AddWithValue("@FullPath", full);
                cmd.Parameters.AddWithValue("@LikeWin", likeWin);
                cmd.Parameters.AddWithValue("@LikeUnix", likeUnix);
                cmd.Parameters.AddWithValue("@FileLw", fileLastWriteTimeUtc);
                var scalar = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return scalar is int i && i == 1
                       || scalar is long l && l == 1
                       || (scalar is not null && Convert.ToInt32(scalar, CultureInfo.InvariantCulture) == 1);
            }
            catch (SqlException ex) when (IsMissingSourceLastWriteColumn(ex))
            {
                const string sqlPathOnly = @"
SELECT TOP 1 1
FROM dbo.Input_Slit_Row
WHERE Source_File = @FullPath
   OR Source_File LIKE @LikeWin
   OR Source_File LIKE @LikeUnix;";
                await using var cmd = new SqlCommand(sqlPathOnly, conn);
                cmd.Parameters.AddWithValue("@FullPath", full);
                cmd.Parameters.AddWithValue("@LikeWin", likeWin);
                cmd.Parameters.AddWithValue("@LikeUnix", likeUnix);
                var scalar = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                return scalar is not null;
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Input_Slit_Row version check failed for {File}; treating as not imported.",
                sourceFileFullPath);
            return false;
        }
    }

    public async Task<bool> IsInputSlitFileSeenAsync(
        string sourceFileFullPath,
        DateTime fileLastWriteTimeUtc,
        CancellationToken cancellationToken)
    {
        if (!Enabled || string.IsNullOrWhiteSpace(sourceFileFullPath))
            return false;

        var full = Path.GetFullPath(sourceFileFullPath);
        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await OpenConnectionAsync(conn, "Input_Slit_File_Seen check", cancellationToken).ConfigureAwait(false);
            const string sql = @"
SELECT TOP 1 1
FROM dbo.Input_Slit_File_Seen
WHERE Source_File = @FullPath
  AND Source_LastWriteTimeUtc = @FileLw;";
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@FullPath", full);
            cmd.Parameters.AddWithValue("@FileLw", fileLastWriteTimeUtc);
            var scalar = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return scalar is not null;
        }
        catch (SqlException ex) when (IsMissingInputSlitFileSeenTable(ex))
        {
            _logger.LogDebug(
                "Input_Slit_File_Seen missing (run docs/Input_Slit_File_Seen_AddTable.sql); treating {File} as not seen.",
                sourceFileFullPath);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Input_Slit_File_Seen check failed for {File}.", sourceFileFullPath);
            return false;
        }
    }

    public async Task MarkInputSlitFileSeenAsync(
        string sourceFileFullPath,
        DateTime fileLastWriteTimeUtc,
        string reason,
        CancellationToken cancellationToken)
    {
        if (!Enabled || string.IsNullOrWhiteSpace(sourceFileFullPath))
            return;

        var full = Path.GetFullPath(sourceFileFullPath);
        var reasonTrim = string.IsNullOrWhiteSpace(reason) ? "Unknown" : reason.Trim();
        if (reasonTrim.Length > 64)
            reasonTrim = reasonTrim[..64];

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await OpenConnectionAsync(conn, "Input_Slit_File_Seen upsert", cancellationToken).ConfigureAwait(false);
            const string sql = @"
IF NOT EXISTS (
    SELECT 1 FROM dbo.Input_Slit_File_Seen
    WHERE Source_File = @FullPath AND Source_LastWriteTimeUtc = @FileLw)
BEGIN
    INSERT INTO dbo.Input_Slit_File_Seen (Source_File, Source_LastWriteTimeUtc, Reason)
    VALUES (@FullPath, @FileLw, @Reason);
END";
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@FullPath", full);
            cmd.Parameters.AddWithValue("@FileLw", fileLastWriteTimeUtc);
            cmd.Parameters.AddWithValue("@Reason", reasonTrim);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            _writeTracker.RecordSuccess("Input_Slit_File_Seen", $"{Path.GetFileName(full)} ({reasonTrim})");
        }
        catch (SqlException ex) when (IsMissingInputSlitFileSeenTable(ex))
        {
            _logger.LogWarning(
                "Input_Slit_File_Seen missing — run docs/Input_Slit_File_Seen_AddTable.sql. Could not mark {File} seen ({Reason}).",
                sourceFileFullPath,
                reasonTrim);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to mark Input_Slit_File_Seen for {File}.", sourceFileFullPath);
        }
    }

    public async Task<OutputSlitBatchCorrectionResult> UpdateOutputSlitBatchNoAsync(
        string poNumber,
        int millNo,
        string oldBatchNo,
        string newBatchNo,
        CancellationToken cancellationToken)
    {
        if (!Enabled
            || string.IsNullOrWhiteSpace(oldBatchNo)
            || string.IsNullOrWhiteSpace(newBatchNo)
            || string.Equals(oldBatchNo, newBatchNo, StringComparison.OrdinalIgnoreCase))
        {
            return OutputSlitBatchCorrectionResult.NoOp;
        }

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        var files = new List<string>();
        const int maxAttempts = 3;
        Exception? lastError = null;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            try
            {
                await using var conn = SqlTraceabilityConnection.Create(Opt);
                await OpenConnectionAsync(conn, "Output_Slit_Row batch correct", cancellationToken).ConfigureAwait(false);

                files.Clear();
                const string selectSql = @"
SELECT DISTINCT Source_File
FROM dbo.Output_Slit_Row
WHERE NDT_Batch_No = @OldBatch
  AND Mill_No = @Mill
  AND PO_Number = @Po;";
                await using (var sel = new SqlCommand(selectSql, conn))
                {
                    sel.Parameters.AddWithValue("@OldBatch", oldBatchNo.Trim());
                    sel.Parameters.AddWithValue("@Mill", millNo);
                    sel.Parameters.AddWithValue("@Po", po);
                    await using var reader = await sel.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                    while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        if (!reader.IsDBNull(0))
                            files.Add(reader.GetString(0));
                    }
                }

                if (files.Count == 0)
                    return new OutputSlitBatchCorrectionResult(true, 0, Array.Empty<string>());

                var frozenFiles = await GetSapFrozenSourceFilesAsync(conn, files, cancellationToken).ConfigureAwait(false);
                await AugmentSapFrozenFromBatchRowsAsync(
                        conn,
                        oldBatchNo.Trim(),
                        millNo,
                        po,
                        frozenFiles,
                        cancellationToken)
                    .ConfigureAwait(false);
                if (frozenFiles.Count > 0)
                {
                    _logger.LogWarning(
                        "Batch renumber {OldBatch} → {NewBatch} for PO {PO} Mill {Mill}: {Count} SAP-accepted file(s) stay frozen on the old batch: {Files}. Corrections for posted data must go through PPC.",
                        oldBatchNo,
                        newBatchNo,
                        po,
                        millNo,
                        frozenFiles.Count,
                        string.Join(", ", frozenFiles.Select(Path.GetFileName)));
                    files.RemoveAll(frozenFiles.Contains);
                    if (files.Count == 0 && frozenFiles.Count > 0)
                        return new OutputSlitBatchCorrectionResult(true, 0, Array.Empty<string>());
                }

                // FK safety: the final bundle row is inserted by the close/print flow AFTER stamp
                // correction, so create the parent row here if missing (placeholder Total 0 — the
                // close upsert overwrites it moments later). Without this the UPDATE hits
                // FK_Output_Slit_Row_Bundle and rows silently stay on the provisional number
                // (2026-07-26 bundle 1226100002/1226100003 incident).
                await EnsureBundleParentRowAsync(conn, po, millNo, newBatchNo.Trim(), cancellationToken).ConfigureAwait(false);

                var updateSql = @"
UPDATE dbo.Output_Slit_Row
SET NDT_Batch_No = @NewBatch
WHERE NDT_Batch_No = @OldBatch
  AND Mill_No = @Mill
  AND PO_Number = @Po
  AND NOT EXISTS (
    SELECT 1 FROM dbo.Output_Slit_Sap_Status s
    WHERE s.Status = N'Accepted'
      AND (
        Output_Slit_Row.Source_File = s.File_Name
        OR Output_Slit_Row.Source_File LIKE N'%\' + s.File_Name
        OR Output_Slit_Row.Source_File LIKE N'%/' + s.File_Name
      )
  )";
                await using var upd = new SqlCommand();
                upd.Connection = conn;
                upd.Parameters.AddWithValue("@NewBatch", newBatchNo.Trim());
                upd.Parameters.AddWithValue("@OldBatch", oldBatchNo.Trim());
                upd.Parameters.AddWithValue("@Mill", millNo);
                upd.Parameters.AddWithValue("@Po", po);
                if (frozenFiles.Count > 0)
                {
                    var frozenList = frozenFiles.ToList();
                    var frozenParams = frozenList.Select(static (_, i) => $"@Fz{i}").ToList();
                    updateSql += $@"
  AND (Source_File IS NULL OR Source_File NOT IN ({string.Join(", ", frozenParams)}))";
                    for (var i = 0; i < frozenList.Count; i++)
                        upd.Parameters.AddWithValue(frozenParams[i], frozenList[i]);
                }

                upd.CommandText = updateSql + ";";
                var updated = await ExecuteBatchRenumberAsync(
                        upd,
                        oldBatchNo,
                        newBatchNo,
                        po,
                        millNo,
                        frozenFiles,
                        cancellationToken)
                    .ConfigureAwait(false);
                if (updated > 0)
                {
                    _writeTracker.RecordSuccess("Output_Slit_Row", $"batch {oldBatchNo}→{newBatchNo} ({updated})");
                    _logger.LogInformation(
                        "Corrected {Count} Output_Slit_Row row(s) batch {OldBatch} → {NewBatch} for PO {PO} Mill {Mill}.",
                        updated,
                        oldBatchNo,
                        newBatchNo,
                        po,
                        millNo);
                }

                return new OutputSlitBatchCorrectionResult(true, updated, files);
            }
            catch (Exception ex)
            {
                lastError = ex;
                if (attempt < maxAttempts)
                {
                    _logger.LogWarning(
                        ex,
                        "Output_Slit_Row batch correct {OldBatch} → {NewBatch} for PO {PO} Mill {Mill} failed (attempt {Attempt}/{Max}); retrying.",
                        oldBatchNo,
                        newBatchNo,
                        po,
                        millNo,
                        attempt,
                        maxAttempts);
                    await Task.Delay(TimeSpan.FromMilliseconds(400 * attempt), cancellationToken).ConfigureAwait(false);
                }
            }
        }

        _writeTracker.RecordFailure("Output_Slit_Row", lastError?.Message ?? "batch correct failed", $"{oldBatchNo}→{newBatchNo}");
        _logger.LogError(
            lastError,
            "FAILED to correct Output_Slit_Row batch {OldBatch} → {NewBatch} for PO {PO} Mill {Mill} after {Max} attempts. "
            + "SQL rows still reference the old batch; reconcile slit sums and SAP traceability are wrong for this bundle until remediated.",
            oldBatchNo,
            newBatchNo,
            po,
            millNo,
            maxAttempts);
        return new OutputSlitBatchCorrectionResult(false, 0, files);
    }

    /// <summary>
    /// Inserts a placeholder <c>NDT_Bundle</c> row (Total 0) for the batch when none exists,
    /// so <c>Output_Slit_Row</c> FK writes cannot fail on a missing parent.
    /// </summary>
    private async Task EnsureBundleParentRowAsync(
        SqlConnection conn,
        string po,
        int millNo,
        string batchNo,
        CancellationToken cancellationToken)
    {
        const string sql = @"
IF NOT EXISTS (SELECT 1 FROM dbo.NDT_Bundle WHERE Bundle_No = @BundleNo)
BEGIN
    INSERT INTO dbo.NDT_Bundle
        (PO_Number, Mill_No, Bundle_No, Total_NDT_Pcs, Rejected_P, IsReprint)
    VALUES
        (@PoNumber, @MillNo, @BundleNo, 0, 0, 0);
    SELECT CAST(1 AS INT);
END
ELSE
    SELECT CAST(0 AS INT);";
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@PoNumber", po);
        cmd.Parameters.AddWithValue("@MillNo", millNo is >= 1 and <= 4 ? millNo : 1);
        cmd.Parameters.AddWithValue("@BundleNo", batchNo);
        var created = (int?)await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) ?? 0;
        if (created == 1)
        {
            _logger.LogInformation(
                "Created placeholder NDT_Bundle row for batch {Batch} (PO {PO}, Mill {Mill}, Total_NDT_Pcs=0) ahead of Output_Slit_Row batch correction.",
                batchNo,
                po,
                millNo);
        }
    }

    /// <summary>
    /// Source files (full paths as stored on <c>Output_Slit_Row.Source_File</c>) whose data SAP
    /// already accepted — their rows are frozen on the batch SAP posted them under. Detected via
    /// <c>Output_Slit_Sap_Status</c> (Status=Accepted, terminal) plus the NDT Input Slit Accepted
    /// folder on disk (covers deployments where the status table is not migrated yet).
    /// </summary>
    public async Task<IReadOnlyList<string>> GetSapFrozenSourceFilesAsync(
        IReadOnlyList<string> sourceFiles,
        CancellationToken cancellationToken)
    {
        if (!Enabled || sourceFiles.Count == 0)
            return Array.Empty<string>();

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await OpenConnectionAsync(conn, "Output_Slit_Row SAP freeze lookup", cancellationToken).ConfigureAwait(false);
            var frozen = await GetSapFrozenSourceFilesAsync(conn, sourceFiles, cancellationToken).ConfigureAwait(false);
            return frozen.ToList();
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "SAP frozen source-file lookup failed; proceeding without freeze list.");
            return Array.Empty<string>();
        }
    }

    /// <summary>
    /// Adds SAP-Accepted rows on <paramref name="oldBatchNo"/> to <paramref name="frozen"/> by
    /// joining <c>Output_Slit_Row</c> to <c>Output_Slit_Sap_Status</c> (covers basename/path drift).
    /// </summary>
    private async Task AugmentSapFrozenFromBatchRowsAsync(
        SqlConnection conn,
        string oldBatchNo,
        int millNo,
        string po,
        HashSet<string> frozen,
        CancellationToken cancellationToken)
    {
        try
        {
            const string sql = @"
SELECT DISTINCT o.Source_File
FROM dbo.Output_Slit_Row o
INNER JOIN dbo.Output_Slit_Sap_Status s ON s.Status = N'Accepted'
  AND (
    o.Source_File = s.File_Name
    OR o.Source_File LIKE N'%\' + s.File_Name
    OR o.Source_File LIKE N'%/' + s.File_Name
  )
WHERE o.NDT_Batch_No = @OldBatch
  AND o.Mill_No = @Mill
  AND o.PO_Number = @Po
  AND o.Source_File IS NOT NULL;";
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@OldBatch", oldBatchNo);
            cmd.Parameters.AddWithValue("@Mill", millNo);
            cmd.Parameters.AddWithValue("@Po", po);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!reader.IsDBNull(0))
                    frozen.Add(reader.GetString(0));
            }
        }
        catch (SqlException ex) when (IsMissingSapStatusTable(ex))
        {
            // Status table not migrated; Accepted-folder probe on the file list still applies.
        }
    }

    private async Task<int> ExecuteBatchRenumberAsync(
        SqlCommand upd,
        string oldBatchNo,
        string newBatchNo,
        string po,
        int millNo,
        HashSet<string> frozenFiles,
        CancellationToken cancellationToken)
    {
        try
        {
            return await upd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (SqlException ex) when (IsMissingSapStatusTable(ex))
        {
            _logger.LogWarning(
                "Output_Slit_Sap_Status missing — batch renumber {OldBatch} → {NewBatch} for PO {PO} Mill {Mill} "
                + "falls back to Accepted-folder freeze only (run docs/Output_Slit_Sap_Status_AddTable.sql).",
                oldBatchNo,
                newBatchNo,
                po,
                millNo);

            var fallbackSql = @"
UPDATE dbo.Output_Slit_Row
SET NDT_Batch_No = @NewBatch
WHERE NDT_Batch_No = @OldBatch
  AND Mill_No = @Mill
  AND PO_Number = @Po";
            if (frozenFiles.Count > 0)
            {
                var frozenList = frozenFiles.ToList();
                var frozenParams = frozenList.Select(static (_, i) => $"@Fz{i}").ToList();
                fallbackSql += $@"
  AND (Source_File IS NULL OR Source_File NOT IN ({string.Join(", ", frozenParams)}))";
            }

            upd.CommandText = fallbackSql + ";";
            return await upd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<HashSet<string>> GetSapFrozenSourceFilesAsync(
        SqlConnection conn,
        IReadOnlyList<string> sourceFiles,
        CancellationToken cancellationToken)
    {
        var frozen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (sourceFiles.Count == 0)
            return frozen;

        var pathsByBaseName = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        foreach (var path in sourceFiles)
        {
            var baseName = Path.GetFileName(path);
            if (string.IsNullOrEmpty(baseName))
                continue;
            if (!pathsByBaseName.TryGetValue(baseName, out var list))
                pathsByBaseName[baseName] = list = new List<string>();
            list.Add(path);
        }

        if (pathsByBaseName.Count == 0)
            return frozen;

        var acceptedFolder = (Opt.NdtInputSlitAcceptedFolder ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(acceptedFolder))
        {
            foreach (var (baseName, paths) in pathsByBaseName)
            {
                try
                {
                    if (File.Exists(Path.Combine(acceptedFolder, baseName)))
                        paths.ForEach(p => frozen.Add(p));
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "Could not probe SAP Accepted folder for {File}; SQL status check still applies.",
                        baseName);
                }
            }
        }
        else if (sourceFiles.Count > 0)
        {
            _logger.LogDebug(
                "NdtInputSlitAcceptedFolder is not configured; SAP Accepted freeze relies on Output_Slit_Sap_Status only.");
        }

        try
        {
            var names = pathsByBaseName.Keys.ToList();
            var parameters = names.Select(static (_, i) => $"@B{i}").ToList();
            var sql = $@"
SELECT File_Name
FROM dbo.Output_Slit_Sap_Status
WHERE Status = 'Accepted'
  AND File_Name IN ({string.Join(", ", parameters)});";
            await using var cmd = new SqlCommand(sql, conn);
            for (var i = 0; i < names.Count; i++)
                cmd.Parameters.AddWithValue(parameters[i], names[i]);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (reader.IsDBNull(0))
                    continue;
                if (pathsByBaseName.TryGetValue(reader.GetString(0).Trim(), out var paths))
                    paths.ForEach(p => frozen.Add(p));
            }
        }
        catch (SqlException ex) when (IsMissingSapStatusTable(ex))
        {
            // Status table not migrated; the Accepted-folder probe above still protects posted files.
        }

        return frozen;
    }

    public async Task<string?> TryGetExistingOutputSlitBatchAsync(
        string sourceFileFullPath,
        string poNumber,
        int millNo,
        CancellationToken cancellationToken)
    {
        if (!Enabled || string.IsNullOrWhiteSpace(sourceFileFullPath) || millNo is < 1 or > 4)
            return null;

        var baseName = Path.GetFileName(sourceFileFullPath);
        if (string.IsNullOrEmpty(baseName))
            return null;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po))
            return null;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await OpenConnectionAsync(conn, "Output_Slit_Row existing batch lookup", cancellationToken).ConfigureAwait(false);

            // Rows are keyed on the output path (same basename as the input file); match either.
            var esc = SqlLikeEscape(baseName);
            const string sql = @"
SELECT TOP 1 NDT_Batch_No
FROM dbo.Output_Slit_Row
WHERE PO_Number = @Po
  AND Mill_No = @Mill
  AND NDT_Batch_No IS NOT NULL
  AND LTRIM(RTRIM(NDT_Batch_No)) <> N''
  AND (Source_File = @BaseName OR Source_File LIKE @LikeWin OR Source_File LIKE @LikeUnix);";
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@Po", po);
            cmd.Parameters.AddWithValue("@Mill", millNo);
            cmd.Parameters.AddWithValue("@BaseName", baseName);
            cmd.Parameters.AddWithValue("@LikeWin", "%\\" + esc);
            cmd.Parameters.AddWithValue("@LikeUnix", "%/" + esc);
            var scalar = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return scalar is string batch && !string.IsNullOrWhiteSpace(batch) ? batch.Trim() : null;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Existing Output_Slit_Row batch lookup failed for {File} (PO {PO} Mill {Mill}).",
                baseName,
                poNumber,
                millNo);
            return null;
        }
    }

    private static bool IsMissingSapStatusTable(SqlException ex)
    {
        // 208 = invalid object name
        foreach (SqlError err in ex.Errors)
        {
            if (err.Number == 208
                && err.Message.Contains("Output_Slit_Sap_Status", StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return ex.Message.Contains("Output_Slit_Sap_Status", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsMissingInputSlitFileSeenTable(SqlException ex)
    {
        // 208 = invalid object name
        foreach (SqlError err in ex.Errors)
        {
            if (err.Number == 208
                && err.Message.Contains("Input_Slit_File_Seen", StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return ex.Message.Contains("Input_Slit_File_Seen", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsMissingSourceLastWriteColumn(SqlException ex)
    {
        // 207 = invalid column name
        foreach (SqlError err in ex.Errors)
        {
            if (err.Number == 207
                && err.Message.Contains("Source_LastWriteTimeUtc", StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return ex.Message.Contains("Source_LastWriteTimeUtc", StringComparison.OrdinalIgnoreCase);
    }

    public async Task RecordOutputSlitRowsAsync(
        string sourceFile,
        IReadOnlyList<(InputSlitRecord Record, string NdtBatchNo, int SourceRowNumber)> rows,
        CancellationToken cancellationToken)
    {
        if (!Enabled || rows.Count == 0)
        {
            if (rows.Count > 0 && !Enabled)
                _logger.LogWarning(
                    "SQL disabled: {Count} Output_Slit_Row row(s) from {File} were not saved to JazeeraMES_Prod.",
                    rows.Count,
                    sourceFile);
            return;
        }

        var inserted = 0;
        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await OpenConnectionAsync(conn, "Output_Slit_Row insert", cancellationToken).ConfigureAwait(false);

            // Idempotent per source file: a re-dropped/re-processed file REPLACES its earlier rows.
            // Blind appends double-count slit sums (2026-07-26: same late file attached 3× to a
            // closed bundle inflated Post_Recon_Csv_Sum 76 → 108).
            if (!string.IsNullOrWhiteSpace(sourceFile))
            {
                const string deleteSql = "DELETE FROM dbo.Output_Slit_Row WHERE Source_File = @SourceFile;";
                await using var del = new SqlCommand(deleteSql, conn);
                del.Parameters.AddWithValue("@SourceFile", sourceFile);
                var removed = await del.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                if (removed > 0)
                {
                    _logger.LogInformation(
                        "Replacing {Removed} existing Output_Slit_Row row(s) for re-processed file {File}.",
                        removed,
                        sourceFile);
                }
            }

            const string sql = @"
INSERT INTO dbo.Output_Slit_Row
    (PO_Number, Slit_No, NDT_Pipes, Rejected_P, Slit_Start_Time, Slit_Finish_Time, Mill_No, NDT_Short_Length_Pipe, Rejected_Short_Length_Pipe, NDT_Batch_No, Source_File, Source_Row_Number)
VALUES
    (@PoNumber, @SlitNo, @NdtPipes, @RejectedP, @StartTime, @FinishTime, @MillNo, @NdtShort, @RejShort, @BatchNo, @SourceFile, @SourceRowNumber);";

            foreach (var (r, batchNo, rowNo) in rows)
            {
                if (string.IsNullOrWhiteSpace(r.PoNumber) || string.IsNullOrWhiteSpace(batchNo))
                    continue;

                try
                {
                    await EnsureBundleRowExistsAsync(conn, r, batchNo, cancellationToken).ConfigureAwait(false);

                    await using var cmd = new SqlCommand(sql, conn);
                    cmd.Parameters.AddWithValue("@PoNumber", InputSlitCsvParsing.NormalizePo(r.PoNumber));
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
                    inserted++;
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Failed Output_Slit_Row insert for batch {BatchNo} PO {Po} row {RowNo} file {File}.",
                        batchNo,
                        r.PoNumber,
                        rowNo,
                        sourceFile);
                }
            }

            if (inserted > 0)
            {
                _writeTracker.RecordSuccess("Output_Slit_Row", $"{inserted} row(s) from {Path.GetFileName(sourceFile)}");
                _logger.LogInformation(
                    "Recorded {Count} Output_Slit_Row row(s) for file {File} in JazeeraMES_Prod.",
                    inserted,
                    sourceFile);
            }
            else
            {
                _writeTracker.RecordFailure("Output_Slit_Row", "No rows inserted.", sourceFile);
            }
        }
        catch (Exception ex)
        {
            _writeTracker.RecordFailure("Output_Slit_Row", ex.Message, sourceFile);
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
            await using var conn = SqlTraceabilityConnection.Create(Opt);
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

    public Task RecordManualStationRunAsync(
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
        CancellationToken cancellationToken) =>
        UpsertManualStationRunAsync(
            poNumber,
            ndtBatchNo,
            ndtPcs,
            okPcs,
            rejectPcs,
            workStation,
            start,
            end,
            hydrotestingType,
            sourceFile,
            cancellationToken);

    public async Task UpsertManualStationRunAsync(
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
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await OpenConnectionAsync(conn, "Manual_Station_Run upsert", cancellationToken).ConfigureAwait(false);

            const string updateSql = @"
UPDATE dbo.Manual_Station_Run
SET PO_Number = @PoNumber,
    NDT_Pcs = @NdtPcs,
    OK_Pcs = @Ok,
    Reject_Pcs = @Reject,
    Bundle_Start = @Start,
    Bundle_End = @End,
    Hydrotesting_Type = @HydroType,
    Source_File = @SourceFile
WHERE Manual_Station_Run_ID = (
    SELECT TOP (1) Manual_Station_Run_ID
    FROM dbo.Manual_Station_Run
    WHERE NDT_Batch_No = @BatchNo AND Work_Station = @WorkStation
    ORDER BY Manual_Station_Run_ID DESC);";

            await using (var updateCmd = new SqlCommand(updateSql, conn))
            {
                AddManualStationParameters(updateCmd, poNumber, ndtBatchNo, ndtPcs, okPcs, rejectPcs, workStation, start, end, hydrotestingType, sourceFile);
                var updated = await updateCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                if (updated > 0)
                {
                    _writeTracker.RecordSuccess("Manual_Station_Run", $"{ndtBatchNo} {workStation}");
                    _logger.LogInformation(
                        "Updated Manual_Station_Run for batch {BatchNo} station {Station} in JazeeraMES_Prod.",
                        ndtBatchNo,
                        workStation);
                    return;
                }
            }

            const string insertSql = @"
INSERT INTO dbo.Manual_Station_Run
    (PO_Number, NDT_Batch_No, NDT_Pcs, OK_Pcs, Reject_Pcs, Work_Station, Bundle_Start, Bundle_End, Hydrotesting_Type, Source_File)
VALUES
    (@PoNumber, @BatchNo, @NdtPcs, @Ok, @Reject, @WorkStation, @Start, @End, @HydroType, @SourceFile);";

            await using var insertCmd = new SqlCommand(insertSql, conn);
            AddManualStationParameters(insertCmd, poNumber, ndtBatchNo, ndtPcs, okPcs, rejectPcs, workStation, start, end, hydrotestingType, sourceFile);
            await insertCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            _writeTracker.RecordSuccess("Manual_Station_Run", $"{ndtBatchNo} {workStation}");
            _logger.LogInformation(
                "Inserted Manual_Station_Run for batch {BatchNo} station {Station} in JazeeraMES_Prod.",
                ndtBatchNo,
                workStation);
        }
        catch (Exception ex)
        {
            _writeTracker.RecordFailure("Manual_Station_Run", ex.Message, ndtBatchNo);
            _logger.LogError(ex, "Failed to upsert Manual_Station_Run for batch {BatchNo} in JazeeraMES_Prod.", ndtBatchNo);
        }
    }

    private static void AddManualStationParameters(
        SqlCommand cmd,
        string poNumber,
        string ndtBatchNo,
        int ndtPcs,
        int okPcs,
        int rejectPcs,
        string workStation,
        DateTime start,
        DateTime end,
        string? hydrotestingType,
        string sourceFile)
    {
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
            await using var conn = SqlTraceabilityConnection.Create(Opt);
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
            _writeTracker.RecordSuccess("NDT_Process_Consolidated", ndtBatchNo);
            _logger.LogInformation(
                "Recorded NDT_Process_Consolidated for batch {BatchNo} in JazeeraMES_Prod (output {OutputFile}).",
                ndtBatchNo,
                outputFilePath);
        }
        catch (Exception ex)
        {
            _writeTracker.RecordFailure("NDT_Process_Consolidated", ex.Message, ndtBatchNo);
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
            await using var conn = SqlTraceabilityConnection.Create(Opt);
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
            _writeTracker.RecordSuccess("Bundle_Label", $"PO {po} mill {millNo}");
            _logger.LogInformation("Recorded Bundle_Label for PO {PoNumber} mill {MillNo} in JazeeraMES_Prod.", po, millNo);
        }
        catch (Exception ex)
        {
            _writeTracker.RecordFailure("Bundle_Label", ex.Message, $"PO {po} mill {millNo}");
            _logger.LogError(ex, "Failed to record Bundle_Label for PO {PoNumber} mill {MillNo} in JazeeraMES_Prod.", po, millNo);
        }
    }

    public async Task RecordUploadBundleRowsAsync(string generatedFile, IReadOnlyList<UploadBundleRow> rows, CancellationToken cancellationToken)
    {
        if (!Enabled || rows.Count == 0)
            return;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
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

    public async Task<int> UpdateOutputSlitRowNdtPipesByBatchAndSlitAsync(
        string ndtBatchNo,
        string slitNo,
        int ndtPipes,
        CancellationToken cancellationToken)
    {
        if (!Enabled || string.IsNullOrWhiteSpace(ndtBatchNo))
            return 0;

        var batch = ndtBatchNo.Trim();
        var slit = ReconcileCsvParsing.NormalizeSlitKey(slitNo);

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await OpenConnectionAsync(conn, "Output_Slit_Row update by slit", cancellationToken).ConfigureAwait(false);

            const string sql = @"
UPDATE dbo.Output_Slit_Row
SET NDT_Pipes = @NdtPipes
WHERE NDT_Batch_No = @BatchNo
  AND (
    (@MatchDash = 0 AND Slit_No = @SlitNo)
    OR (@MatchDash = 1 AND (Slit_No IS NULL OR Slit_No = N'—' OR LTRIM(RTRIM(Slit_No)) = N''))
  );";

            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@NdtPipes", ndtPipes);
            cmd.Parameters.AddWithValue("@BatchNo", batch);
            cmd.Parameters.AddWithValue("@SlitNo", slit);
            cmd.Parameters.AddWithValue("@MatchDash", slit == "—" ? 1 : 0);
            var rows = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            if (rows > 0)
            {
                _writeTracker.RecordSuccess("Output_Slit_Row", $"{batch} slit {slit}");
                _logger.LogInformation(
                    "Updated {Count} Output_Slit_Row row(s) for batch {BatchNo} slit {SlitNo} to {NdtPipes} pipes.",
                    rows,
                    batch,
                    slit,
                    ndtPipes);
            }

            return rows;
        }
        catch (Exception ex)
        {
            _writeTracker.RecordFailure("Output_Slit_Row", ex.Message, batch);
            _logger.LogWarning(ex, "Failed to update Output_Slit_Row for batch {BatchNo} slit {SlitNo}.", batch, slit);
            return 0;
        }
    }

    public async Task SyncOutputSlitRowsFromPerSlitCsvForBatchAsync(string ndtBatchNo, CancellationToken cancellationToken)
    {
        if (!Enabled || string.IsNullOrWhiteSpace(ndtBatchNo))
            return;

        var folder = (Opt.OutputBundleFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return;

        var batch = ndtBatchNo.Trim();
        const int colNdtPipes = 2;
        const int colNdtBatchNo = 9;
        const int minColumns = 10;
        var synced = 0;
        var ioToken = CancellationToken.None;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await OpenConnectionAsync(conn, "Output_Slit_Row sync from CSV", CancellationToken.None).ConfigureAwait(false);

            const string sql = @"
UPDATE dbo.Output_Slit_Row
SET NDT_Pipes = @NdtPipes
WHERE NDT_Batch_No = @BatchNo
  AND Source_File = @SourceFile
  AND Source_Row_Number = @SourceRow;";

            var candidatePaths = await ResolvePerSlitCsvPathsForBatchSyncAsync(conn, folder, batch, CancellationToken.None)
                .ConfigureAwait(false);

            foreach (var path in candidatePaths)
            {
                string[] lines;
                try
                {
                    lines = await File.ReadAllLinesAsync(path, ioToken).ConfigureAwait(false);
                }
                catch
                {
                    continue;
                }

                var baseName = Path.GetFileName(path);
                for (var i = 1; i < lines.Length; i++)
                {
                    var line = lines[i];
                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    var cols = ReconcileCsvParsing.SplitCsvLine(line);
                    if (cols.Count < minColumns)
                        continue;
                    if (!cols[colNdtBatchNo].Trim().Equals(batch, StringComparison.OrdinalIgnoreCase))
                        continue;

                    if (!int.TryParse(cols[colNdtPipes].Trim(), out var pipes))
                        pipes = 0;

                    await using var cmd = new SqlCommand(sql, conn);
                    cmd.Parameters.AddWithValue("@NdtPipes", pipes);
                    cmd.Parameters.AddWithValue("@BatchNo", batch);
                    cmd.Parameters.AddWithValue("@SourceFile", baseName);
                    cmd.Parameters.AddWithValue("@SourceRow", i + 1);
                    synced += await cmd.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
                }
            }

            if (synced > 0)
            {
                _writeTracker.RecordSuccess("Output_Slit_Row", $"{batch} sync {synced}");
                _logger.LogInformation(
                    "Synced {Count} Output_Slit_Row row(s) from per-slit CSVs for batch {BatchNo}.",
                    synced,
                    batch);
            }
        }
        catch (Exception ex)
        {
            _writeTracker.RecordFailure("Output_Slit_Row", ex.Message, batch);
            _logger.LogWarning(ex, "Failed to sync Output_Slit_Row from CSV for batch {BatchNo}.", batch);
        }
    }

    private static async Task<List<string>> ResolvePerSlitCsvPathsForBatchSyncAsync(
        SqlConnection conn,
        string folder,
        string batch,
        CancellationToken cancellationToken)
    {
        const string sourceSql = @"
SELECT DISTINCT Source_File
FROM dbo.Output_Slit_Row
WHERE NDT_Batch_No = @BatchNo
  AND Source_File IS NOT NULL
  AND LTRIM(RTRIM(Source_File)) <> N''";

        var paths = new List<string>();
        await using (var cmd = new SqlCommand(sourceSql, conn))
        {
            cmd.Parameters.AddWithValue("@BatchNo", batch);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
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
        }

        if (paths.Count > 0)
            return paths;

        return Directory.EnumerateFiles(folder, "*.csv")
            .Where(p => !Path.GetFileName(p).StartsWith("NDT_Bundle_", StringComparison.OrdinalIgnoreCase))
            .ToList();
    }

    public async Task UpdateNdtProcessConsolidatedFromStationsAsync(
        string poNumber,
        string ndtBatchNo,
        int ndtPcs,
        int okPcs,
        int visualReject,
        int hydrotestReject,
        int revisualReject,
        DateTime? bundleStart,
        DateTime? bundleEnd,
        string? outputFilePath,
        CancellationToken cancellationToken)
    {
        if (!Enabled || string.IsNullOrWhiteSpace(ndtBatchNo))
            return;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await OpenConnectionAsync(conn, "NDT_Process_Consolidated partial update", CancellationToken.None).ConfigureAwait(false);

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
        Bundle_Start = COALESCE(@BundleStart, Bundle_Start),
        Bundle_End = COALESCE(@BundleEnd, Bundle_End),
        Output_File = COALESCE(@OutputFile, Output_File)
    WHERE NDT_Batch_No = @BatchNo;
END";

            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@PoNumber", poNumber);
            cmd.Parameters.AddWithValue("@BatchNo", ndtBatchNo.Trim());
            cmd.Parameters.AddWithValue("@NdtPcs", ndtPcs);
            cmd.Parameters.AddWithValue("@Ok", okPcs);
            cmd.Parameters.AddWithValue("@VisualRej", visualReject);
            cmd.Parameters.AddWithValue("@HydroRej", hydrotestReject);
            cmd.Parameters.AddWithValue("@RevisualRej", revisualReject);
            cmd.Parameters.AddWithValue("@BundleStart", (object?)bundleStart ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@BundleEnd", (object?)bundleEnd ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@OutputFile", (object?)NullIfEmpty(outputFilePath) ?? DBNull.Value);
            var rows = await cmd.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
            if (rows > 0)
            {
                _writeTracker.RecordSuccess("NDT_Process_Consolidated", ndtBatchNo);
                _logger.LogInformation(
                    "Updated NDT_Process_Consolidated for batch {BatchNo} after manual station reconcile.",
                    ndtBatchNo);
            }
        }
        catch (Exception ex)
        {
            _writeTracker.RecordFailure("NDT_Process_Consolidated", ex.Message, ndtBatchNo);
            _logger.LogWarning(ex, "Failed partial NDT_Process_Consolidated update for batch {BatchNo}.", ndtBatchNo);
        }
    }

    private static string? NullIfEmpty(string? s) =>
        string.IsNullOrWhiteSpace(s) ? null : s.Trim();

    private async Task EnsureBundleRowExistsAsync(
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
    SELECT CAST(1 AS INT);
END
ELSE
    SELECT CAST(0 AS INT);";

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@PoNumber", InputSlitCsvParsing.NormalizePo(record.PoNumber));
        cmd.Parameters.AddWithValue("@MillNo", record.MillNo is >= 1 and <= 4 ? record.MillNo : 1);
        cmd.Parameters.AddWithValue("@BundleNo", batchNo);
        cmd.Parameters.AddWithValue("@SlitNo", (object?)NullIfEmpty(record.SlitNo) ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@SlitStartTime", (object?)record.SlitStartTime ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@SlitFinishTime", (object?)record.SlitFinishTime ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@RejectedPipes", record.RejectedPipes);
        cmd.Parameters.AddWithValue("@NdtShortLengthPipe", (object?)NullIfEmpty(record.NdtShortLengthPipe) ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@RejectedShortLengthPipe", (object?)NullIfEmpty(record.RejectedShortLengthPipe) ?? DBNull.Value);
        var created = (int?)await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) ?? 0;
        if (created == 1)
        {
            // Also fires when a deleted/remediated bundle number is resurrected by a late slit row —
            // the PO on this log line is the slit row's PO, which may differ from the original bundle owner.
            _logger.LogInformation(
                "Created NDT_Bundle parent row for batch {Batch} (PO {PO}, Mill {Mill}, Total_NDT_Pcs=0) from Output_Slit_Row write (forming bundle or missing/deleted parent).",
                batchNo,
                InputSlitCsvParsing.NormalizePo(record.PoNumber),
                record.MillNo);
        }
    }
}

