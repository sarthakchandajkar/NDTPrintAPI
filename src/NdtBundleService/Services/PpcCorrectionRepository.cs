using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>One tracked "PPC must fix this in SAP" correction (Phase 3 of SAP status tracking).</summary>
public sealed record PpcCorrectionItem(
    long Id,
    string NdtBatchNo,
    string FileName,
    string SlitNo,
    int? OldNdtPipes,
    int CorrectedNdtPipes,
    string Status,
    DateTime CreatedAtUtc,
    DateTime? UpdatedAtUtc,
    DateTime? ClearedAtUtc,
    string? ClearedBy,
    string? ClearedNote)
{
    public const string StatusOpen = "Open";
    public const string StatusCleared = "Cleared";
}

/// <summary>Outcome of an upsert: the item id and whether a new Open item was created (vs updated).</summary>
public sealed record PpcCorrectionUpsertResult(long Id, bool Created);

/// <summary>
/// Persists <c>Ppc_Correction_Item</c> rows: auto-created when an operator's slit reconcile touches
/// data whose output file is already SAP-Accepted (local MES correction is applied; SAP needs a
/// manual fix by PPC). "Ppc_Correction_Pending" on a bundle is derived — any Open item for the
/// batch — never a stored flag. Nothing is ever sent automatically. Best-effort like the other
/// traceability repositories: SQL failures are logged, never thrown to callers.
/// </summary>
public interface IPpcCorrectionRepository
{
    /// <summary>False when SQL traceability is not configured; callers may skip work.</summary>
    bool Enabled { get; }

    /// <summary>
    /// Creates an Open item for (batch, file, slit), or updates <c>Corrected_NDT_Pipes</c> on the
    /// existing Open item (repeated local corrections do not stack duplicates; the original
    /// <c>Old_NDT_Pipes</c> — the value SAP still has — is preserved). Null on SQL failure.
    /// </summary>
    Task<PpcCorrectionUpsertResult?> UpsertOpenItemAsync(
        string batchNo,
        string fileName,
        string slitNo,
        int? oldNdtPipes,
        int correctedNdtPipes,
        CancellationToken cancellationToken);

    Task<PpcCorrectionUpsertResult?> UpsertOpenItemAsync(
        string batchNo,
        string fileName,
        string slitNo,
        int? oldNdtPipes,
        int correctedNdtPipes,
        CancellationToken cancellationToken,
        string? replacementBatchNo) =>
        UpsertOpenItemAsync(batchNo, fileName, slitNo, oldNdtPipes, correctedNdtPipes, cancellationToken);

    /// <summary>Items for a bundle, Open first then newest; optionally including Cleared history.</summary>
    Task<IReadOnlyList<PpcCorrectionItem>> GetItemsForBatchAsync(
        string batchNo,
        bool includeCleared,
        CancellationToken cancellationToken);

    /// <summary>Open item count for the bundle (drives the derived Ppc_Correction_Pending status).</summary>
    Task<int> CountOpenItemsForBatchAsync(string batchNo, CancellationToken cancellationToken);

    /// <summary>
    /// Marks an Open item Cleared (operator confirms PPC applied the SAP-side fix).
    /// False when the item does not exist, is already Cleared, or SQL failed.
    /// </summary>
    Task<bool> ClearItemAsync(long id, string? clearedBy, string? note, CancellationToken cancellationToken);
}

public sealed class PpcCorrectionRepository : IPpcCorrectionRepository
{
    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly ISqlTraceabilityWriteTracker _writeTracker;
    private readonly ILogger<PpcCorrectionRepository> _logger;

    public PpcCorrectionRepository(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        ISqlTraceabilityWriteTracker writeTracker,
        ILogger<PpcCorrectionRepository> logger)
    {
        _optionsMonitor = optionsMonitor;
        _writeTracker = writeTracker;
        _logger = logger;
    }

    private NdtBundleOptions Opt => _optionsMonitor.CurrentValue;

    public bool Enabled => SqlTraceabilityConnection.IsSqlEnabled(Opt);

    public async Task<PpcCorrectionUpsertResult?> UpsertOpenItemAsync(
        string batchNo,
        string fileName,
        string slitNo,
        int? oldNdtPipes,
        int correctedNdtPipes,
        CancellationToken cancellationToken,
        string? replacementBatchNo)
    {
        var created = await UpsertOpenItemAsync(
            batchNo, fileName, slitNo, oldNdtPipes, correctedNdtPipes, cancellationToken)
            .ConfigureAwait(false);
        if (created is null || string.IsNullOrWhiteSpace(replacementBatchNo))
            return created;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var cmd = new SqlCommand(@"
UPDATE dbo.Ppc_Correction_Item
SET Replacement_NDT_Batch_No = @Rep
WHERE NDT_Batch_No = @Batch AND File_Name = @File AND Slit_No = @Slit AND Status = N'Open';", conn);
            cmd.Parameters.AddWithValue("@Rep", replacementBatchNo.Trim());
            cmd.Parameters.AddWithValue("@Batch", batchNo.Trim());
            cmd.Parameters.AddWithValue("@File", fileName.Trim());
            cmd.Parameters.AddWithValue("@Slit", ReconcileCsvParsing.NormalizeSlitKey(slitNo));
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not set Replacement_NDT_Batch_No (run docs/Ppc_Correction_Item_Alter_ReplacementBatch.sql).");
        }

        return created;
    }

    public async Task<PpcCorrectionUpsertResult?> UpsertOpenItemAsync(
        string batchNo,
        string fileName,
        string slitNo,
        int? oldNdtPipes,
        int correctedNdtPipes,
        CancellationToken cancellationToken)
    {
        if (!Enabled || string.IsNullOrWhiteSpace(batchNo) || string.IsNullOrWhiteSpace(fileName))
            return null;

        var batch = batchNo.Trim();
        var file = fileName.Trim();
        var slit = ReconcileCsvParsing.NormalizeSlitKey(slitNo);

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await SqlTraceabilityConnection
                .OpenAsync(conn, _logger, "Ppc_Correction_Item upsert", cancellationToken)
                .ConfigureAwait(false);

            const string updateSql = @"
UPDATE dbo.Ppc_Correction_Item
SET Corrected_NDT_Pipes = @Corrected,
    Updated_AtUtc = SYSUTCDATETIME()
OUTPUT INSERTED.Ppc_Correction_Item_ID
WHERE NDT_Batch_No = @Batch AND File_Name = @File AND Slit_No = @Slit AND Status = N'Open';";
            await using (var update = new SqlCommand(updateSql, conn))
            {
                update.Parameters.AddWithValue("@Batch", batch);
                update.Parameters.AddWithValue("@File", file);
                update.Parameters.AddWithValue("@Slit", slit);
                update.Parameters.AddWithValue("@Corrected", correctedNdtPipes);
                var existingId = await update.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                if (existingId is long id)
                {
                    _writeTracker.RecordSuccess("Ppc_Correction_Item", $"updated open item {id}");
                    _logger.LogInformation(
                        "PPC correction item {Id} updated for bundle {BatchNo} slit {SlitNo} ({File}): corrected value now {Corrected}.",
                        id, batch, slit, file, correctedNdtPipes);
                    return new PpcCorrectionUpsertResult(id, Created: false);
                }
            }

            const string insertSql = @"
INSERT INTO dbo.Ppc_Correction_Item
    (NDT_Batch_No, File_Name, Slit_No, Old_NDT_Pipes, Corrected_NDT_Pipes, Status)
OUTPUT INSERTED.Ppc_Correction_Item_ID
VALUES
    (@Batch, @File, @Slit, @Old, @Corrected, N'Open');";
            await using (var insert = new SqlCommand(insertSql, conn))
            {
                insert.Parameters.AddWithValue("@Batch", batch);
                insert.Parameters.AddWithValue("@File", file);
                insert.Parameters.AddWithValue("@Slit", slit);
                insert.Parameters.AddWithValue("@Old", oldNdtPipes.HasValue ? oldNdtPipes.Value : DBNull.Value);
                insert.Parameters.AddWithValue("@Corrected", correctedNdtPipes);
                var newId = (long)(await insert.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
                _writeTracker.RecordSuccess("Ppc_Correction_Item", $"created open item {newId}");
                _logger.LogInformation(
                    "PPC correction item {Id} created for bundle {BatchNo} slit {SlitNo} ({File}): {Old} → {Corrected}. "
                    + "Bundle is now Ppc_Correction_Pending until the operator clears it after the PPC-side SAP fix.",
                    newId, batch, slit, file, oldNdtPipes, correctedNdtPipes);
                return new PpcCorrectionUpsertResult(newId, Created: true);
            }
        }
        catch (SqlException ex) when (IsMissingPpcTable(ex))
        {
            _logger.LogWarning(
                "Ppc_Correction_Item table missing — run docs/Ppc_Correction_Item_AddTable.sql. "
                + "PPC correction not recorded for bundle {BatchNo} slit {SlitNo} ({File}).",
                batch, slit, file);
            return null;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex, "PPC correction upsert failed for bundle {BatchNo} slit {SlitNo} ({File}).", batch, slit, file);
            return null;
        }
    }

    public async Task<IReadOnlyList<PpcCorrectionItem>> GetItemsForBatchAsync(
        string batchNo,
        bool includeCleared,
        CancellationToken cancellationToken)
    {
        if (!Enabled || string.IsNullOrWhiteSpace(batchNo))
            return Array.Empty<PpcCorrectionItem>();

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await SqlTraceabilityConnection
                .OpenAsync(conn, _logger, "Ppc_Correction_Item read", cancellationToken)
                .ConfigureAwait(false);

            var sql = @"
SELECT Ppc_Correction_Item_ID, NDT_Batch_No, File_Name, Slit_No, Old_NDT_Pipes, Corrected_NDT_Pipes,
       Status, Created_AtUtc, Updated_AtUtc, Cleared_AtUtc, Cleared_By, Cleared_Note
FROM dbo.Ppc_Correction_Item
WHERE (NDT_Batch_No = @Batch OR Replacement_NDT_Batch_No = @Batch)"
                + (includeCleared ? string.Empty : " AND Status = N'Open'")
                + @"
ORDER BY CASE WHEN Status = N'Open' THEN 0 ELSE 1 END, Created_AtUtc DESC;";

            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@Batch", batchNo.Trim());

            var items = new List<PpcCorrectionItem>();
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                items.Add(new PpcCorrectionItem(
                    reader.GetInt64(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetInt32(4),
                    reader.GetInt32(5),
                    reader.GetString(6),
                    ReadUtc(reader, 7)!.Value,
                    ReadUtc(reader, 8),
                    ReadUtc(reader, 9),
                    reader.IsDBNull(10) ? null : reader.GetString(10),
                    reader.IsDBNull(11) ? null : reader.GetString(11)));
            }

            return items;
        }
        catch (SqlException ex) when (IsMissingPpcTable(ex))
        {
            _logger.LogDebug("Ppc_Correction_Item missing (run docs/Ppc_Correction_Item_AddTable.sql); no items returned.");
            return Array.Empty<PpcCorrectionItem>();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "PPC correction read failed for bundle {BatchNo}.", batchNo);
            return Array.Empty<PpcCorrectionItem>();
        }
    }

    public async Task<int> CountOpenItemsForBatchAsync(string batchNo, CancellationToken cancellationToken)
    {
        if (!Enabled || string.IsNullOrWhiteSpace(batchNo))
            return 0;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await SqlTraceabilityConnection
                .OpenAsync(conn, _logger, "Ppc_Correction_Item count", cancellationToken)
                .ConfigureAwait(false);

            const string sql = @"
SELECT COUNT(*) FROM dbo.Ppc_Correction_Item
WHERE NDT_Batch_No = @Batch AND Status = N'Open';";
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@Batch", batchNo.Trim());
            var scalar = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return scalar is int count ? count : 0;
        }
        catch (SqlException ex) when (IsMissingPpcTable(ex))
        {
            return 0;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "PPC correction open-count failed for bundle {BatchNo}.", batchNo);
            return 0;
        }
    }

    public async Task<bool> ClearItemAsync(long id, string? clearedBy, string? note, CancellationToken cancellationToken)
    {
        if (!Enabled || id <= 0)
            return false;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await SqlTraceabilityConnection
                .OpenAsync(conn, _logger, "Ppc_Correction_Item clear", cancellationToken)
                .ConfigureAwait(false);

            const string sql = @"
UPDATE dbo.Ppc_Correction_Item
SET Status = N'Cleared',
    Cleared_AtUtc = SYSUTCDATETIME(),
    Cleared_By = @By,
    Cleared_Note = @Note
WHERE Ppc_Correction_Item_ID = @Id AND Status = N'Open';";
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@Id", id);
            cmd.Parameters.AddWithValue("@By", string.IsNullOrWhiteSpace(clearedBy) ? DBNull.Value : clearedBy.Trim());
            cmd.Parameters.AddWithValue("@Note", string.IsNullOrWhiteSpace(note) ? DBNull.Value : note.Trim());
            var rows = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            if (rows > 0)
            {
                _writeTracker.RecordSuccess("Ppc_Correction_Item", $"cleared item {id}");
                _logger.LogInformation("PPC correction item {Id} cleared (by {By}).", id, clearedBy ?? "(unspecified)");
                return true;
            }

            return false;
        }
        catch (SqlException ex) when (IsMissingPpcTable(ex))
        {
            _logger.LogWarning("Ppc_Correction_Item table missing — cannot clear item {Id}.", id);
            return false;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "PPC correction clear failed for item {Id}.", id);
            return false;
        }
    }

    private static DateTime? ReadUtc(SqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : DateTime.SpecifyKind(reader.GetDateTime(ordinal), DateTimeKind.Utc);

    private static bool IsMissingPpcTable(SqlException ex)
    {
        // 208 = invalid object name
        foreach (SqlError err in ex.Errors)
        {
            if (err.Number == 208
                && err.Message.Contains("Ppc_Correction_Item", StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }
}
