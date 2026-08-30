using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

public sealed record CsvFillIncompleteBundle(
    string BundleNo,
    int TargetNdtPcs,
    int CsvFilled,
    string FillState,
    DateTime? PrintedAtUtc,
    DateTime? CsvLastRowAtUtc);

public sealed record CsvFillHoldReason
{
    public const string NoOpenBundle = "NoOpenBundle";
    public const string QuietExpired = "HoldQuietExpired";
}

public interface ICsvFillService
{
    /// <summary>
    /// After close/print: set <c>Target_Ndt_Pcs</c>, reset fill counters, optional Close_Source.
    /// </summary>
    Task TryInitializeFillTargetAsync(
        string bundleNo,
        int targetNdtPcs,
        string? closeSource,
        CancellationToken cancellationToken);

    /// <summary>
    /// Oldest incomplete fill slot. Voided bundles are excluded in SQL, not only via
    /// <see cref="CsvFillState.IsIncomplete"/>.
    /// </summary>
    public const string OldestIncompleteSelectSql = @"
SELECT TOP 1
    Bundle_No,
    COALESCE(Target_Ndt_Pcs, Total_NDT_Pcs) AS TargetPcs,
    Csv_Filled,
    Csv_Fill_State,
    PrintedAt,
    Csv_Last_Row_AtUtc
FROM dbo.NDT_Bundle
WHERE Mill_No = @MillNo
  AND (PO_Number = @Po OR PO_Number = @PoNormalized)
  AND Target_Ndt_Pcs IS NOT NULL
  AND Csv_Fill_State IN (N'PlcClosed', N'CsvFilling')
  AND ISNULL(Voided, 0) = 0
ORDER BY PrintedAt ASC, Bundle_No ASC;";

    /// <summary>Stamp queue: same incomplete states, voided excluded in SQL.</summary>
    public const string StampFindIncompleteSql = @"
SELECT TOP 1
    Bundle_No,
    COALESCE(Target_Ndt_Pcs, Total_NDT_Pcs),
    Csv_Filled
FROM dbo.NDT_Bundle WITH (UPDLOCK, ROWLOCK)
WHERE Mill_No = @MillNo
  AND (PO_Number = @Po OR PO_Number = @PoNormalized)
  AND Target_Ndt_Pcs IS NOT NULL
  AND Csv_Fill_State IN (N'PlcClosed', N'CsvFilling')
  AND ISNULL(Voided, 0) = 0
ORDER BY PrintedAt ASC, Bundle_No ASC;";

    Task<CsvFillIncompleteBundle?> TryGetOldestIncompleteAsync(
        string poNumber,
        int millNo,
        string? pipeSize,
        CancellationToken cancellationToken);

    /// <summary>
    /// Single transaction: stamp whole-file pipes onto the oldest incomplete bundle.
    /// Null when no incomplete slot exists.
    /// </summary>
    Task<CsvFillStampResult?> TryStampFileAsync(
        string poNumber,
        int millNo,
        string? pipeSize,
        int fileNdtPipes,
        CancellationToken cancellationToken);

    /// <summary>Mark incomplete slots quiet-short and advance (PO end or quiet timer).</summary>
    Task<int> AdvanceQuietShortAsync(
        string? poNumber,
        int? millNo,
        int quietMinutes,
        DateTime utcNow,
        bool forcePoEnd,
        CancellationToken cancellationToken);

    Task UpsertHoldAsync(
        string sourceFileName,
        string poNumber,
        int millNo,
        string? pipeSize,
        string reasonCode,
        CancellationToken cancellationToken);

    Task<int> EscalateExpiredHoldsAsync(
        int quietMinutes,
        DateTime utcNow,
        CancellationToken cancellationToken,
        int? millNo = null);

    /// <summary>Same-basename count revision: adjust Csv_Filled by delta only.</summary>
    Task ApplyCountRevisionAsync(
        string sourceFileName,
        string batchNo,
        int oldNdtPipes,
        int newNdtPipes,
        CancellationToken cancellationToken);

    /// <summary>
    /// Output_Slit_Row batch reassignment for one file basename (resubmit §5.3).
    /// Matches <c>Source_File</c> by basename (full path or name). Not <c>Source_File_Name</c>.
    /// </summary>
    public const string OutputSlitRowBatchMoveSql = @"
UPDATE dbo.Output_Slit_Row
SET NDT_Batch_No = @NewBatch
WHERE NDT_Batch_No = @OldBatch
  AND (
    Source_File = @File
    OR Source_File LIKE @LikeWin
    OR Source_File LIKE @LikeUnix
  );";

    /// <summary>Atomic batch move for one file basename (source loses n, target gains n).</summary>
    Task<Guid> ApplyBatchMoveAsync(
        string sourceFileName,
        string oldBatchNo,
        string newBatchNo,
        int ndtPipes,
        CancellationToken cancellationToken);

    Task<bool> HasAwaitingCsvReconRowsAsync(CancellationToken cancellationToken, int? millNo = null);

    Task<bool> HasBundlesMissingFillTargetAsync(CancellationToken cancellationToken, int? millNo = null);
}

public sealed class CsvFillService : ICsvFillService
{
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly ILogger<CsvFillService> _logger;

    public CsvFillService(IOptionsMonitor<NdtBundleOptions> options, ILogger<CsvFillService> logger)
    {
        _options = options;
        _logger = logger;
    }

    private NdtBundleOptions Opt => _options.CurrentValue;
    private bool UseDatabase => SqlTraceabilityConnection.IsSqlEnabled(Opt);

    public async Task TryInitializeFillTargetAsync(
        string bundleNo,
        int targetNdtPcs,
        string? closeSource,
        CancellationToken cancellationToken)
    {
        if (!UseDatabase || string.IsNullOrWhiteSpace(bundleNo) || targetNdtPcs < 0)
            return;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            const string sql = @"
UPDATE dbo.NDT_Bundle
SET Target_Ndt_Pcs = @Target,
    Csv_Filled = 0,
    Csv_Fill_State = N'PlcClosed',
    Csv_Last_Row_AtUtc = NULL,
    Awaiting_Csv_Recon = 0,
    Close_Source = COALESCE(@CloseSource, Close_Source)
WHERE Bundle_No = @BundleNo;";
            await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@Target", targetNdtPcs);
            cmd.Parameters.AddWithValue("@BundleNo", bundleNo.Trim());
            cmd.Parameters.AddWithValue(
                "@CloseSource",
                string.IsNullOrWhiteSpace(closeSource) ? (object)DBNull.Value : closeSource.Trim());
            var n = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            if (n > 0)
            {
                _logger.LogInformation(
                    "Initialized fill target for bundle {BundleNo}: Target_Ndt_Pcs={Target} Close_Source={CloseSource}.",
                    bundleNo.Trim(),
                    targetNdtPcs,
                    closeSource ?? "(unchanged)");
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to initialize fill target for {BundleNo} (run docs/NDT_Bundle_Alter_CsvFill.sql).",
                bundleNo);
        }
    }

    public async Task<CsvFillIncompleteBundle?> TryGetOldestIncompleteAsync(
        string poNumber,
        int millNo,
        string? pipeSize,
        CancellationToken cancellationToken)
    {
        if (!UseDatabase || millNo is < 1 or > 4)
            return null;

        var normalized = InputSlitCsvParsing.NormalizePo(poNumber);
        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(ICsvFillService.OldestIncompleteSelectSql, conn);
            cmd.Parameters.AddWithValue("@MillNo", millNo);
            cmd.Parameters.AddWithValue("@Po", poNumber.Trim());
            cmd.Parameters.AddWithValue("@PoNormalized", normalized);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                return null;

            return new CsvFillIncompleteBundle(
                reader.GetString(0),
                reader.GetInt32(1),
                reader.GetInt32(2),
                reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                reader.IsDBNull(5) ? null : reader.GetDateTime(5));
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "TryGetOldestIncomplete failed for PO {PO} Mill {Mill}.", normalized, millNo);
            return null;
        }
    }

    public async Task<CsvFillStampResult?> TryStampFileAsync(
        string poNumber,
        int millNo,
        string? pipeSize,
        int fileNdtPipes,
        CancellationToken cancellationToken)
    {
        if (!UseDatabase || millNo is < 1 or > 4 || fileNdtPipes < 0)
            return null;

        var normalized = InputSlitCsvParsing.NormalizePo(poNumber);
        var threshold = Math.Max(0, Opt.PlcCsvDiscrepancyReviewThresholdPercent);

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var tx = (Microsoft.Data.SqlClient.SqlTransaction)
                await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

            string bundleNo;
            int target;
            int filled;
            // PrintedAt is NOT NULL (schema default + pending-print upsert). PrintFailed never clears it.
            // Bundle_No tiebreak keeps same-tick closes deterministic.
            await using (var find = new Microsoft.Data.SqlClient.SqlCommand(ICsvFillService.StampFindIncompleteSql, conn, tx))
            {
                find.Parameters.AddWithValue("@MillNo", millNo);
                find.Parameters.AddWithValue("@Po", poNumber.Trim());
                find.Parameters.AddWithValue("@PoNormalized", normalized);
                await using var reader = await find.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    await tx.RollbackAsync(cancellationToken).ConfigureAwait(false);
                    return null;
                }

                bundleNo = reader.GetString(0);
                target = reader.GetInt32(1);
                filled = reader.GetInt32(2);
            }

            var result = CsvFillLogic.ComputeAfterStamp(bundleNo, target, filled, fileNdtPipes, threshold);

            await using (var upd = new Microsoft.Data.SqlClient.SqlCommand(@"
UPDATE dbo.NDT_Bundle
SET Csv_Filled = @Filled,
    Csv_Fill_State = @State,
    Csv_Last_Row_AtUtc = SYSUTCDATETIME(),
    Count_Discrepancy = CASE WHEN @Discrepancy = 1 THEN 1 ELSE Count_Discrepancy END,
    Manual_Review = CASE WHEN @ManualReview = 1 THEN 1 ELSE Manual_Review END,
    Awaiting_Csv_Recon = 0
WHERE Bundle_No = @BundleNo;", conn, tx))
            {
                upd.Parameters.AddWithValue("@Filled", result.CsvFilledAfter);
                upd.Parameters.AddWithValue("@State", result.FillState);
                upd.Parameters.AddWithValue("@Discrepancy", result.CountDiscrepancy ? 1 : 0);
                upd.Parameters.AddWithValue("@ManualReview", result.ManualReviewEscalated ? 1 : 0);
                upd.Parameters.AddWithValue("@BundleNo", bundleNo);
                await upd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);

            if (result.ManualReviewEscalated)
            {
                _logger.LogWarning(
                    "CSV fill discrepancy exceeds review threshold for bundle {BundleNo}: target={Target} filled={Filled} thresholdPercent={Threshold}. Manual_Review set.",
                    bundleNo,
                    target,
                    result.CsvFilledAfter,
                    threshold);
            }
            else if (result.CountDiscrepancy)
            {
                _logger.LogWarning(
                    "CSV fill overshoot for bundle {BundleNo}: target={Target} filled={Filled}.",
                    bundleNo,
                    target,
                    result.CsvFilledAfter);
            }
            else
            {
                _logger.LogInformation(
                    "CSV fill stamp for bundle {BundleNo}: +{Delta} → filled={Filled}/{Target} state={State}.",
                    bundleNo,
                    fileNdtPipes,
                    result.CsvFilledAfter,
                    target,
                    result.FillState);
            }

            return result;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "TryStampFile failed for PO {PO} Mill {Mill}.", normalized, millNo);
            return null;
        }
    }

    public async Task<int> AdvanceQuietShortAsync(
        string? poNumber,
        int? millNo,
        int quietMinutes,
        DateTime utcNow,
        bool forcePoEnd,
        CancellationToken cancellationToken)
    {
        if (!UseDatabase)
            return 0;

        var threshold = Math.Max(0, Opt.PlcCsvDiscrepancyReviewThresholdPercent);
        var quiet = Math.Max(1, quietMinutes);

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            const string listSql = @"
SELECT Bundle_No, COALESCE(Target_Ndt_Pcs, Total_NDT_Pcs), Csv_Filled, Csv_Last_Row_AtUtc, PrintedAt
FROM dbo.NDT_Bundle
WHERE Target_Ndt_Pcs IS NOT NULL
  AND Csv_Fill_State IN (N'PlcClosed', N'CsvFilling')
  AND ISNULL(Voided, 0) = 0
  AND (@MillNo IS NULL OR Mill_No = @MillNo)
  AND (@Po IS NULL OR PO_Number = @Po OR PO_Number = @PoNormalized);";
            await using var list = new Microsoft.Data.SqlClient.SqlCommand(listSql, conn);
            list.Parameters.AddWithValue("@MillNo", millNo.HasValue ? millNo.Value : (object)DBNull.Value);
            list.Parameters.AddWithValue(
                "@Po",
                string.IsNullOrWhiteSpace(poNumber) ? (object)DBNull.Value : poNumber.Trim());
            list.Parameters.AddWithValue(
                "@PoNormalized",
                string.IsNullOrWhiteSpace(poNumber)
                    ? (object)DBNull.Value
                    : InputSlitCsvParsing.NormalizePo(poNumber));

            var candidates = new List<(string BundleNo, int Target, int Filled, DateTime? LastRow, DateTime? Printed)>();
            await using (var reader = await list.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    candidates.Add((
                        reader.GetString(0),
                        reader.GetInt32(1),
                        reader.GetInt32(2),
                        reader.IsDBNull(3) ? null : reader.GetDateTime(3),
                        reader.IsDBNull(4) ? null : reader.GetDateTime(4)));
                }
            }

            var advanced = 0;
            foreach (var c in candidates)
            {
                if (!forcePoEnd)
                {
                    var anchor = c.LastRow ?? c.Printed ?? utcNow;
                    if ((utcNow - anchor).TotalMinutes < quiet)
                        continue;
                }

                var (state, discrepancy, manual) = CsvFillLogic.ComputeQuietShort(c.Target, c.Filled, threshold);
                await using var upd = new Microsoft.Data.SqlClient.SqlCommand(@"
UPDATE dbo.NDT_Bundle
SET Csv_Fill_State = @State,
    Count_Discrepancy = CASE WHEN @Discrepancy = 1 THEN 1 ELSE Count_Discrepancy END,
    Manual_Review = CASE WHEN @ManualReview = 1 THEN 1 ELSE Manual_Review END,
    Awaiting_Csv_Recon = 0
WHERE Bundle_No = @BundleNo
  AND Csv_Fill_State IN (N'PlcClosed', N'CsvFilling')
  AND ISNULL(Voided, 0) = 0;", conn);
                upd.Parameters.AddWithValue("@State", state);
                upd.Parameters.AddWithValue("@Discrepancy", discrepancy ? 1 : 0);
                upd.Parameters.AddWithValue("@ManualReview", manual ? 1 : 0);
                upd.Parameters.AddWithValue("@BundleNo", c.BundleNo);
                if (await upd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) > 0)
                {
                    advanced++;
                    _logger.LogWarning(
                        "CSV fill advanced CsvShort for bundle {BundleNo}: filled={Filled} target={Target} forcePoEnd={Force}.",
                        c.BundleNo,
                        c.Filled,
                        c.Target,
                        forcePoEnd);
                }
            }

            return advanced;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "AdvanceQuietShort failed.");
            return 0;
        }
    }

    public async Task UpsertHoldAsync(
        string sourceFileName,
        string poNumber,
        int millNo,
        string? pipeSize,
        string reasonCode,
        CancellationToken cancellationToken)
    {
        if (!UseDatabase || string.IsNullOrWhiteSpace(sourceFileName))
            return;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            const string sql = @"
MERGE dbo.NDT_Csv_Fill_Hold AS t
USING (SELECT @File AS Source_File_Name) AS s
ON t.Source_File_Name = s.Source_File_Name
WHEN MATCHED THEN
    UPDATE SET PO_Number = @Po, Mill_No = @Mill, Pipe_Size = @Size, Reason_Code = @Reason, Held_AtUtc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN
    INSERT (Source_File_Name, PO_Number, Mill_No, Pipe_Size, Reason_Code)
    VALUES (@File, @Po, @Mill, @Size, @Reason);";
            await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@File", Path.GetFileName(sourceFileName));
            cmd.Parameters.AddWithValue("@Po", InputSlitCsvParsing.NormalizePo(poNumber));
            cmd.Parameters.AddWithValue("@Mill", millNo);
            cmd.Parameters.AddWithValue("@Size", (object?)pipeSize ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Reason", reasonCode);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "UpsertHold failed for {File} (run docs/NDT_Bundle_Alter_CsvFill.sql).", sourceFileName);
        }
    }

    public async Task<int> EscalateExpiredHoldsAsync(
        int quietMinutes,
        DateTime utcNow,
        CancellationToken cancellationToken,
        int? millNo = null)
    {
        if (!UseDatabase)
            return 0;

        var quiet = Math.Max(1, quietMinutes);
        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            const string sql = @"
UPDATE dbo.NDT_Csv_Fill_Hold
SET Manual_Review = 1,
    Reason_Code = N'HoldQuietExpired'
WHERE Manual_Review = 0
  AND Held_AtUtc <= DATEADD(MINUTE, -@Quiet, @Now)
  AND (@MillNo IS NULL OR Mill_No = @MillNo);";
            await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@Quiet", quiet);
            cmd.Parameters.AddWithValue("@Now", utcNow);
            cmd.Parameters.AddWithValue("@MillNo", millNo.HasValue ? millNo.Value : (object)DBNull.Value);
            var n = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            if (n > 0)
            {
                _logger.LogWarning(
                    "Escalated {Count} held slit file(s) to Manual_Review after {Quiet} quiet minutes.",
                    n,
                    quiet);
            }

            return n;
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "EscalateExpiredHolds failed (table may be missing).");
            return 0;
        }
    }

    public async Task ApplyCountRevisionAsync(
        string sourceFileName,
        string batchNo,
        int oldNdtPipes,
        int newNdtPipes,
        CancellationToken cancellationToken)
    {
        if (!UseDatabase || string.IsNullOrWhiteSpace(batchNo))
            return;

        var delta = newNdtPipes - oldNdtPipes;
        if (delta == 0)
            return;

        var threshold = Math.Max(0, Opt.PlcCsvDiscrepancyReviewThresholdPercent);
        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var tx = (Microsoft.Data.SqlClient.SqlTransaction)
                await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

            int target;
            int filled;
            await using (var find = new Microsoft.Data.SqlClient.SqlCommand(@"
SELECT COALESCE(Target_Ndt_Pcs, Total_NDT_Pcs), Csv_Filled
FROM dbo.NDT_Bundle WITH (UPDLOCK, ROWLOCK)
WHERE Bundle_No = @Batch;", conn, tx))
            {
                find.Parameters.AddWithValue("@Batch", batchNo.Trim());
                await using var reader = await find.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    await tx.RollbackAsync(cancellationToken).ConfigureAwait(false);
                    return;
                }

                target = reader.GetInt32(0);
                filled = reader.GetInt32(1);
            }

            var (after, state, discrepancy, manual) =
                CsvFillLogic.ApplyFilledDelta(target, filled, delta, threshold);

            await using (var upd = new Microsoft.Data.SqlClient.SqlCommand(@"
UPDATE dbo.NDT_Bundle
SET Csv_Filled = @Filled,
    Csv_Fill_State = @State,
    Count_Discrepancy = CASE WHEN @Discrepancy = 1 THEN 1 ELSE Count_Discrepancy END,
    Manual_Review = CASE WHEN @ManualReview = 1 THEN 1 ELSE Manual_Review END
WHERE Bundle_No = @Batch;", conn, tx))
            {
                upd.Parameters.AddWithValue("@Filled", after);
                upd.Parameters.AddWithValue("@State", state);
                upd.Parameters.AddWithValue("@Discrepancy", discrepancy ? 1 : 0);
                upd.Parameters.AddWithValue("@ManualReview", manual ? 1 : 0);
                upd.Parameters.AddWithValue("@Batch", batchNo.Trim());
                await upd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await InsertAuditAsync(
                    conn,
                    tx,
                    Guid.NewGuid(),
                    "CountRevision",
                    sourceFileName,
                    batchNo,
                    batchNo,
                    delta,
                    cancellationToken)
                .ConfigureAwait(false);

            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            _logger.LogInformation(
                "CSV fill count revision for {File} batch {Batch}: delta={Delta} filled={Filled}/{Target} state={State}.",
                Path.GetFileName(sourceFileName),
                batchNo,
                delta,
                after,
                target,
                state);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "ApplyCountRevision failed for {Batch}.", batchNo);
        }
    }

    public async Task<Guid> ApplyBatchMoveAsync(
        string sourceFileName,
        string oldBatchNo,
        string newBatchNo,
        int ndtPipes,
        CancellationToken cancellationToken)
    {
        var correlationId = Guid.NewGuid();
        if (!UseDatabase || string.IsNullOrWhiteSpace(oldBatchNo) || string.IsNullOrWhiteSpace(newBatchNo))
            return correlationId;

        if (string.Equals(oldBatchNo.Trim(), newBatchNo.Trim(), StringComparison.OrdinalIgnoreCase))
            return correlationId;

        var threshold = Math.Max(0, Opt.PlcCsvDiscrepancyReviewThresholdPercent);
        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var tx = (Microsoft.Data.SqlClient.SqlTransaction)
                await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await AdjustFilledInTxAsync(conn, tx, oldBatchNo.Trim(), -ndtPipes, threshold, cancellationToken)
                    .ConfigureAwait(false);
                await AdjustFilledInTxAsync(conn, tx, newBatchNo.Trim(), ndtPipes, threshold, cancellationToken)
                    .ConfigureAwait(false);

                var fileBase = Path.GetFileName(sourceFileName);
                await using (var updRows = new Microsoft.Data.SqlClient.SqlCommand(
                    ICsvFillService.OutputSlitRowBatchMoveSql, conn, tx))
                {
                    updRows.Parameters.AddWithValue("@NewBatch", newBatchNo.Trim());
                    updRows.Parameters.AddWithValue("@OldBatch", oldBatchNo.Trim());
                    updRows.Parameters.AddWithValue("@File", fileBase);
                    updRows.Parameters.AddWithValue("@LikeWin", @"%\" + fileBase);
                    updRows.Parameters.AddWithValue("@LikeUnix", "%/" + fileBase);
                    await updRows.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                await InsertAuditAsync(
                        conn,
                        tx,
                        correlationId,
                        "BatchMove",
                        sourceFileName,
                        oldBatchNo,
                        newBatchNo,
                        ndtPipes,
                        cancellationToken)
                    .ConfigureAwait(false);

                await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                try
                {
                    await tx.RollbackAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (Exception rollbackEx)
                {
                    _logger.LogError(
                        rollbackEx,
                        "ApplyBatchMove rollback failed CorrelationId={CorrelationId}.",
                        correlationId);
                }

                throw;
            }

            _logger.LogWarning(
                "CSV fill batch move CorrelationId={CorrelationId} file={File}: {OldBatch} → {NewBatch} pipes={Pipes}.",
                correlationId,
                Path.GetFileName(sourceFileName),
                oldBatchNo,
                newBatchNo,
                ndtPipes);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ApplyBatchMove failed CorrelationId={CorrelationId}.", correlationId);
            throw;
        }

        return correlationId;
    }

    public async Task<bool> HasAwaitingCsvReconRowsAsync(CancellationToken cancellationToken, int? millNo = null)
    {
        if (!UseDatabase)
            return false;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(
                @"SELECT TOP 1 1 FROM dbo.NDT_Bundle
WHERE Awaiting_Csv_Recon = 1
  AND (@MillNo IS NULL OR Mill_No = @MillNo);",
                conn);
            cmd.Parameters.AddWithValue("@MillNo", millNo.HasValue ? millNo.Value : (object)DBNull.Value);
            var scalar = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return scalar is not null && scalar is not DBNull;
        }
        catch
        {
            return false;
        }
    }

    public async Task<bool> HasBundlesMissingFillTargetAsync(CancellationToken cancellationToken, int? millNo = null)
    {
        if (!UseDatabase)
            return false;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
            // Incomplete fill slots must have a target after schema + quiet drain.
            await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(
                @"SELECT TOP 1 1
FROM dbo.NDT_Bundle
WHERE Target_Ndt_Pcs IS NULL
  AND Total_NDT_Pcs > 0
  AND ISNULL(Voided, 0) = 0
  AND Csv_Fill_State IN (N'PlcClosed', N'CsvFilling')
  AND (@MillNo IS NULL OR Mill_No = @MillNo);",
                conn);
            cmd.Parameters.AddWithValue("@MillNo", millNo.HasValue ? millNo.Value : (object)DBNull.Value);
            var scalar = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return scalar is not null && scalar is not DBNull;
        }
        catch
        {
            // Column missing / SQL unavailable: do not block cutover check here (Awaiting check already ran);
            // Production must apply NDT_Bundle_Alter_CsvFill.sql before start.
            return false;
        }
    }

    private static async Task AdjustFilledInTxAsync(
        Microsoft.Data.SqlClient.SqlConnection conn,
        Microsoft.Data.SqlClient.SqlTransaction tx,
        string batchNo,
        int delta,
        int threshold,
        CancellationToken cancellationToken)
    {
        int target;
        int filled;
        await using (var find = new Microsoft.Data.SqlClient.SqlCommand(@"
SELECT COALESCE(Target_Ndt_Pcs, Total_NDT_Pcs), Csv_Filled
FROM dbo.NDT_Bundle WITH (UPDLOCK, ROWLOCK)
WHERE Bundle_No = @Batch;", conn, tx))
        {
            find.Parameters.AddWithValue("@Batch", batchNo);
            await using var reader = await find.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                return;
            target = reader.GetInt32(0);
            filled = reader.GetInt32(1);
        }

        var (after, state, discrepancy, manual) =
            CsvFillLogic.ApplyFilledDelta(target, filled, delta, threshold);

        await using var upd = new Microsoft.Data.SqlClient.SqlCommand(@"
UPDATE dbo.NDT_Bundle
SET Csv_Filled = @Filled,
    Csv_Fill_State = @State,
    Count_Discrepancy = CASE WHEN @Discrepancy = 1 THEN 1 ELSE Count_Discrepancy END,
    Manual_Review = CASE WHEN @ManualReview = 1 THEN 1 ELSE Manual_Review END
WHERE Bundle_No = @Batch;", conn, tx);
        upd.Parameters.AddWithValue("@Filled", after);
        upd.Parameters.AddWithValue("@State", state);
        upd.Parameters.AddWithValue("@Discrepancy", discrepancy ? 1 : 0);
        upd.Parameters.AddWithValue("@ManualReview", manual ? 1 : 0);
        upd.Parameters.AddWithValue("@Batch", batchNo);
        await upd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task InsertAuditAsync(
        Microsoft.Data.SqlClient.SqlConnection conn,
        Microsoft.Data.SqlClient.SqlTransaction tx,
        Guid correlationId,
        string eventType,
        string? sourceFile,
        string? oldBatch,
        string? newBatch,
        int? delta,
        CancellationToken cancellationToken)
    {
        try
        {
            await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(@"
INSERT INTO dbo.NDT_Csv_Fill_Audit
    (Correlation_Id, Event_Type, Source_File_Name, Old_Batch_No, New_Batch_No, Pipe_Delta)
VALUES
    (@Cid, @Type, @File, @Old, @New, @Delta);", conn, tx);
            cmd.Parameters.AddWithValue("@Cid", correlationId);
            cmd.Parameters.AddWithValue("@Type", eventType);
            cmd.Parameters.AddWithValue("@File", (object?)Path.GetFileName(sourceFile ?? "") ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Old", (object?)oldBatch ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@New", (object?)newBatch ?? DBNull.Value);
            cmd.Parameters.AddWithValue("@Delta", delta.HasValue ? delta.Value : DBNull.Value);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Audit table may be missing in early deploys; fill mutation already applied.
        }
    }
}
