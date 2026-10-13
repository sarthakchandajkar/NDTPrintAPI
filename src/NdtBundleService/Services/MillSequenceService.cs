using System.Globalization;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

public sealed record MillSequenceSnapshot(
    int MillNo,
    int CurrentSequence,
    int LiveMaxSequence,
    string NextBundleNo,
    string? LiveMaxBundleNo,
    DateTime? UpdatedAtUtc,
    string? UpdatedBy,
    string? Reason);

public sealed record MillSequenceSetResult(
    int MillNo,
    int OldSequence,
    int NewSequence,
    string NextBundleNo,
    string? Warning);

public interface IMillSequenceService
{
    bool IsEnabled { get; }

    Task SeedMissingRowsAsync(CancellationToken cancellationToken);

    Task<IReadOnlyList<MillSequenceSnapshot>> GetSnapshotsAsync(CancellationToken cancellationToken);

    Task<MillSequenceSnapshot?> GetSnapshotAsync(int millNo, CancellationToken cancellationToken);

    /// <summary>Live (non-voided, current-year) max sequence for a mill. 0 when none.</summary>
    Task<int> GetLiveMaxSequenceAsync(int millNo, CancellationToken cancellationToken);

    /// <summary>
    /// <c>UPDATE Current_Sequence = Current_Sequence + 1 OUTPUT</c> on the mill row.
    /// Caller must hold <paramref name="tx"/>.
    /// </summary>
    Task<int> AllocateNextInTxAsync(
        SqlConnection conn,
        SqlTransaction tx,
        int millNo,
        string updatedBy,
        string reason,
        CancellationToken cancellationToken);

    Task<MillSequenceSetResult> SetCurrentSequenceAsync(
        int millNo,
        int currentSequence,
        string reason,
        string updatedBy,
        bool forceBelowLiveMax,
        CancellationToken cancellationToken);

    /// <summary>
    /// Decrement by one when <paramref name="sourceSequence"/> is still the mill's allocated high-water
    /// and no live bundle has a higher sequence. Returns whether rollback happened.
    /// </summary>
    Task<bool> TryRollbackIfHighestInTxAsync(
        SqlConnection conn,
        SqlTransaction tx,
        int millNo,
        int sourceSequence,
        string updatedBy,
        string reason,
        CancellationToken cancellationToken);

    Task EnsureScanDoesNotExceedTableAsync(int millNo, CancellationToken cancellationToken);

    /// <summary>
    /// Increment <c>Mill_Sequence</c>, insert <c>NDT_Bundle</c>, and delete armed
    /// <c>Bundle_Accumulation</c> in one transaction.
    /// When an open fill parent exists for the same PO/mill (<c>CsvAdvance</c>, or any
    /// <c>PlcClosed</c>/<c>CsvFilling</c> forming/placeholder row), claims that bundle instead of
    /// allocating a colliding Bundle_No. Raises <c>Mill_Sequence</c> to at least the claimed
    /// sequence so the next close allocates the following number.
    /// Throws when SQL cannot allocate; does not print or invent a batch number.
    /// </summary>
    Task<(int Sequence, string Formatted, bool ClaimedCsvAdvance)> AllocateAndInsertBundleAsync(
        NdtBundleRecord pending,
        CancellationToken cancellationToken);

    /// <summary>
    /// After CSV Complete/Overshoot: allocate next sequence and insert a stamp-only fill target
    /// (<c>Close_Source=CsvAdvance</c>, no tag print). No-op when another incomplete already exists.
    /// Before opening, voids empty orphan <c>CsvAdvance</c> stamps for other POs on this mill.
    /// </summary>
    Task<(int Sequence, string Formatted)?> TryOpenCsvAdvanceStampTargetAsync(
        string poNumber,
        int millNo,
        int provisionalTargetNdtPcs,
        string? slitNo,
        CancellationToken cancellationToken);

    /// <summary>
    /// Void empty incomplete <c>CsvAdvance</c> forming stamps on this mill that belong to a different PO
    /// than <paramref name="keepPoNumber"/> (phantoms left after PO change). Rolls back
    /// <c>Mill_Sequence</c> when a voided stamp was still the high-water allocation.
    /// </summary>
    Task<int> TryVoidEmptyOrphanCsvAdvanceStampsExceptPoAsync(
        int millNo,
        string keepPoNumber,
        CancellationToken cancellationToken);

    /// <summary>
    /// Read-only: Bundle_No that <see cref="AllocateAndInsertBundleAsync"/> would claim for this PO/mill,
    /// or null when a fresh Mill_Sequence allocate would run instead.
    /// </summary>
    Task<string?> TryPeekClaimableOpenFillBundleNoAsync(
        string poNumber,
        int millNo,
        CancellationToken cancellationToken);
}

public sealed class MillSequenceService : IMillSequenceService
{
    public const int LargeJumpWarnThreshold = 100;

    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly INdtBundleRepository _bundles;
    private readonly INdtBundleRuntimeStateStore _runtimeState;
    private readonly ILogger<MillSequenceService> _logger;

    public MillSequenceService(
        IOptionsMonitor<NdtBundleOptions> options,
        INdtBundleRepository bundles,
        INdtBundleRuntimeStateStore runtimeState,
        ILogger<MillSequenceService> logger)
    {
        _options = options;
        _bundles = bundles;
        _runtimeState = runtimeState;
        _logger = logger;
    }

    private NdtBundleOptions Opt => _options.CurrentValue;
    public bool IsEnabled => SqlTraceabilityConnection.IsSqlEnabled(Opt);

    public async Task<(int Sequence, string Formatted, bool ClaimedCsvAdvance)> AllocateAndInsertBundleAsync(
        NdtBundleRecord pending,
        CancellationToken cancellationToken)
    {
        if (!IsEnabled)
            throw new InvalidOperationException(BundleCloseFailure.AllocateUnavailable);

        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await SqlTraceabilityConnection
            .OpenAsync(conn, _logger, "Mill_Sequence allocate+insert", cancellationToken)
            .ConfigureAwait(false);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var claimed = await TryClaimOpenFillParentInTxAsync(conn, tx, pending, preferredBundleNo: null, cancellationToken)
                .ConfigureAwait(false);
            if (claimed is { } c)
            {
                await EnsureSequenceAtLeastInTxAsync(
                        conn,
                        tx,
                        pending.MillNo,
                        c.Sequence,
                        "BundleCloseClaim",
                        $"Claimed open fill parent {c.Formatted}",
                        cancellationToken)
                    .ConfigureAwait(false);
                await _runtimeState
                    .DeleteArmedSizeInTxAsync(conn, tx, pending.PoNumber, pending.MillNo, cancellationToken)
                    .ConfigureAwait(false);
                await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
                _logger.LogInformation(
                    "PLC close claimed open fill parent {BundleNo} for PO {PO} Mill {Mill} (total={Total}); Mill_Sequence raised to at least {Seq}.",
                    c.Formatted,
                    InputSlitCsvParsing.NormalizePo(pending.PoNumber),
                    pending.MillNo,
                    pending.TotalNdtPcs,
                    c.Sequence);
                return (c.Sequence, c.Formatted, ClaimedCsvAdvance: true);
            }

            var seq = await AllocateNextInTxAsync(
                    conn, tx, pending.MillNo, "BundleClose", "Bundle close", cancellationToken)
                .ConfigureAwait(false);
            var formatted = NdtBundleSequence.Format(seq, pending.MillNo);

            // Sequence lag / race: next Bundle_No already exists as this PO's open fill parent.
            var collisionClaim = await TryClaimOpenFillParentInTxAsync(
                    conn, tx, pending, preferredBundleNo: formatted, cancellationToken)
                .ConfigureAwait(false);
            if (collisionClaim is { } cc)
            {
                await _runtimeState
                    .DeleteArmedSizeInTxAsync(conn, tx, pending.PoNumber, pending.MillNo, cancellationToken)
                    .ConfigureAwait(false);
                await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
                _logger.LogWarning(
                    "PLC close allocated {BundleNo} but row already existed; claimed open fill parent for PO {PO} Mill {Mill} (total={Total}) instead of insert.",
                    cc.Formatted,
                    InputSlitCsvParsing.NormalizePo(pending.PoNumber),
                    pending.MillNo,
                    pending.TotalNdtPcs);
                return (cc.Sequence, cc.Formatted, ClaimedCsvAdvance: true);
            }

            if (await BundleNoExistsInTxAsync(conn, tx, formatted, cancellationToken).ConfigureAwait(false))
            {
                throw new InvalidOperationException(
                    $"Cannot insert NDT_Bundle {formatted}: Bundle_No already exists and is not a claimable open fill parent for PO {InputSlitCsvParsing.NormalizePo(pending.PoNumber)} Mill {pending.MillNo}.");
            }

            pending.BundleNo = formatted;
            await _bundles
                .RecordBundlePendingPrintInTxAsync(conn, tx, pending, cancellationToken)
                .ConfigureAwait(false);
            await _runtimeState
                .DeleteArmedSizeInTxAsync(conn, tx, pending.PoNumber, pending.MillNo, cancellationToken)
                .ConfigureAwait(false);
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return (seq, formatted, ClaimedCsvAdvance: false);
        }
        catch
        {
            try { await tx.RollbackAsync(cancellationToken).ConfigureAwait(false); } catch { /* ignore */ }
            throw;
        }
    }

    public async Task<(int Sequence, string Formatted)?> TryOpenCsvAdvanceStampTargetAsync(
        string poNumber,
        int millNo,
        int provisionalTargetNdtPcs,
        string? slitNo,
        CancellationToken cancellationToken)
    {
        if (!IsEnabled || millNo is < 1 or > 4 || provisionalTargetNdtPcs < 0)
            return null;

        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await SqlTraceabilityConnection
            .OpenAsync(conn, _logger, "Mill_Sequence CSV-advance open", cancellationToken)
            .ConfigureAwait(false);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await VoidEmptyOrphanCsvAdvanceStampsExceptPoInTxAsync(
                    conn, tx, millNo, poNumber, cancellationToken)
                .ConfigureAwait(false);

            // Another incomplete may have appeared (PLC or race); do not skip ahead.
            if (await HasIncompleteFillInTxAsync(conn, tx, poNumber, millNo, cancellationToken).ConfigureAwait(false))
            {
                await tx.RollbackAsync(cancellationToken).ConfigureAwait(false);
                return null;
            }

            var seq = await AllocateNextInTxAsync(
                    conn, tx, millNo, "CsvAdvance", "CSV complete/overshoot stamp advance", cancellationToken)
                .ConfigureAwait(false);
            var formatted = NdtBundleSequence.Format(seq, millNo);
            var pending = new NdtBundleRecord
            {
                BundleNo = formatted,
                PoNumber = poNumber,
                MillNo = millNo,
                TotalNdtPcs = provisionalTargetNdtPcs,
                TargetNdtPcs = provisionalTargetNdtPcs,
                CsvFilled = 0,
                CsvFillState = CsvFillState.PlcClosed,
                CloseSource = BundleCloseSource.CsvAdvance,
                SlitNo = slitNo ?? string.Empty
            };

            // Voided Bundle_No still occupies UQ — revive empty voided row for the new PO
            // (common after linked-PO / orphan CsvAdvance void + sequence rollback).
            if (await TryReviveVoidedCsvAdvanceStampInTxAsync(
                        conn, tx, pending, cancellationToken)
                    .ConfigureAwait(false))
            {
                await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
                _logger.LogInformation(
                    "Revived voided CSV-advance stamp {BundleNo} for PO {PO} Mill {Mill} provisionalTarget={Target} (no tag print).",
                    formatted,
                    InputSlitCsvParsing.NormalizePo(poNumber),
                    millNo,
                    provisionalTargetNdtPcs);
                return (seq, formatted);
            }

            if (await BundleNoExistsInTxAsync(conn, tx, formatted, cancellationToken).ConfigureAwait(false))
            {
                throw new InvalidOperationException(
                    $"Cannot insert NDT_Bundle {formatted}: Bundle_No already exists and is not a voided revive candidate for PO {InputSlitCsvParsing.NormalizePo(poNumber)} Mill {millNo}.");
            }

            await _bundles
                .RecordBundlePendingPrintInTxAsync(conn, tx, pending, cancellationToken)
                .ConfigureAwait(false);
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);

            _logger.LogInformation(
                "Opened CSV-advance stamp target {BundleNo} for PO {PO} Mill {Mill} provisionalTarget={Target} (no tag print).",
                formatted,
                InputSlitCsvParsing.NormalizePo(poNumber),
                millNo,
                provisionalTargetNdtPcs);
            return (seq, formatted);
        }
        catch (Exception ex)
        {
            try { await tx.RollbackAsync(cancellationToken).ConfigureAwait(false); } catch { /* ignore */ }
            _logger.LogWarning(
                ex,
                "Failed to open CSV-advance stamp target for PO {PO} Mill {Mill}.",
                InputSlitCsvParsing.NormalizePo(poNumber),
                millNo);
            return null;
        }
    }

    public async Task<int> TryVoidEmptyOrphanCsvAdvanceStampsExceptPoAsync(
        int millNo,
        string keepPoNumber,
        CancellationToken cancellationToken)
    {
        if (!IsEnabled || millNo is < 1 or > 4 || string.IsNullOrWhiteSpace(keepPoNumber))
            return 0;

        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await SqlTraceabilityConnection
            .OpenAsync(conn, _logger, "Mill_Sequence void orphan CsvAdvance", cancellationToken)
            .ConfigureAwait(false);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var voided = await VoidEmptyOrphanCsvAdvanceStampsExceptPoInTxAsync(
                    conn, tx, millNo, keepPoNumber, cancellationToken)
                .ConfigureAwait(false);
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return voided;
        }
        catch (Exception ex)
        {
            try { await tx.RollbackAsync(cancellationToken).ConfigureAwait(false); } catch { /* ignore */ }
            _logger.LogWarning(
                ex,
                "Failed to void orphan CsvAdvance stamps on Mill {Mill} (keep PO {PO}).",
                millNo,
                InputSlitCsvParsing.NormalizePo(keepPoNumber));
            return 0;
        }
    }

    private async Task<int> VoidEmptyOrphanCsvAdvanceStampsExceptPoInTxAsync(
        SqlConnection conn,
        SqlTransaction tx,
        int millNo,
        string keepPoNumber,
        CancellationToken cancellationToken)
    {
        var keep = InputSlitCsvParsing.NormalizePo(keepPoNumber);
        if (string.IsNullOrWhiteSpace(keep))
            return 0;

        await using var find = new SqlCommand(@"
SELECT Bundle_No
FROM dbo.NDT_Bundle WITH (UPDLOCK, ROWLOCK)
WHERE Mill_No = @MillNo
  AND ISNULL(Voided, 0) = 0
  AND Close_Source = N'CsvAdvance'
  AND Csv_Fill_State IN (N'PlcClosed', N'CsvFilling', N'CsvShort')
  AND COALESCE(Csv_Filled, 0) = 0
  AND PO_Number <> @KeepPo
  AND PO_Number <> @KeepPoNormalized
ORDER BY Bundle_No ASC;", conn, tx);
        find.Parameters.AddWithValue("@MillNo", millNo);
        find.Parameters.AddWithValue("@KeepPo", keepPoNumber.Trim());
        find.Parameters.AddWithValue("@KeepPoNormalized", keep);

        var orphans = new List<(string BundleNo, int Sequence)>();
        await using (var reader = await find.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var bundleNo = reader.IsDBNull(0) ? null : reader.GetString(0);
                if (string.IsNullOrWhiteSpace(bundleNo))
                    continue;
                var trimmed = bundleNo.Trim();
                var seq = NdtBundleSequence.TryParseSequence(trimmed, millNo, out var parsed) ? parsed : 0;
                orphans.Add((trimmed, seq));
            }
        }

        if (orphans.Count == 0)
            return 0;

        const string reason = "Orphan CsvAdvance after new running PO stamp";
        foreach (var (bundleNo, seq) in orphans)
        {
            await using var voidCmd = new SqlCommand(@"
UPDATE dbo.NDT_Bundle
SET Voided = 1,
    Voided_AtUtc = SYSUTCDATETIME(),
    Voided_Reason = @Reason,
    Total_NDT_Pcs = 0,
    Target_Ndt_Pcs = 0,
    Csv_Filled = 0,
    Csv_Fill_State = N'Voided'
WHERE Bundle_No = @BundleNo AND ISNULL(Voided, 0) = 0;", conn, tx);
            voidCmd.Parameters.AddWithValue("@BundleNo", bundleNo);
            voidCmd.Parameters.AddWithValue("@Reason", reason);
            await voidCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            if (seq > 0)
            {
                await TryRollbackIfHighestInTxAsync(
                        conn, tx, millNo, seq, "CsvAdvanceOrphanVoid", reason, cancellationToken)
                    .ConfigureAwait(false);
            }

            _logger.LogInformation(
                "Voided empty orphan CsvAdvance stamp {BundleNo} on Mill {Mill} (keep PO {PO}).",
                bundleNo,
                millNo,
                keep);
        }

        return orphans.Count;
    }

    public async Task<string?> TryPeekClaimableOpenFillBundleNoAsync(
        string poNumber,
        int millNo,
        CancellationToken cancellationToken)
    {
        if (!IsEnabled || millNo is < 1 or > 4 || string.IsNullOrWhiteSpace(poNumber))
            return null;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await SqlTraceabilityConnection
                .OpenAsync(conn, _logger, "Mill_Sequence peek open fill", cancellationToken)
                .ConfigureAwait(false);

            var normalized = InputSlitCsvParsing.NormalizePo(poNumber);
            await using var cmd = new SqlCommand(@"
SELECT TOP 1 Bundle_No
FROM dbo.NDT_Bundle
WHERE Mill_No = @MillNo
  AND (PO_Number = @Po OR PO_Number = @PoNormalized)
  AND Csv_Fill_State IN (N'PlcClosed', N'CsvFilling')
  AND ISNULL(Voided, 0) = 0
ORDER BY
  CASE WHEN Close_Source = N'CsvAdvance' THEN 0 ELSE 1 END,
  Bundle_No ASC;", conn);
            cmd.Parameters.AddWithValue("@MillNo", millNo);
            cmd.Parameters.AddWithValue("@Po", poNumber.Trim());
            cmd.Parameters.AddWithValue("@PoNormalized", normalized);
            var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            return result is string s && !string.IsNullOrWhiteSpace(s) ? s.Trim() : null;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogDebug(
                ex,
                "Peek claimable open fill failed for PO {PO} Mill {Mill}.",
                InputSlitCsvParsing.NormalizePo(poNumber),
                millNo);
            return null;
        }
    }

    private async Task<(int Sequence, string Formatted)?> TryClaimOpenFillParentInTxAsync(
        SqlConnection conn,
        SqlTransaction tx,
        NdtBundleRecord pending,
        string? preferredBundleNo,
        CancellationToken cancellationToken)
    {
        var normalized = InputSlitCsvParsing.NormalizePo(pending.PoNumber);
        const string findSql = @"
SELECT TOP 1 Bundle_No, COALESCE(Csv_Filled, 0)
FROM dbo.NDT_Bundle WITH (UPDLOCK, ROWLOCK)
WHERE Mill_No = @MillNo
  AND (PO_Number = @Po OR PO_Number = @PoNormalized)
  AND Csv_Fill_State IN (N'PlcClosed', N'CsvFilling')
  AND ISNULL(Voided, 0) = 0
  AND (@PreferredBundleNo IS NULL OR Bundle_No = @PreferredBundleNo)
ORDER BY
  CASE WHEN Close_Source = N'CsvAdvance' THEN 0 ELSE 1 END,
  Bundle_No ASC;";

        string? bundleNo = null;
        var csvFilled = 0;
        await using (var find = new SqlCommand(findSql, conn, tx))
        {
            find.Parameters.AddWithValue("@MillNo", pending.MillNo);
            find.Parameters.AddWithValue("@Po", pending.PoNumber.Trim());
            find.Parameters.AddWithValue("@PoNormalized", normalized);
            find.Parameters.AddWithValue(
                "@PreferredBundleNo",
                string.IsNullOrWhiteSpace(preferredBundleNo) ? (object)DBNull.Value : preferredBundleNo.Trim());
            await using var reader = await find.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                return null;

            bundleNo = reader.GetString(0);
            csvFilled = reader.GetInt32(1);
        }

        await ApplyClaimUpdateInTxAsync(conn, tx, pending, bundleNo!, csvFilled, cancellationToken)
            .ConfigureAwait(false);

        if (!NdtBundleSequence.TryParseSequence(bundleNo, pending.MillNo, out var seq) || seq <= 0)
            throw new InvalidOperationException($"Could not parse sequence from claimed bundle {bundleNo}.");

        return (seq, bundleNo!);
    }

    private async Task ApplyClaimUpdateInTxAsync(
        SqlConnection conn,
        SqlTransaction tx,
        NdtBundleRecord pending,
        string bundleNo,
        int csvFilled,
        CancellationToken cancellationToken)
    {
        var target = pending.TotalNdtPcs;
        var (state, discrepancy, manualReview) = CsvFillLogic.ComputeAfterTargetRevision(
            target,
            csvFilled,
            Math.Max(0, Opt.PlcCsvDiscrepancyReviewThresholdPercent));

        const string updSql = @"
UPDATE dbo.NDT_Bundle
SET Total_NDT_Pcs = @Total,
    Target_Ndt_Pcs = @Target,
    Close_Source = N'Plc',
    Awaiting_Csv_Recon = 0,
    Csv_Fill_State = @State,
    Count_Discrepancy = CASE WHEN @Discrepancy = 1 THEN 1 ELSE Count_Discrepancy END,
    Manual_Review = CASE WHEN @ManualReview = 1 THEN 1 ELSE Manual_Review END,
    Context_Slit_No = COALESCE(NULLIF(@SlitNo, N''), Context_Slit_No),
    Slit_Start_Time = COALESCE(@SlitStart, Slit_Start_Time),
    Slit_Finish_Time = COALESCE(@SlitFinish, Slit_Finish_Time),
    Rejected_P = @Rejected,
    NDT_Short_Length_Pipe = @NdtShort,
    Rejected_Short_Length_Pipe = @RejShort,
    Print_Status = N'Pending',
    Print_Attempted_At = SYSDATETIME(),
    Print_Error = NULL,
    PrintedAt = SYSDATETIME(),
    IsReprint = 0
WHERE Bundle_No = @BundleNo;";

        await using var upd = new SqlCommand(updSql, conn, tx);
        upd.Parameters.AddWithValue("@Total", target);
        upd.Parameters.AddWithValue("@Target", target);
        upd.Parameters.AddWithValue("@State", state);
        upd.Parameters.AddWithValue("@Discrepancy", discrepancy ? 1 : 0);
        upd.Parameters.AddWithValue("@ManualReview", manualReview ? 1 : 0);
        upd.Parameters.AddWithValue("@SlitNo", pending.SlitNo ?? string.Empty);
        upd.Parameters.AddWithValue("@SlitStart", (object?)pending.SlitStartTime ?? DBNull.Value);
        upd.Parameters.AddWithValue("@SlitFinish", (object?)pending.SlitFinishTime ?? DBNull.Value);
        upd.Parameters.AddWithValue("@Rejected", pending.RejectedPipes);
        upd.Parameters.AddWithValue("@NdtShort", pending.NdtShortLengthPipe ?? string.Empty);
        upd.Parameters.AddWithValue("@RejShort", pending.RejectedShortLengthPipe ?? string.Empty);
        upd.Parameters.AddWithValue("@BundleNo", bundleNo);
        await upd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task EnsureSequenceAtLeastInTxAsync(
        SqlConnection conn,
        SqlTransaction tx,
        int millNo,
        int claimedSequence,
        string updatedBy,
        string reason,
        CancellationToken cancellationToken)
    {
        if (claimedSequence <= 0 || millNo is < 1 or > 4)
            return;

        int current;
        await using (var lockRow = new SqlCommand(@"
SELECT Current_Sequence FROM dbo.Mill_Sequence WITH (UPDLOCK, ROWLOCK) WHERE Mill_No = @Mill;", conn, tx))
        {
            lockRow.Parameters.AddWithValue("@Mill", millNo);
            var scalar = await lockRow.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (scalar is null or DBNull)
                throw new InvalidOperationException($"Mill_Sequence row missing for mill {millNo}.");
            current = Convert.ToInt32(scalar, CultureInfo.InvariantCulture);
        }

        if (!CsvFillSequenceAdvance.ShouldRaiseMillSequenceToClaimed(current, claimedSequence))
            return;

        await using (var upd = new SqlCommand(@"
UPDATE dbo.Mill_Sequence
SET Current_Sequence = @Seq,
    Updated_AtUtc = SYSUTCDATETIME(),
    Updated_By = @By,
    Reason = @Reason
WHERE Mill_No = @Mill AND Current_Sequence < @Seq;", conn, tx))
        {
            upd.Parameters.AddWithValue("@Seq", claimedSequence);
            upd.Parameters.AddWithValue("@By", Truncate(updatedBy, 128));
            upd.Parameters.AddWithValue("@Reason", Truncate(reason, 512));
            upd.Parameters.AddWithValue("@Mill", millNo);
            await upd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await InsertAuditAsync(
                conn,
                tx,
                millNo,
                current,
                claimedSequence,
                "BundleCloseClaim",
                updatedBy,
                reason,
                cancellationToken)
            .ConfigureAwait(false);
    }

    private static async Task<bool> BundleNoExistsInTxAsync(
        SqlConnection conn,
        SqlTransaction tx,
        string bundleNo,
        CancellationToken cancellationToken)
    {
        await using var cmd = new SqlCommand(@"
SELECT TOP 1 1 FROM dbo.NDT_Bundle WITH (UPDLOCK, ROWLOCK) WHERE Bundle_No = @BundleNo;", conn, tx);
        cmd.Parameters.AddWithValue("@BundleNo", bundleNo);
        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is not null and not DBNull;
    }

    /// <summary>
    /// Re-opens a voided empty row for <paramref name="pending"/>.BundleNo as a fresh CsvAdvance stamp.
    /// Returns false when no voided row exists (caller should INSERT).
    /// </summary>
    private static async Task<bool> TryReviveVoidedCsvAdvanceStampInTxAsync(
        SqlConnection conn,
        SqlTransaction tx,
        NdtBundleRecord pending,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(pending.BundleNo))
            return false;

        await using var cmd = new SqlCommand(@"
UPDATE dbo.NDT_Bundle
SET PO_Number = @Po,
    Mill_No = @Mill,
    Total_NDT_Pcs = @Target,
    Target_Ndt_Pcs = @Target,
    Csv_Filled = 0,
    Csv_Fill_State = N'PlcClosed',
    Close_Source = N'CsvAdvance',
    Context_Slit_No = @Slit,
    Slit_Start_Time = NULL,
    Slit_Finish_Time = NULL,
    Rejected_P = 0,
    PrintedAt = NULL,
    Print_Status = N'Pending',
    Print_Attempted_At = SYSDATETIME(),
    Print_Error = NULL,
    IsReprint = 0,
    Manual_Review = 0,
    Awaiting_Csv_Recon = 0,
    Count_Discrepancy = 0,
    Manual_Recon = 0,
    Voided = 0,
    Voided_AtUtc = NULL,
    Voided_Reason = NULL,
    Csv_Last_Row_AtUtc = NULL
WHERE Bundle_No = @BundleNo
  AND ISNULL(Voided, 0) = 1
  AND COALESCE(Csv_Filled, 0) = 0;", conn, tx);
        cmd.Parameters.AddWithValue("@Po", pending.PoNumber.Trim());
        cmd.Parameters.AddWithValue("@Mill", pending.MillNo);
        cmd.Parameters.AddWithValue("@Target", pending.TargetNdtPcs ?? pending.TotalNdtPcs);
        cmd.Parameters.AddWithValue(
            "@Slit",
            string.IsNullOrWhiteSpace(pending.SlitNo) ? (object)DBNull.Value : pending.SlitNo.Trim());
        cmd.Parameters.AddWithValue("@BundleNo", pending.BundleNo.Trim());
        var n = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        return n > 0;
    }

    private static async Task<bool> HasIncompleteFillInTxAsync(
        SqlConnection conn,
        SqlTransaction tx,
        string poNumber,
        int millNo,
        CancellationToken cancellationToken)
    {
        var normalized = InputSlitCsvParsing.NormalizePo(poNumber);
        await using var cmd = new SqlCommand(@"
SELECT TOP 1 1
FROM dbo.NDT_Bundle WITH (UPDLOCK, ROWLOCK)
WHERE Mill_No = @MillNo
  AND (PO_Number = @Po OR PO_Number = @PoNormalized)
  AND Target_Ndt_Pcs IS NOT NULL
  AND Csv_Fill_State IN (N'PlcClosed', N'CsvFilling')
  AND ISNULL(Voided, 0) = 0;", conn, tx);
        cmd.Parameters.AddWithValue("@MillNo", millNo);
        cmd.Parameters.AddWithValue("@Po", poNumber.Trim());
        cmd.Parameters.AddWithValue("@PoNormalized", normalized);
        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is not null and not DBNull;
    }

    public async Task SeedMissingRowsAsync(CancellationToken cancellationToken)
    {
        if (!IsEnabled)
            return;

        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "Mill_Sequence seed", cancellationToken)
            .ConfigureAwait(false);

        for (var mill = 1; mill <= 4; mill++)
        {
            var exists = false;
            await using (var find = new SqlCommand(
                "SELECT 1 FROM dbo.Mill_Sequence WHERE Mill_No = @Mill;", conn))
            {
                find.Parameters.AddWithValue("@Mill", mill);
                exists = await find.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is not null
                    and not DBNull;
            }

            if (exists)
                continue;

            var scan = await GetLiveMaxSequenceAsync(conn, tx: null, mill, cancellationToken).ConfigureAwait(false);
            var config = ParseInitialSeed(mill);
            var seed = Math.Max(scan, config);

            await using var ins = new SqlCommand(@"
INSERT INTO dbo.Mill_Sequence (Mill_No, Current_Sequence, Updated_By, Reason)
VALUES (@Mill, @Seq, N'StartupSeed', N'Seed missing row from scan/config');", conn);
            ins.Parameters.AddWithValue("@Mill", mill);
            ins.Parameters.AddWithValue("@Seq", seed);
            await ins.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            await InsertAuditAsync(conn, tx: null, mill, oldSeq: null, seed, "Seed", "StartupSeed",
                "Seed missing row", cancellationToken).ConfigureAwait(false);

            _logger.LogInformation(
                "Mill_Sequence mill {Mill} seeded Current_Sequence={Seq} (scan={Scan}, config={Config}).",
                mill, seed, scan, config);
        }
    }

    public async Task EnsureScanDoesNotExceedTableAsync(int millNo, CancellationToken cancellationToken)
    {
        if (!IsEnabled || millNo is < 1 or > 4)
            return;

        var snap = await GetSnapshotAsync(millNo, cancellationToken).ConfigureAwait(false);
        if (snap is null)
            throw new InvalidOperationException(
                $"Mill_Sequence row missing for mill {millNo}. Run docs/Mill_Sequence.sql.");

        if (snap.LiveMaxSequence > snap.CurrentSequence)
        {
            throw new InvalidOperationException(
                $"Mill_Sequence for mill {millNo} is {snap.CurrentSequence}; live bundles go to {snap.LiveMaxSequence}"
                + (string.IsNullOrEmpty(snap.LiveMaxBundleNo) ? "." : $" ({snap.LiveMaxBundleNo}).")
                + " Sequence was allocated outside Mill_Sequence.");
        }
    }

    public async Task<IReadOnlyList<MillSequenceSnapshot>> GetSnapshotsAsync(CancellationToken cancellationToken)
    {
        var list = new List<MillSequenceSnapshot>();
        for (var mill = 1; mill <= 4; mill++)
        {
            var snap = await GetSnapshotAsync(mill, cancellationToken).ConfigureAwait(false);
            if (snap is not null)
                list.Add(snap);
        }

        return list;
    }

    public async Task<MillSequenceSnapshot?> GetSnapshotAsync(int millNo, CancellationToken cancellationToken)
    {
        if (!IsEnabled || millNo is < 1 or > 4)
            return null;

        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "Mill_Sequence read", cancellationToken)
            .ConfigureAwait(false);

        int current = 0;
        DateTime? updated = null;
        string? by = null;
        string? reason = null;
        await using (var cmd = new SqlCommand(@"
SELECT Current_Sequence, Updated_AtUtc, Updated_By, Reason
FROM dbo.Mill_Sequence WHERE Mill_No = @Mill;", conn))
        {
            cmd.Parameters.AddWithValue("@Mill", millNo);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                return null;
            current = reader.GetInt32(0);
            updated = reader.IsDBNull(1) ? null : reader.GetDateTime(1);
            by = reader.IsDBNull(2) ? null : reader.GetString(2);
            reason = reader.IsDBNull(3) ? null : reader.GetString(3);
        }

        var (liveMax, liveBundle) = await GetLiveMaxAsync(conn, tx: null, millNo, cancellationToken).ConfigureAwait(false);
        return new MillSequenceSnapshot(
            millNo,
            current,
            liveMax,
            NdtBundleSequence.Format(current + 1, millNo),
            liveBundle,
            updated,
            by,
            reason);
    }

    public Task<int> GetLiveMaxSequenceAsync(int millNo, CancellationToken cancellationToken)
    {
        if (!IsEnabled || millNo is < 1 or > 4)
            return Task.FromResult(0);

        return GetLiveMaxCoreAsync(millNo, cancellationToken);
    }

    private async Task<int> GetLiveMaxCoreAsync(int millNo, CancellationToken cancellationToken)
    {
        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "Mill_Sequence live max", cancellationToken)
            .ConfigureAwait(false);
        return await GetLiveMaxSequenceAsync(conn, tx: null, millNo, cancellationToken).ConfigureAwait(false);
    }

    public async Task<int> AllocateNextInTxAsync(
        SqlConnection conn,
        SqlTransaction tx,
        int millNo,
        string updatedBy,
        string reason,
        CancellationToken cancellationToken)
    {
        if (millNo is < 1 or > 4)
            throw new ArgumentOutOfRangeException(nameof(millNo));

        await using var cmd = new SqlCommand(@"
UPDATE dbo.Mill_Sequence WITH (UPDLOCK, ROWLOCK)
SET Current_Sequence = Current_Sequence + 1,
    Updated_AtUtc = SYSUTCDATETIME(),
    Updated_By = @By,
    Reason = @Reason
OUTPUT DELETED.Current_Sequence, INSERTED.Current_Sequence
WHERE Mill_No = @Mill;", conn, tx);
        cmd.Parameters.AddWithValue("@Mill", millNo);
        cmd.Parameters.AddWithValue("@By", Truncate(updatedBy, 128));
        cmd.Parameters.AddWithValue("@Reason", Truncate(reason, 512));

        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            throw new InvalidOperationException($"Mill_Sequence row missing for mill {millNo}. Run docs/Mill_Sequence.sql.");

        var oldSeq = reader.GetInt32(0);
        var newSeq = reader.GetInt32(1);
        await reader.CloseAsync().ConfigureAwait(false);

        await InsertAuditAsync(conn, tx, millNo, oldSeq, newSeq, "BundleClose", updatedBy, reason, cancellationToken)
            .ConfigureAwait(false);
        return newSeq;
    }

    public static bool ShouldRefuseBelowLiveMax(int requested, int liveMax, bool forceBelowLiveMax) =>
        requested < liveMax && !forceBelowLiveMax;

    public static bool IsLargeJump(int oldSequence, int newSequence) =>
        Math.Abs(newSequence - oldSequence) >= LargeJumpWarnThreshold;

    public async Task<MillSequenceSetResult> SetCurrentSequenceAsync(
        int millNo,
        int currentSequence,
        string reason,
        string updatedBy,
        bool forceBelowLiveMax,
        CancellationToken cancellationToken)
    {
        if (!IsEnabled)
            throw new InvalidOperationException("Mill_Sequence requires SQL mode.");
        if (millNo is < 1 or > 4)
            throw new ArgumentOutOfRangeException(nameof(millNo));
        if (currentSequence < 0)
            throw new ArgumentOutOfRangeException(nameof(currentSequence));
        if (string.IsNullOrWhiteSpace(reason))
            throw new ArgumentException("Reason is required.", nameof(reason));

        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "Mill_Sequence set", cancellationToken)
            .ConfigureAwait(false);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            int oldSeq;
            await using (var lockRow = new SqlCommand(@"
SELECT Current_Sequence FROM dbo.Mill_Sequence WITH (UPDLOCK, ROWLOCK) WHERE Mill_No = @Mill;", conn, tx))
            {
                lockRow.Parameters.AddWithValue("@Mill", millNo);
                var scalar = await lockRow.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                if (scalar is null or DBNull)
                    throw new InvalidOperationException($"Mill_Sequence row missing for mill {millNo}.");
                oldSeq = Convert.ToInt32(scalar, CultureInfo.InvariantCulture);
            }

            var liveMax = await GetLiveMaxSequenceAsync(conn, tx, millNo, cancellationToken).ConfigureAwait(false);
            if (ShouldRefuseBelowLiveMax(currentSequence, liveMax, forceBelowLiveMax))
            {
                throw new InvalidOperationException(
                    $"Refuse to set mill {millNo} Current_Sequence to {currentSequence}: live bundles go to {liveMax}. "
                    + "Pass forceBelowLiveMax=true with a second confirmation.");
            }

            await using (var upd = new SqlCommand(@"
UPDATE dbo.Mill_Sequence
SET Current_Sequence = @Seq, Updated_AtUtc = SYSUTCDATETIME(), Updated_By = @By, Reason = @Reason
WHERE Mill_No = @Mill;", conn, tx))
            {
                upd.Parameters.AddWithValue("@Mill", millNo);
                upd.Parameters.AddWithValue("@Seq", currentSequence);
                upd.Parameters.AddWithValue("@By", Truncate(updatedBy, 128));
                upd.Parameters.AddWithValue("@Reason", Truncate(reason, 512));
                await upd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await InsertAuditAsync(conn, tx, millNo, oldSeq, currentSequence, "Set", updatedBy, reason, cancellationToken)
                .ConfigureAwait(false);
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);

            string? warning = null;
            if (IsLargeJump(oldSeq, currentSequence))
                warning = $"Large jump: mill {millNo} sequence {oldSeq} → {currentSequence}.";
            if (currentSequence < liveMax)
                warning = (warning is null ? "" : warning + " ")
                    + $"Set below live max {liveMax}; next close may collide with an existing tag.";

            return new MillSequenceSetResult(
                millNo, oldSeq, currentSequence, NdtBundleSequence.Format(currentSequence + 1, millNo), warning);
        }
        catch
        {
            try { await tx.RollbackAsync(cancellationToken).ConfigureAwait(false); } catch { /* ignore */ }
            throw;
        }
    }

    public async Task<bool> TryRollbackIfHighestInTxAsync(
        SqlConnection conn,
        SqlTransaction tx,
        int millNo,
        int sourceSequence,
        string updatedBy,
        string reason,
        CancellationToken cancellationToken)
    {
        int current;
        await using (var lockRow = new SqlCommand(@"
SELECT Current_Sequence FROM dbo.Mill_Sequence WITH (UPDLOCK, ROWLOCK) WHERE Mill_No = @Mill;", conn, tx))
        {
            lockRow.Parameters.AddWithValue("@Mill", millNo);
            var scalar = await lockRow.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (scalar is null or DBNull)
                return false;
            current = Convert.ToInt32(scalar, CultureInfo.InvariantCulture);
        }

        if (current != sourceSequence)
            return false;

        var liveMax = await GetLiveMaxSequenceAsync(conn, tx, millNo, cancellationToken).ConfigureAwait(false);
        if (liveMax > sourceSequence)
            return false;

        var next = Math.Max(0, sourceSequence - 1);
        await using (var upd = new SqlCommand(@"
UPDATE dbo.Mill_Sequence
SET Current_Sequence = @Seq, Updated_AtUtc = SYSUTCDATETIME(), Updated_By = @By, Reason = @Reason
WHERE Mill_No = @Mill AND Current_Sequence = @Old;", conn, tx))
        {
            upd.Parameters.AddWithValue("@Seq", next);
            upd.Parameters.AddWithValue("@By", Truncate(updatedBy, 128));
            upd.Parameters.AddWithValue("@Reason", Truncate(reason, 512));
            upd.Parameters.AddWithValue("@Mill", millNo);
            upd.Parameters.AddWithValue("@Old", sourceSequence);
            if (await upd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false) <= 0)
                return false;
        }

        await InsertAuditAsync(conn, tx, millNo, sourceSequence, next, "MergeRollback", updatedBy, reason, cancellationToken)
            .ConfigureAwait(false);
        return true;
    }

    private int ParseInitialSeed(int millNo)
    {
        var seeds = Opt.InitialMillBatchNumbers;
        if (seeds is null || !seeds.TryGetValue(millNo.ToString(CultureInfo.InvariantCulture), out var raw))
            return 0;

        var batchNo = (raw ?? string.Empty).Trim();
        if (string.IsNullOrEmpty(batchNo))
            return 0;

        if (NdtBundleSequence.TryParseSequence(batchNo, millNo, out var seq))
            return seq;
        if (int.TryParse(batchNo, NumberStyles.None, CultureInfo.InvariantCulture, out seq) && seq >= 0)
            return seq;
        return 0;
    }

    private static async Task<int> GetLiveMaxSequenceAsync(
        SqlConnection conn,
        SqlTransaction? tx,
        int millNo,
        CancellationToken cancellationToken)
    {
        var (max, _) = await GetLiveMaxAsync(conn, tx, millNo, cancellationToken).ConfigureAwait(false);
        return max;
    }

    private static async Task<(int Max, string? BundleNo)> GetLiveMaxAsync(
        SqlConnection conn,
        SqlTransaction? tx,
        int millNo,
        CancellationToken cancellationToken)
    {
        await using var cmd = new SqlCommand(@"
SELECT Bundle_No
FROM dbo.NDT_Bundle
WHERE Mill_No = @Mill
  AND ISNULL(Voided, 0) = 0
  AND Total_NDT_Pcs > 0;", conn, tx);
        cmd.Parameters.AddWithValue("@Mill", millNo);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var max = 0;
        string? bundle = null;
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var no = reader.IsDBNull(0) ? "" : reader.GetString(0);
            if (!NdtBundleSequence.TryParseSequenceForCurrentYear(no, millNo, out var seq) || seq <= max)
                continue;
            max = seq;
            bundle = no;
        }

        return (max, bundle);
    }

    private static async Task InsertAuditAsync(
        SqlConnection conn,
        SqlTransaction? tx,
        int millNo,
        int? oldSeq,
        int newSeq,
        string eventType,
        string updatedBy,
        string reason,
        CancellationToken cancellationToken)
    {
        try
        {
            await using var cmd = new SqlCommand(@"
INSERT INTO dbo.Mill_Sequence_Audit (Mill_No, Old_Sequence, New_Sequence, Event_Type, Updated_By, Reason)
VALUES (@Mill, @Old, @New, @Type, @By, @Reason);", conn, tx);
            cmd.Parameters.AddWithValue("@Mill", millNo);
            cmd.Parameters.AddWithValue("@Old", oldSeq.HasValue ? oldSeq.Value : DBNull.Value);
            cmd.Parameters.AddWithValue("@New", newSeq);
            cmd.Parameters.AddWithValue("@Type", eventType);
            cmd.Parameters.AddWithValue("@By", Truncate(updatedBy, 128));
            cmd.Parameters.AddWithValue("@Reason", Truncate(reason, 512));
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // Audit table may be missing until docs/Mill_Sequence.sql is applied.
        }
    }

    private static string Truncate(string value, int max)
    {
        var s = (value ?? string.Empty).Trim();
        return s.Length <= max ? s : s[..max];
    }
}
