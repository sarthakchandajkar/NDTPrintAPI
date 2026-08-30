using System.Globalization;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

public sealed record BundleMergePreview(
    string SourceBundleNo,
    string TargetBundleNo,
    int SourcePcs,
    int TargetPcs,
    int ResultingTotal,
    string SequenceMessage,
    bool SequenceWillRollback,
    int AcceptedFileCount,
    string? PipeSize);

public sealed record BundleMergeResult(
    string SourceBundleNo,
    string TargetBundleNo,
    string TombstoneBundleNo,
    int SourcePcs,
    int ResultingTotal,
    bool SequenceRolledBack,
    string SequenceMessage,
    int PpcItemsCreated,
    bool PrintSuccess,
    string? PrintMessage);

public interface IBundleMergeService
{
    Task<BundleMergePreview?> TryPreviewAsync(string sourceBundleNo, CancellationToken cancellationToken);

    Task<BundleMergeResult> MergeIntoPreviousAsync(
        string sourceBundleNo,
        string reason,
        string updatedBy,
        CancellationToken cancellationToken);
}

public sealed class BundleMergeService : IBundleMergeService
{
    /// <summary>
    /// Reassign FK/logical children BEFORE renaming NDT_Bundle.Bundle_No.
    /// Test: BundleMergeTests.Tombstone_sql_rewrites_child_tables_before_bundle_no_rename
    /// </summary>
    public const string ReassignOutputSlitSql = @"
UPDATE dbo.Output_Slit_Row
SET NDT_Batch_No = @Target
WHERE NDT_Batch_No = @Source;";

    public const string ReassignManualStationSql = @"
UPDATE dbo.Manual_Station_Run
SET NDT_Batch_No = @Target
WHERE NDT_Batch_No = @Source;";

    public const string ReassignProcessSql = @"
UPDATE dbo.NDT_Process_Consolidated
SET NDT_Batch_No = @Target
WHERE NDT_Batch_No = @Source;";

    public const string ReassignUploadSql = @"
UPDATE dbo.Upload_Bundle_Row
SET Bundle_Number = @Target
WHERE Bundle_Number = @Source;";

    public const string ReassignPipelineSql = @"
UPDATE dbo.Pipeline_Event
SET Bundle_No = @Tombstone
WHERE Bundle_No = @Source;";

    public const string ReassignPpcSql = @"
UPDATE dbo.Ppc_Correction_Item
SET NDT_Batch_No = @Tombstone,
    Replacement_NDT_Batch_No = COALESCE(Replacement_NDT_Batch_No, @Target)
WHERE NDT_Batch_No = @Source;";

    public const string TombstoneBundleSql = @"
UPDATE dbo.NDT_Bundle
SET Bundle_No = @Tombstone,
    Original_Bundle_No = @Source,
    Merged_Into_Bundle_No = @Target,
    Voided = 1,
    Voided_AtUtc = SYSUTCDATETIME(),
    Voided_Reason = @Reason,
    Total_NDT_Pcs = 0,
    Target_Ndt_Pcs = 0,
    Csv_Filled = 0,
    Csv_Fill_State = N'Voided'
WHERE Bundle_No = @Source AND ISNULL(Voided, 0) = 0;";

    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly INdtBundleRepository _bundles;
    private readonly IMillSequenceService _millSequence;
    private readonly IPpcCorrectionRepository _ppc;
    private readonly IOutputSlitSapStatusRepository _sapStatus;
    private readonly IReconcileBundleTagService _reprint;
    private readonly IPipeSizeProvider _pipeSize;
    private readonly ILogger<BundleMergeService> _logger;

    public BundleMergeService(
        IOptionsMonitor<NdtBundleOptions> options,
        INdtBundleRepository bundles,
        IMillSequenceService millSequence,
        IPpcCorrectionRepository ppc,
        IOutputSlitSapStatusRepository sapStatus,
        IReconcileBundleTagService reprint,
        IPipeSizeProvider pipeSize,
        ILogger<BundleMergeService> logger)
    {
        _options = options;
        _bundles = bundles;
        _millSequence = millSequence;
        _ppc = ppc;
        _sapStatus = sapStatus;
        _reprint = reprint;
        _pipeSize = pipeSize;
        _logger = logger;
    }

    private NdtBundleOptions Opt => _options.CurrentValue;

    public async Task<BundleMergePreview?> TryPreviewAsync(string sourceBundleNo, CancellationToken cancellationToken)
    {
        var source = await _bundles.GetByBatchNoAsync(sourceBundleNo, cancellationToken).ConfigureAwait(false);
        if (source is null || source.Voided)
            return null;

        var target = await FindPreviousAsync(source, cancellationToken).ConfigureAwait(false);
        if (target is null)
            return null;

        if (!NdtBundleSequence.TryParseSequenceForCurrentYear(source.BundleNo, source.MillNo, out var sourceSeq))
            return null;

        var snap = await _millSequence.GetSnapshotAsync(source.MillNo, cancellationToken).ConfigureAwait(false);
        var millCurrent = snap?.CurrentSequence ?? sourceSeq;
        var liveMaxExcl = snap is null ? 0 : (snap.LiveMaxSequence == sourceSeq ? 0 : snap.LiveMaxSequence);
        var rollback = BundleMergeLogic.ShouldRollbackSequence(sourceSeq, millCurrent, liveMaxExcl);
        var millAfter = rollback ? Math.Max(0, millCurrent - 1) : millCurrent;

        var files = await _bundles.GetOutputSourceFilesForBatchAsync(source.BundleNo, cancellationToken)
            .ConfigureAwait(false);
        var accepted = 0;
        foreach (var file in files)
        {
            if (await IsAcceptedAsync(file, cancellationToken).ConfigureAwait(false))
                accepted++;
        }

        var pipeSize = await _pipeSize.TryGetPipeSizeForPoAsync(source.PoNumber, cancellationToken).ConfigureAwait(false);

        return new BundleMergePreview(
            source.BundleNo,
            target.BundleNo,
            source.TotalNdtPcs,
            target.TotalNdtPcs,
            source.TotalNdtPcs + target.TotalNdtPcs,
            BundleMergeLogic.SequenceMessage(source.BundleNo, source.MillNo, rollback, millAfter),
            rollback,
            accepted,
            pipeSize);
    }

    public async Task<BundleMergeResult> MergeIntoPreviousAsync(
        string sourceBundleNo,
        string reason,
        string updatedBy,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(reason))
            throw new ArgumentException("Reason is required.", nameof(reason));
        if (!SqlTraceabilityConnection.IsSqlEnabled(Opt))
            throw new InvalidOperationException("Bundle merge requires SQL mode.");

        var source = await _bundles.GetByBatchNoAsync(sourceBundleNo, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException($"Bundle {sourceBundleNo} not found.");
        if (source.Voided)
            throw new InvalidOperationException($"Bundle {sourceBundleNo} is already voided.");
        if (source.TotalNdtPcs <= 0)
            throw new InvalidOperationException($"Bundle {sourceBundleNo} has no pipes to merge.");

        var target = await FindPreviousAsync(source, cancellationToken).ConfigureAwait(false)
            ?? throw new InvalidOperationException(
                $"No immediately previous live bundle for PO {source.PoNumber} mill {source.MillNo}.");

        if (!NdtBundleSequence.TryParseSequenceForCurrentYear(source.BundleNo, source.MillNo, out var sourceSeq))
            throw new InvalidOperationException($"Cannot parse sequence from {source.BundleNo}.");

        var tombstone = BundleMergeLogic.Tombstone(source.BundleNo);
        var threshold = Math.Max(0, Opt.PlcCsvDiscrepancyReviewThresholdPercent);
        var sourceFiles = (await _bundles.GetOutputSourceFilesForBatchAsync(source.BundleNo, cancellationToken)
            .ConfigureAwait(false)).ToList();

        var rolledBack = false;
        var millAfter = 0;
        await using (var conn = SqlTraceabilityConnection.Create(Opt))
        {
            await SqlTraceabilityConnection.OpenAsync(conn, _logger, "bundle merge", cancellationToken)
                .ConfigureAwait(false);
            await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await using (var lockSeq = new SqlCommand(
                    "SELECT Current_Sequence FROM dbo.Mill_Sequence WITH (UPDLOCK, ROWLOCK) WHERE Mill_No = @Mill;",
                    conn, tx))
                {
                    lockSeq.Parameters.AddWithValue("@Mill", source.MillNo);
                    _ = await lockSeq.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                }

                await ExecAsync(conn, tx, ReassignOutputSlitSql, source.BundleNo, target.BundleNo, tombstone, reason, cancellationToken)
                    .ConfigureAwait(false);
                await TryExecAsync(conn, tx, ReassignManualStationSql, source.BundleNo, target.BundleNo, tombstone, reason, cancellationToken)
                    .ConfigureAwait(false);
                await TryExecAsync(conn, tx, ReassignProcessSql, source.BundleNo, target.BundleNo, tombstone, reason, cancellationToken)
                    .ConfigureAwait(false);
                await TryExecAsync(conn, tx, ReassignUploadSql, source.BundleNo, target.BundleNo, tombstone, reason, cancellationToken)
                    .ConfigureAwait(false);
                await TryExecAsync(conn, tx, ReassignPipelineSql, source.BundleNo, target.BundleNo, tombstone, reason, cancellationToken)
                    .ConfigureAwait(false);
                await TryExecAsync(conn, tx, ReassignPpcSql, source.BundleNo, target.BundleNo, tombstone, reason, cancellationToken)
                    .ConfigureAwait(false);

                var fill = BundleMergeLogic.ComputeTargetFillAfterMerge(
                    target.TotalNdtPcs,
                    target.TargetNdtPcs ?? target.TotalNdtPcs,
                    target.CsvFilled,
                    source.TotalNdtPcs,
                    source.CsvFilled,
                    threshold);

                await using (var updTarget = new SqlCommand(@"
UPDATE dbo.NDT_Bundle
SET Total_NDT_Pcs = @Total,
    Target_Ndt_Pcs = @TargetPcs,
    Csv_Filled = @Filled,
    Csv_Fill_State = @State
WHERE Bundle_No = @Target AND ISNULL(Voided, 0) = 0;", conn, tx))
                {
                    updTarget.Parameters.AddWithValue("@Total", fill.Total);
                    updTarget.Parameters.AddWithValue("@TargetPcs", fill.Target);
                    updTarget.Parameters.AddWithValue("@Filled", fill.Filled);
                    updTarget.Parameters.AddWithValue("@State", fill.FillState);
                    updTarget.Parameters.AddWithValue("@Target", target.BundleNo);
                    await updTarget.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                await ExecAsync(conn, tx, TombstoneBundleSql, source.BundleNo, target.BundleNo, tombstone, reason, cancellationToken)
                    .ConfigureAwait(false);

                rolledBack = await _millSequence
                    .TryRollbackIfHighestInTxAsync(
                        conn, tx, source.MillNo, sourceSeq, updatedBy, "BundleMerge " + reason, cancellationToken)
                    .ConfigureAwait(false);

                await using (var after = new SqlCommand(
                    "SELECT Current_Sequence FROM dbo.Mill_Sequence WHERE Mill_No = @Mill;", conn, tx))
                {
                    after.Parameters.AddWithValue("@Mill", source.MillNo);
                    millAfter = Convert.ToInt32(
                        await after.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) ?? 0,
                        CultureInfo.InvariantCulture);
                }

                await InsertFillAuditAsync(conn, tx, source.BundleNo, target.BundleNo, source.TotalNdtPcs, cancellationToken)
                    .ConfigureAwait(false);

                await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                try { await tx.RollbackAsync(cancellationToken).ConfigureAwait(false); } catch { /* ignore */ }
                throw;
            }
        }

        await _bundles.RewriteNdtBatchNoInOutputCsvsAsync(source.BundleNo, target.BundleNo, cancellationToken)
            .ConfigureAwait(false);
        ArchiveBundleArtifacts(source.BundleNo, tombstone);

        var ppcCreated = 0;
        foreach (var file in sourceFiles)
        {
            if (!await IsAcceptedAsync(file, cancellationToken).ConfigureAwait(false))
                continue;
            var result = await _ppc.UpsertOpenItemAsync(
                    tombstone,
                    Path.GetFileName(file),
                    "—",
                    source.TotalNdtPcs,
                    source.TotalNdtPcs,
                    cancellationToken,
                    replacementBatchNo: target.BundleNo)
                .ConfigureAwait(false);
            if (result?.Created == true)
                ppcCreated++;
        }

        var targetAfter = await _bundles.GetByBatchNoAsync(target.BundleNo, cancellationToken).ConfigureAwait(false)
            ?? target;
        var print = await _reprint.ReprintAsync(targetAfter, cancellationToken).ConfigureAwait(false);

        return new BundleMergeResult(
            source.BundleNo,
            target.BundleNo,
            tombstone,
            source.TotalNdtPcs,
            source.TotalNdtPcs + target.TotalNdtPcs,
            rolledBack,
            BundleMergeLogic.SequenceMessage(source.BundleNo, source.MillNo, rolledBack, millAfter),
            ppcCreated,
            print.Success,
            print.Message);
    }

    private async Task<NdtBundleRecord?> FindPreviousAsync(NdtBundleRecord source, CancellationToken cancellationToken)
    {
        var all = await _bundles.GetBundlesAsync(cancellationToken).ConfigureAwait(false);
        var same = all
            .Where(b => b.MillNo == source.MillNo
                && !b.Voided
                && string.Equals(b.PoNumber, source.PoNumber, StringComparison.OrdinalIgnoreCase))
            .Select(b => new MergeCandidateBundle(
                b.BundleNo, b.PoNumber, b.MillNo, b.TotalNdtPcs, b.TargetNdtPcs ?? b.TotalNdtPcs,
                b.CsvFilled, b.CsvFillState, b.Voided))
            .ToList();

        if (!BundleMergeLogic.TryGetPreviousLive(same, source.BundleNo, source.MillNo, out var prev))
            return null;
        return await _bundles.GetByBatchNoAsync(prev.BundleNo, cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> IsAcceptedAsync(string sourceFile, CancellationToken cancellationToken)
    {
        try
        {
            var baseName = Path.GetFileName(sourceFile);
            var map = await _sapStatus.GetStatusesForFilesAsync(new[] { baseName }, cancellationToken)
                .ConfigureAwait(false);
            return map.TryGetValue(baseName, out var st)
                && st.Status == OutputSlitSapStatus.Accepted;
        }
        catch
        {
            return false;
        }
    }

    private void ArchiveBundleArtifacts(string sourceBundleNo, string tombstone)
    {
        try
        {
            NdtBundleOutputPaths.ArchiveBundleArtifacts(Opt, sourceBundleNo, tombstone);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not archive bundle artifacts for {Source}.", sourceBundleNo);
        }
    }

    private static async Task ExecAsync(
        SqlConnection conn,
        SqlTransaction tx,
        string sql,
        string source,
        string target,
        string tombstone,
        string reason,
        CancellationToken cancellationToken)
    {
        await using var cmd = new SqlCommand(sql, conn, tx);
        cmd.Parameters.AddWithValue("@Source", source);
        cmd.Parameters.AddWithValue("@Target", target);
        cmd.Parameters.AddWithValue("@Tombstone", tombstone);
        cmd.Parameters.AddWithValue("@Reason", reason);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task TryExecAsync(
        SqlConnection conn,
        SqlTransaction tx,
        string sql,
        string source,
        string target,
        string tombstone,
        string reason,
        CancellationToken cancellationToken)
    {
        try
        {
            await ExecAsync(conn, tx, sql, source, target, tombstone, reason, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Optional merge rewrite skipped for SQL starting {Prefix}.", sql.AsSpan(0, Math.Min(40, sql.Length)).ToString());
        }
    }

    private static async Task InsertFillAuditAsync(
        SqlConnection conn,
        SqlTransaction tx,
        string source,
        string target,
        int pipes,
        CancellationToken cancellationToken)
    {
        try
        {
            await using var cmd = new SqlCommand(@"
INSERT INTO dbo.NDT_Csv_Fill_Audit
    (Correlation_Id, Event_Type, Source_File_Name, Old_Batch_No, New_Batch_No, Pipe_Delta)
VALUES
    (@Cid, N'BundleMerge', NULL, @Old, @New, @Delta);", conn, tx);
            cmd.Parameters.AddWithValue("@Cid", Guid.NewGuid());
            cmd.Parameters.AddWithValue("@Old", source);
            cmd.Parameters.AddWithValue("@New", target);
            cmd.Parameters.AddWithValue("@Delta", pipes);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // audit table optional
        }
    }
}
