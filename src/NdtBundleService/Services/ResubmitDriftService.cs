using Microsoft.Extensions.Logging;

namespace NdtBundleService.Services;

/// <summary>One (batch, slit) whose resubmitted-file value differs from <c>Output_Slit_Row</c>.</summary>
public sealed record ResubmitDriftChange(string BatchNo, string SlitNo, int SqlNdtPipes, int FileNdtPipes);

/// <summary>
/// Diff between a resubmitted pending output CSV and <c>Output_Slit_Row</c> for the same basename.
/// <see cref="FileOnly"/> / <see cref="SqlOnly"/> are anomalies (rows added or removed in the edited
/// file) that are logged for manual action, never auto-applied.
/// </summary>
public sealed record ResubmitDriftPlan(
    IReadOnlyList<ResubmitDriftChange> Changes,
    IReadOnlyList<(string BatchNo, string SlitNo, int NdtPipes)> FileOnly,
    IReadOnlyList<(string BatchNo, string SlitNo, int NdtPipes)> SqlOnly)
{
    public bool HasDrift => Changes.Count > 0 || FileOnly.Count > 0 || SqlOnly.Count > 0;
}

/// <summary>Outcome of <see cref="IResubmitDriftService.DetectAndReconcileAsync"/> for logging.</summary>
public sealed record ResubmitDriftResult(
    ResubmitDriftPlan Plan,
    int SlitsSynced,
    IReadOnlyList<string> BatchTotalsSynced,
    IReadOnlyList<string> ManualReconLockedBatches);

/// <summary>
/// Pure Phase 4 helpers: parse a per-slit output CSV into (batch, slit) → NDT pipes sums and diff
/// them against the SQL sums.
/// </summary>
public static class ResubmitDriftPlanner
{
    /// <summary>
    /// Per-(batch, normalized slit) NDT-pipes sums from output CSV lines. Rows without a batch
    /// number are ignored (never stamped, so never posted to SAP). Returns null for an empty file.
    /// Header resolution falls back to the standard positional columns, same as the reconcile CSV
    /// fallback readers.
    /// </summary>
    public static Dictionary<(string BatchNo, string SlitNo), int>? ParseOutputCsvSums(IReadOnlyList<string> lines)
    {
        if (lines.Count == 0)
            return null;

        var columns = ReconcileCsvParsing.ResolveOutputCsvColumns(lines[0]);
        var sums = new Dictionary<(string, string), int>();
        for (var i = 1; i < lines.Count; i++)
        {
            if (string.IsNullOrWhiteSpace(lines[i]))
                continue;

            var cols = ReconcileCsvParsing.SplitCsvLine(lines[i]);
            if (!columns.TryGetField(cols, columns.NdtBatchNo, out var batchRaw) || string.IsNullOrWhiteSpace(batchRaw))
                continue;

            columns.TryGetField(cols, columns.SlitNo, out var slitRaw);
            if (!columns.TryGetField(cols, columns.NdtPipes, out var pipesRaw) || !int.TryParse(pipesRaw, out var pipes))
                continue;

            var key = (batchRaw.Trim(), ReconcileCsvParsing.NormalizeSlitKey(slitRaw));
            sums[key] = sums.TryGetValue(key, out var existing) ? existing + pipes : pipes;
        }

        return sums;
    }

    public static ResubmitDriftPlan Compute(
        IReadOnlyDictionary<(string BatchNo, string SlitNo), int> fileSums,
        IReadOnlyDictionary<(string BatchNo, string SlitNo), int> sqlSums)
    {
        var changes = new List<ResubmitDriftChange>();
        var fileOnly = new List<(string, string, int)>();
        var sqlOnly = new List<(string, string, int)>();

        foreach (var (key, fileValue) in fileSums)
        {
            if (sqlSums.TryGetValue(key, out var sqlValue))
            {
                if (sqlValue != fileValue)
                    changes.Add(new ResubmitDriftChange(key.BatchNo, key.SlitNo, sqlValue, fileValue));
            }
            else
            {
                fileOnly.Add((key.BatchNo, key.SlitNo, fileValue));
            }
        }

        foreach (var (key, sqlValue) in sqlSums)
        {
            if (!fileSums.ContainsKey(key))
                sqlOnly.Add((key.BatchNo, key.SlitNo, sqlValue));
        }

        return new ResubmitDriftPlan(changes, fileOnly, sqlOnly);
    }
}

/// <summary>
/// Phase 4: when a Rejected file is edited and resubmitted into the pending folder, the resubmitted
/// CSV is authoritative — it is what SAP will now post. This service diffs it against
/// <c>Output_Slit_Row</c> and re-syncs SQL per-slit values (same path as operator slit reconcile)
/// plus bundle totals so the bundle-total invariant (Q5) holds. <c>Manual_Recon</c>-locked bundles
/// keep their locked total; only <c>Post_Recon_Csv_Sum</c> refreshes (unchanged lock semantics).
/// Row additions/removals in the edited file are logged as anomalies, never auto-applied.
/// </summary>
public interface IResubmitDriftService
{
    /// <summary>Null when the pending file is missing/unreadable or SQL has no rows for the basename.</summary>
    Task<ResubmitDriftResult?> DetectAndReconcileAsync(
        string pendingFolder,
        string fileName,
        CancellationToken cancellationToken);
}

public sealed class ResubmitDriftService : IResubmitDriftService
{
    private readonly INdtBundleRepository _bundleRepository;
    private readonly IReconcileSyncService _reconcileSync;
    private readonly IOutputSlitSapStatusRepository _sapStatus;
    private readonly ICsvFillService _csvFill;
    private readonly ILogger<ResubmitDriftService> _logger;

    public ResubmitDriftService(
        INdtBundleRepository bundleRepository,
        IReconcileSyncService reconcileSync,
        IOutputSlitSapStatusRepository sapStatus,
        ICsvFillService csvFill,
        ILogger<ResubmitDriftService> logger)
    {
        _bundleRepository = bundleRepository;
        _reconcileSync = reconcileSync;
        _sapStatus = sapStatus;
        _csvFill = csvFill;
        _logger = logger;
    }

    public async Task<ResubmitDriftResult?> DetectAndReconcileAsync(
        string pendingFolder,
        string fileName,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(pendingFolder) || string.IsNullOrWhiteSpace(fileName))
            return null;

        var path = Path.Combine(pendingFolder.Trim(), fileName.Trim());
        Dictionary<(string BatchNo, string SlitNo), int>? fileSums;
        try
        {
            if (!File.Exists(path))
            {
                _logger.LogDebug("Resubmit drift: pending file {Path} no longer exists; skipping.", path);
                return null;
            }

            fileSums = ResubmitDriftPlanner.ParseOutputCsvSums(
                await File.ReadAllLinesAsync(path, cancellationToken).ConfigureAwait(false));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Resubmit drift: could not read {Path}; skipping drift check.", path);
            return null;
        }

        if (fileSums is null)
        {
            _logger.LogWarning(
                "Resubmit drift: {File} is empty; cannot compare against Output_Slit_Row.",
                fileName);
            return null;
        }

        var sqlRows = await _bundleRepository
            .GetOutputSlitRowSumsForSourceFileAsync(fileName, cancellationToken)
            .ConfigureAwait(false);
        if (sqlRows.Count == 0)
        {
            _logger.LogWarning(
                "Resubmit drift: no Output_Slit_Row rows found for resubmitted file {File}; nothing to reconcile "
                + "(original ingest may predate SQL traceability).",
                fileName);
            return null;
        }

        var sqlSums = new Dictionary<(string, string), int>();
        foreach (var (batchNo, slitNo, ndtPipes) in sqlRows)
            sqlSums[(batchNo, slitNo)] = sqlSums.TryGetValue((batchNo, slitNo), out var existing) ? existing + ndtPipes : ndtPipes;

        // Batch-number move: file batch column differs from SQL for this basename (detect-on-resubmit).
        var sqlBatches = sqlSums.Keys.Select(static k => k.Item1).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        var fileBatches = fileSums.Keys.Select(static k => k.Item1).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        if (sqlBatches.Count == 1
            && fileBatches.Count == 1
            && !string.Equals(sqlBatches[0], fileBatches[0], StringComparison.OrdinalIgnoreCase))
        {
            var oldBatch = sqlBatches[0];
            var newBatch = fileBatches[0];
            var movedPipes = fileSums.Values.Sum();
            var correlationId = await _csvFill
                .ApplyBatchMoveAsync(fileName, oldBatch, newBatch, movedPipes, cancellationToken)
                .ConfigureAwait(false);
            _logger.LogWarning(
                "Resubmit drift BATCH MOVE CorrelationId={CorrelationId} file={File}: {OldBatch} → {NewBatch} pipes={Pipes}. "
                + "Operator email to SAP must delete under OLD key then reload under NEW key.",
                correlationId,
                fileName,
                oldBatch,
                newBatch,
                movedPipes);

            // Sync slit rows to new batch values via reconcile path for matching slits.
            var slitsSyncedMove = 0;
            foreach (var (key, fileValue) in fileSums)
            {
                var rows = await _reconcileSync
                    .SyncAfterSlitReconcileAsync(key.BatchNo, key.SlitNo, fileValue, cancellationToken)
                    .ConfigureAwait(false);
                if (rows > 0)
                    slitsSyncedMove++;
            }

            await _sapStatus
                .RecordResubmitDriftSyncedEventAsync(fileName, pendingFolder, cancellationToken)
                .ConfigureAwait(false);

            var movePlan = new ResubmitDriftPlan(
                Array.Empty<ResubmitDriftChange>(),
                Array.Empty<(string, string, int)>(),
                Array.Empty<(string, string, int)>());
            return new ResubmitDriftResult(movePlan, slitsSyncedMove, [newBatch], Array.Empty<string>());
        }

        var plan = ResubmitDriftPlanner.Compute(fileSums, sqlSums);
        if (!plan.HasDrift)
        {
            _logger.LogInformation(
                "Resubmit drift: {File} matches Output_Slit_Row ({Slits} slit value(s)); no sync needed.",
                fileName,
                fileSums.Count);
            return new ResubmitDriftResult(plan, 0, Array.Empty<string>(), Array.Empty<string>());
        }

        foreach (var anomaly in plan.FileOnly)
        {
            _logger.LogWarning(
                "Resubmit drift: {File} contains batch {BatchNo} slit {SlitNo} ({Pipes} pipes) with no matching "
                + "Output_Slit_Row — row added in the edited file? Not auto-applied; review manually.",
                fileName, anomaly.BatchNo, anomaly.SlitNo, anomaly.NdtPipes);
        }

        foreach (var anomaly in plan.SqlOnly)
        {
            _logger.LogWarning(
                "Resubmit drift: Output_Slit_Row has batch {BatchNo} slit {SlitNo} ({Pipes} pipes) missing from "
                + "resubmitted {File} — row removed in the edited file? Not auto-deleted; review manually.",
                anomaly.BatchNo, anomaly.SlitNo, anomaly.NdtPipes, fileName);
        }

        // Same-bundle count revision: adjust Csv_Filled by DELTA (new − old), never double-add.
        var slitsSynced = 0;
        foreach (var change in plan.Changes)
        {
            _logger.LogWarning(
                "Resubmit drift REVISION: {File} batch {BatchNo} slit {SlitNo}: Output_Slit_Row {SqlValue} → {FileValue} "
                + "(delta={Delta}; resubmitted file is authoritative).",
                fileName, change.BatchNo, change.SlitNo, change.SqlNdtPipes, change.FileNdtPipes,
                change.FileNdtPipes - change.SqlNdtPipes);
            await _csvFill
                .ApplyCountRevisionAsync(
                    fileName,
                    change.BatchNo,
                    change.SqlNdtPipes,
                    change.FileNdtPipes,
                    cancellationToken)
                .ConfigureAwait(false);
            var rows = await _reconcileSync
                .SyncAfterSlitReconcileAsync(change.BatchNo, change.SlitNo, change.FileNdtPipes, cancellationToken)
                .ConfigureAwait(false);
            if (rows > 0)
                slitsSynced++;
        }

        // Bundle-total invariant (Q5): totals follow the new slit sums; Manual_Recon lock unchanged.
        var totalsSynced = new List<string>();
        var lockedBatches = new List<string>();
        foreach (var batchNo in plan.Changes
                     .Select(static c => c.BatchNo)
                     .Distinct(StringComparer.OrdinalIgnoreCase))
        {
            try
            {
                if (await _bundleRepository.IsManualReconLockedAsync(batchNo, cancellationToken).ConfigureAwait(false))
                {
                    await _bundleRepository.TryUpdatePostReconCsvSumAsync(batchNo, cancellationToken).ConfigureAwait(false);
                    lockedBatches.Add(batchNo);
                    _logger.LogInformation(
                        "Resubmit drift: bundle {BatchNo} is Manual_Recon locked; total kept, Post_Recon_Csv_Sum refreshed.",
                        batchNo);
                }
                else
                {
                    await _bundleRepository
                        .TrySyncBundleTotalFromSlitsAsync(batchNo, forceFromSlits: true, cancellationToken)
                        .ConfigureAwait(false);
                    totalsSynced.Add(batchNo);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Resubmit drift: bundle total sync failed for {BatchNo}.", batchNo);
            }
        }

        if (slitsSynced > 0)
        {
            await _sapStatus
                .RecordResubmitDriftSyncedEventAsync(fileName, pendingFolder, cancellationToken)
                .ConfigureAwait(false);
        }

        _logger.LogInformation(
            "Resubmit drift for {File}: {Changes} slit value change(s) applied, {FileOnly} file-only and {SqlOnly} "
            + "SQL-only anomaly(ies) logged, bundle totals synced for [{Totals}], locked bundles [{Locked}].",
            fileName,
            plan.Changes.Count,
            plan.FileOnly.Count,
            plan.SqlOnly.Count,
            string.Join(", ", totalsSynced),
            string.Join(", ", lockedBatches));

        return new ResubmitDriftResult(plan, slitsSynced, totalsSynced, lockedBatches);
    }
}
