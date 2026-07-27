using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Persists SAP lifecycle status per NDT Input Slit output CSV basename
/// (<c>Output_Slit_Sap_Status</c>) plus an append-only audit log (<c>Output_Slit_Sap_Status_Event</c>).
/// Phase 1: read-only observation — nothing gates on these rows yet.
/// </summary>
public interface IOutputSlitSapStatusRepository
{
    /// <summary>False when SQL traceability is not configured; callers may skip work.</summary>
    bool Enabled { get; }

    /// <summary>
    /// Applies watcher folder observations. Reports the number of rows inserted or transitioned
    /// (NoChange refreshes and ignored regressions do not count) plus the basenames that recorded a
    /// Resubmit transition (Rejected → Pending) this cycle — Phase 4 drift detection runs on those.
    /// Best-effort: SQL failures are logged, never thrown.
    /// </summary>
    Task<OutputSlitSapStatusApplyResult> ApplyObservationsAsync(
        IReadOnlyList<OutputSlitSapStatusObservation> observations,
        CancellationToken cancellationToken);

    /// <summary>
    /// Audit marker (Phase 4): the resubmitted pending file's content differed from
    /// <c>Output_Slit_Row</c> and SQL was re-synced to the file. Value details go to the service log.
    /// </summary>
    Task RecordResubmitDriftSyncedEventAsync(
        string fileName,
        string pendingFolder,
        CancellationToken cancellationToken);

    /// <summary>
    /// Seed-on-write: marks a freshly written NDT Input Slit output CSV as Pending so a status row
    /// exists before SAP pulls the file.
    /// </summary>
    Task RecordOutputFileWrittenAsync(
        string fileName,
        DateTime? fileLastWriteTimeUtc,
        string outputFolder,
        CancellationToken cancellationToken);

    /// <summary>
    /// Current SAP status per basename for the given files (keys are the file basenames that have a
    /// status row; unknown files are absent). Best-effort: returns empty on SQL failure or when the
    /// status table has not been migrated yet.
    /// </summary>
    Task<IReadOnlyDictionary<string, OutputSlitSapFileStatus>> GetStatusesForFilesAsync(
        IReadOnlyCollection<string> fileNames,
        CancellationToken cancellationToken);
}

/// <summary>Outcome of one watcher observation batch.</summary>
public sealed record OutputSlitSapStatusApplyResult(int Changed, IReadOnlyList<string> ResubmittedFiles)
{
    public static readonly OutputSlitSapStatusApplyResult Empty =
        new(0, Array.Empty<string>());
}

public sealed class OutputSlitSapStatusRepository : IOutputSlitSapStatusRepository
{
    private const string EventInitial = "Initial";
    private const string EventTransition = "Transition";
    private const string EventResubmitted = "Resubmitted";
    private const string EventRegressionIgnored = "RegressionIgnored";
    private const string EventResubmitDriftSynced = "ResubmitDriftSynced";

    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly ISqlTraceabilityWriteTracker _writeTracker;
    private readonly ILogger<OutputSlitSapStatusRepository> _logger;

    public OutputSlitSapStatusRepository(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        ISqlTraceabilityWriteTracker writeTracker,
        ILogger<OutputSlitSapStatusRepository> logger)
    {
        _optionsMonitor = optionsMonitor;
        _writeTracker = writeTracker;
        _logger = logger;
    }

    private NdtBundleOptions Opt => _optionsMonitor.CurrentValue;

    public bool Enabled => SqlTraceabilityConnection.IsSqlEnabled(Opt);

    public async Task RecordOutputFileWrittenAsync(
        string fileName,
        DateTime? fileLastWriteTimeUtc,
        string outputFolder,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(fileName))
            return;

        await ApplyObservationsAsync(
            new[]
            {
                new OutputSlitSapStatusObservation(
                    fileName.Trim(),
                    OutputSlitSapStatus.Pending,
                    outputFolder,
                    fileLastWriteTimeUtc,
                    OutputSlitSapStatusObservation.SourceSeedOnWrite)
            },
            cancellationToken).ConfigureAwait(false);
    }

    public async Task<OutputSlitSapStatusApplyResult> ApplyObservationsAsync(
        IReadOnlyList<OutputSlitSapStatusObservation> observations,
        CancellationToken cancellationToken)
    {
        if (!Enabled || observations.Count == 0)
            return OutputSlitSapStatusApplyResult.Empty;

        var changed = 0;
        var resubmitted = new List<string>();
        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await SqlTraceabilityConnection
                .OpenAsync(conn, _logger, "Output_Slit_Sap_Status upsert", cancellationToken)
                .ConfigureAwait(false);

            foreach (var obs in observations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (string.IsNullOrWhiteSpace(obs.FileName))
                    continue;

                try
                {
                    var (applied, isResubmit) = await ApplyOneAsync(conn, obs, cancellationToken).ConfigureAwait(false);
                    if (applied)
                        changed++;
                    if (isResubmit)
                        resubmitted.Add(obs.FileName.Trim());
                }
                catch (SqlException ex) when (IsMissingSapStatusTable(ex))
                {
                    _logger.LogWarning(
                        "Output_Slit_Sap_Status tables missing — run docs/Output_Slit_Sap_Status_AddTable.sql. SAP status not recorded for {File}.",
                        obs.FileName);
                    return new OutputSlitSapStatusApplyResult(changed, resubmitted);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed SAP status upsert for {File} ({Status}).", obs.FileName, obs.Observed);
                }
            }

            if (changed > 0)
                _writeTracker.RecordSuccess("Output_Slit_Sap_Status", $"{changed} status change(s)");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "SAP status batch upsert failed ({Count} observation(s)).", observations.Count);
        }

        return new OutputSlitSapStatusApplyResult(changed, resubmitted);
    }

    public async Task<IReadOnlyDictionary<string, OutputSlitSapFileStatus>> GetStatusesForFilesAsync(
        IReadOnlyCollection<string> fileNames,
        CancellationToken cancellationToken)
    {
        var result = new Dictionary<string, OutputSlitSapFileStatus>(StringComparer.OrdinalIgnoreCase);
        var names = fileNames
            .Where(static n => !string.IsNullOrWhiteSpace(n))
            .Select(static n => n.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (!Enabled || names.Count == 0)
            return result;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await SqlTraceabilityConnection
                .OpenAsync(conn, _logger, "Output_Slit_Sap_Status read", cancellationToken)
                .ConfigureAwait(false);

            var parameters = names.Select(static (_, i) => $"@F{i}").ToList();
            var sql = $@"
SELECT File_Name, Status, Status_AtUtc, Resubmit_Count
FROM dbo.Output_Slit_Sap_Status
WHERE File_Name IN ({string.Join(", ", parameters)});";

            await using var cmd = new SqlCommand(sql, conn);
            for (var i = 0; i < names.Count; i++)
                cmd.Parameters.AddWithValue(parameters[i], names[i]);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var fileName = reader.GetString(0);
                var status = OutputSlitSapStatusPolicy.TryParse(reader.IsDBNull(1) ? null : reader.GetString(1));
                if (status is null)
                    continue;

                result[fileName] = new OutputSlitSapFileStatus(
                    status.Value,
                    DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc),
                    reader.IsDBNull(3) ? 0 : reader.GetInt32(3));
            }
        }
        catch (SqlException ex) when (IsMissingSapStatusTable(ex))
        {
            _logger.LogDebug(
                "Output_Slit_Sap_Status missing (run docs/Output_Slit_Sap_Status_AddTable.sql); no SAP statuses returned.");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "SAP status read failed for {Count} file(s).", names.Count);
        }

        return result;
    }

    private async Task<(bool Changed, bool IsResubmit)> ApplyOneAsync(
        SqlConnection conn,
        OutputSlitSapStatusObservation obs,
        CancellationToken cancellationToken)
    {
        var fileName = obs.FileName.Trim();
        var lw = TruncateToMilliseconds(obs.FileLastWriteTimeUtc);

        var (current, storedLw) = await ReadCurrentAsync(conn, fileName, cancellationToken).ConfigureAwait(false);
        var kind = OutputSlitSapStatusPolicy.Decide(current, obs.Observed);

        switch (kind)
        {
            case OutputSlitSapStatusTransitionKind.InsertNew:
            {
                const string sql = @"
INSERT INTO dbo.Output_Slit_Sap_Status
    (File_Name, Status, Status_AtUtc, File_LastWriteTimeUtc, Observed_Folder, Prior_Status, Resubmit_Count)
VALUES
    (@FileName, @Status, SYSUTCDATETIME(), @Lw, @Folder, NULL, 0);";
                await using (var cmd = new SqlCommand(sql, conn))
                {
                    AddCommonParameters(cmd, fileName, obs, lw);
                    await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                await InsertEventAsync(conn, fileName, null, obs, lw, EventInitial, cancellationToken).ConfigureAwait(false);
                return (true, false);
            }

            case OutputSlitSapStatusTransitionKind.NoChange:
            {
                if (lw.HasValue && (!storedLw.HasValue || storedLw.Value < lw.Value))
                {
                    const string sql = @"
UPDATE dbo.Output_Slit_Sap_Status
SET File_LastWriteTimeUtc = @Lw, Observed_Folder = @Folder
WHERE File_Name = @FileName;";
                    await using var cmd = new SqlCommand(sql, conn);
                    AddCommonParameters(cmd, fileName, obs, lw);
                    await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                return (false, false);
            }

            case OutputSlitSapStatusTransitionKind.Transition:
            case OutputSlitSapStatusTransitionKind.Resubmit:
            {
                var isResubmit = kind == OutputSlitSapStatusTransitionKind.Resubmit;
                var sql = $@"
UPDATE dbo.Output_Slit_Sap_Status
SET Prior_Status = Status,
    Status = @Status,
    Status_AtUtc = SYSUTCDATETIME(),
    File_LastWriteTimeUtc = COALESCE(@Lw, File_LastWriteTimeUtc),
    Observed_Folder = @Folder{(isResubmit ? ",\n    Resubmit_Count = Resubmit_Count + 1" : string.Empty)}
WHERE File_Name = @FileName;";
                await using (var cmd = new SqlCommand(sql, conn))
                {
                    AddCommonParameters(cmd, fileName, obs, lw);
                    await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                await InsertEventAsync(
                        conn,
                        fileName,
                        current,
                        obs,
                        lw,
                        isResubmit ? EventResubmitted : EventTransition,
                        cancellationToken)
                    .ConfigureAwait(false);

                _logger.LogInformation(
                    "NDT Input Slit SAP status: {File} {Prior} → {New}{Resubmit} (source {Source}).",
                    fileName,
                    current,
                    obs.Observed,
                    isResubmit ? " [resubmit]" : string.Empty,
                    obs.Source);
                return (true, isResubmit);
            }

            case OutputSlitSapStatusTransitionKind.IgnoreFrozenAccepted:
            {
                // Accepted is terminal. Audit the disagreement once per file version (SQL-side
                // dedupe) so a lingering stale copy does not flood the event table every poll.
                const string sql = @"
IF NOT EXISTS (
    SELECT 1 FROM dbo.Output_Slit_Sap_Status_Event
    WHERE File_Name = @FileName
      AND Event_Type = @EventType
      AND ((File_LastWriteTimeUtc IS NULL AND @Lw IS NULL) OR File_LastWriteTimeUtc = @Lw))
INSERT INTO dbo.Output_Slit_Sap_Status_Event
    (File_Name, Prior_Status, New_Status, Event_Type, Observed_Folder, File_LastWriteTimeUtc, Source)
VALUES
    (@FileName, @PriorStatus, @Status, @EventType, @Folder, @Lw, @Source);";
                await using var cmd = new SqlCommand(sql, conn);
                AddCommonParameters(cmd, fileName, obs, lw);
                cmd.Parameters.AddWithValue("@EventType", EventRegressionIgnored);
                cmd.Parameters.AddWithValue("@PriorStatus", OutputSlitSapStatusPolicy.ToDbString(OutputSlitSapStatus.Accepted));
                cmd.Parameters.AddWithValue("@Source", obs.Source);
                var inserted = await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                if (inserted > 0)
                {
                    _logger.LogWarning(
                        "NDT Input Slit SAP status: {File} is Accepted (frozen) but was observed as {Observed} in {Folder}; keeping Accepted.",
                        fileName,
                        obs.Observed,
                        obs.ObservedFolder);
                }

                return (false, false);
            }

            default:
                return (false, false);
        }
    }

    public async Task RecordResubmitDriftSyncedEventAsync(
        string fileName,
        string pendingFolder,
        CancellationToken cancellationToken)
    {
        if (!Enabled || string.IsNullOrWhiteSpace(fileName))
            return;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(Opt);
            await SqlTraceabilityConnection
                .OpenAsync(conn, _logger, "Output_Slit_Sap_Status_Event drift marker", cancellationToken)
                .ConfigureAwait(false);

            const string sql = @"
INSERT INTO dbo.Output_Slit_Sap_Status_Event
    (File_Name, Prior_Status, New_Status, Event_Type, Observed_Folder, File_LastWriteTimeUtc, Source)
VALUES
    (@FileName, @Status, @Status, @EventType, @Folder, NULL, @Source);";
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@FileName", fileName.Trim());
            cmd.Parameters.AddWithValue("@Status", OutputSlitSapStatusPolicy.ToDbString(OutputSlitSapStatus.Pending));
            cmd.Parameters.AddWithValue("@EventType", EventResubmitDriftSynced);
            cmd.Parameters.AddWithValue("@Folder", string.IsNullOrWhiteSpace(pendingFolder) ? DBNull.Value : pendingFolder.Trim());
            cmd.Parameters.AddWithValue("@Source", OutputSlitSapStatusObservation.SourceWatcher);
            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not record ResubmitDriftSynced event for {File}.", fileName);
        }
    }

    private static async Task<(OutputSlitSapStatus? Status, DateTime? FileLastWriteTimeUtc)> ReadCurrentAsync(
        SqlConnection conn,
        string fileName,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT Status, File_LastWriteTimeUtc
FROM dbo.Output_Slit_Sap_Status
WHERE File_Name = @FileName;";
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@FileName", fileName);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return (null, null);

        var status = OutputSlitSapStatusPolicy.TryParse(reader.IsDBNull(0) ? null : reader.GetString(0));
        DateTime? lw = reader.IsDBNull(1) ? null : DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc);
        return (status, lw);
    }

    private static async Task InsertEventAsync(
        SqlConnection conn,
        string fileName,
        OutputSlitSapStatus? prior,
        OutputSlitSapStatusObservation obs,
        DateTime? lw,
        string eventType,
        CancellationToken cancellationToken)
    {
        const string sql = @"
INSERT INTO dbo.Output_Slit_Sap_Status_Event
    (File_Name, Prior_Status, New_Status, Event_Type, Observed_Folder, File_LastWriteTimeUtc, Source)
VALUES
    (@FileName, @PriorStatus, @Status, @EventType, @Folder, @Lw, @Source);";
        await using var cmd = new SqlCommand(sql, conn);
        AddCommonParameters(cmd, fileName, obs, lw);
        cmd.Parameters.AddWithValue(
            "@PriorStatus",
            prior.HasValue ? OutputSlitSapStatusPolicy.ToDbString(prior.Value) : DBNull.Value);
        cmd.Parameters.AddWithValue("@EventType", eventType);
        cmd.Parameters.AddWithValue("@Source", obs.Source);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static void AddCommonParameters(
        SqlCommand cmd,
        string fileName,
        OutputSlitSapStatusObservation obs,
        DateTime? lw)
    {
        cmd.Parameters.AddWithValue("@FileName", fileName);
        cmd.Parameters.AddWithValue("@Status", OutputSlitSapStatusPolicy.ToDbString(obs.Observed));
        cmd.Parameters.AddWithValue("@Folder", string.IsNullOrWhiteSpace(obs.ObservedFolder)
            ? DBNull.Value
            : obs.ObservedFolder.Trim());
        cmd.Parameters.AddWithValue("@Lw", lw.HasValue ? lw.Value : DBNull.Value);
    }

    /// <summary>datetime2(3) stores milliseconds; truncate so stored/observed comparisons are stable.</summary>
    private static DateTime? TruncateToMilliseconds(DateTime? value)
    {
        if (!value.HasValue)
            return null;

        var v = value.Value;
        return new DateTime(v.Ticks - (v.Ticks % TimeSpan.TicksPerMillisecond), DateTimeKind.Utc);
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

        return false;
    }
}
