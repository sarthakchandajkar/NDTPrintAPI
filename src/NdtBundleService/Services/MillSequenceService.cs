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
    /// Increment <c>Mill_Sequence</c> and insert <c>NDT_Bundle</c> in one transaction.
    /// Throws when SQL cannot allocate; does not print or invent a batch number.
    /// </summary>
    Task<(int Sequence, string Formatted)> AllocateAndInsertBundleAsync(
        NdtBundleRecord pending,
        CancellationToken cancellationToken);
}

public sealed class MillSequenceService : IMillSequenceService
{
    public const int LargeJumpWarnThreshold = 100;

    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly INdtBundleRepository _bundles;
    private readonly ILogger<MillSequenceService> _logger;

    public MillSequenceService(
        IOptionsMonitor<NdtBundleOptions> options,
        INdtBundleRepository bundles,
        ILogger<MillSequenceService> logger)
    {
        _options = options;
        _bundles = bundles;
        _logger = logger;
    }

    private NdtBundleOptions Opt => _options.CurrentValue;
    public bool IsEnabled => SqlTraceabilityConnection.IsSqlEnabled(Opt);

    public async Task<(int Sequence, string Formatted)> AllocateAndInsertBundleAsync(
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
            var seq = await AllocateNextInTxAsync(
                    conn, tx, pending.MillNo, "BundleClose", "Bundle close", cancellationToken)
                .ConfigureAwait(false);
            var formatted = NdtBundleSequence.Format(seq, pending.MillNo);
            pending.BundleNo = formatted;
            await _bundles
                .RecordBundlePendingPrintInTxAsync(conn, tx, pending, cancellationToken)
                .ConfigureAwait(false);
            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return (seq, formatted);
        }
        catch
        {
            try { await tx.RollbackAsync(cancellationToken).ConfigureAwait(false); } catch { /* ignore */ }
            throw;
        }
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
            var leftoverJson = ReadLeftoverJsonMillMaxSequence(mill);
            var seed = Math.Max(scan, Math.Max(config, leftoverJson));

            await using var ins = new SqlCommand(@"
INSERT INTO dbo.Mill_Sequence (Mill_No, Current_Sequence, Updated_By, Reason)
VALUES (@Mill, @Seq, N'StartupSeed', N'Seed missing row from scan/config/JSON');", conn);
            ins.Parameters.AddWithValue("@Mill", mill);
            ins.Parameters.AddWithValue("@Seq", seed);
            await ins.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            await InsertAuditAsync(conn, tx: null, mill, oldSeq: null, seed, "Seed", "StartupSeed",
                "Seed missing row", cancellationToken).ConfigureAwait(false);

            _logger.LogInformation(
                "Mill_Sequence mill {Mill} seeded Current_Sequence={Seq} (scan={Scan}, config={Config}, json={Json}).",
                mill, seed, scan, config, leftoverJson);
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

    private int ReadLeftoverJsonMillMaxSequence(int millNo)
    {
        try
        {
            var path = (Opt.NdtBundleRuntimeStateFile ?? string.Empty).Trim();
            if (string.IsNullOrEmpty(path))
            {
                var folder = (Opt.OutputBundleFolder ?? string.Empty).Trim();
                if (string.IsNullOrEmpty(folder))
                    return 0;
                path = Path.Combine(folder, "NdtBundleRuntimeState.json");
            }

            if (!File.Exists(path))
                return 0;

            using var doc = System.Text.Json.JsonDocument.Parse(File.ReadAllText(path));
            if (!doc.RootElement.TryGetProperty("millMaxSequence", out var map))
                return 0;
            var key = millNo.ToString(CultureInfo.InvariantCulture);
            if (!map.TryGetProperty(key, out var el))
                return 0;
            return el.TryGetInt32(out var seq) ? Math.Max(0, seq) : 0;
        }
        catch
        {
            return 0;
        }
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
