using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services.PoLifecycle;

/// <inheritdoc />
/// <remarks>
/// Draining/Closed persist in <c>dbo.Po_Lifecycle</c>. Running is absence of a row.
/// Restart without this table used to drop Closed (2026-07-26: PO 1000060288).
/// </remarks>
public sealed class PoLifecycleService : IPoLifecycleService
{
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly IMillOwnership? _millOwnership;
    private readonly ILogger<PoLifecycleService>? _logger;
    private readonly object _lock = new();
    private readonly Dictionary<string, Entry> _entries = new(StringComparer.OrdinalIgnoreCase);
    private bool _loaded;

    public PoLifecycleService(
        IOptionsMonitor<NdtBundleOptions> options,
        ILogger<PoLifecycleService>? logger = null)
        : this(options, millOwnership: null, logger)
    {
    }

    public PoLifecycleService(
        IOptionsMonitor<NdtBundleOptions> options,
        IMillOwnership? millOwnership,
        ILogger<PoLifecycleService>? logger = null)
    {
        _options = options;
        _millOwnership = millOwnership;
        _logger = logger;
    }

    public bool TryMarkDraining(int millNo, string poNumber, DateTime endedAtUtc)
    {
        if (!IsPlcLifecycleMill(millNo) || !Allows(millNo))
            return false;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po) || millNo is < 1 or > 4)
            return false;

        lock (_lock)
        {
            EnsureLoaded();
            _entries[MakeKey(millNo, po)] = new Entry
            {
                MillNo = millNo,
                PoNumber = po,
                EndedAtUtc = endedAtUtc,
                Phase = PoLifecyclePhase.Draining
            };
            Persist(millNo, po, PoLifecyclePhase.Draining, endedAtUtc, resume: false, "Draining");
        }

        return true;
    }

    public bool TryMarkClosed(int millNo, string poNumber)
    {
        if (!IsPlcLifecycleMill(millNo) || !Allows(millNo))
            return false;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po) || millNo is < 1 or > 4)
            return false;

        lock (_lock)
        {
            EnsureLoaded();
            if (!_entries.TryGetValue(MakeKey(millNo, po), out var entry))
                return false;

            entry.Phase = PoLifecyclePhase.Closed;
            Persist(millNo, po, PoLifecyclePhase.Closed, entry.EndedAtUtc, entry.IsResumeCandidate, "Closed");
            return true;
        }
    }

    public bool TryReopen(int millNo, string poNumber)
    {
        if (!IsPlcLifecycleMill(millNo) || !Allows(millNo))
            return false;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po) || millNo is < 1 or > 4)
            return false;

        lock (_lock)
        {
            EnsureLoaded();
            if (!_entries.TryGetValue(MakeKey(millNo, po), out var entry))
                return false;

            if (entry.Phase != PoLifecyclePhase.Closed)
                return false;

            _entries.Remove(MakeKey(millNo, po));
            DeleteRow(millNo, po, "Reopen");
            return true;
        }
    }

    public bool TryMarkResumeCandidate(int millNo, string poNumber)
    {
        if (!IsPlcLifecycleMill(millNo) || !Allows(millNo))
            return false;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po) || millNo is < 1 or > 4)
            return false;

        lock (_lock)
        {
            EnsureLoaded();
            if (!_entries.TryGetValue(MakeKey(millNo, po), out var entry))
                return false;

            if (entry.Phase != PoLifecyclePhase.Closed)
                return false;

            entry.IsResumeCandidate = true;
            Persist(millNo, po, PoLifecyclePhase.Closed, entry.EndedAtUtc, resume: true, "ResumeCandidate");
            return true;
        }
    }

    public bool IsResumeCandidate(int millNo, string poNumber)
    {
        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po) || millNo is < 1 or > 4 || !Allows(millNo))
            return false;

        lock (_lock)
        {
            EnsureLoaded();
            return _entries.TryGetValue(MakeKey(millNo, po), out var entry)
                   && entry.Phase == PoLifecyclePhase.Closed
                   && entry.IsResumeCandidate;
        }
    }

    public PoLifecyclePhase GetPhase(int millNo, string poNumber)
    {
        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po) || millNo is < 1 or > 4 || !Allows(millNo))
            return PoLifecyclePhase.Running;

        lock (_lock)
        {
            EnsureLoaded();
            return _entries.TryGetValue(MakeKey(millNo, po), out var entry)
                ? entry.Phase
                : PoLifecyclePhase.Running;
        }
    }

    public IReadOnlyList<PoLifecycleDrainEntry> GetExpiredDrains(DateTime utcNow, TimeSpan drainWindow)
    {
        if (SqlEnabled)
            return QueryExpiredDrains(utcNow, drainWindow);

        lock (_lock)
        {
            EnsureLoaded();
            return FilterOwned(_entries.Values
                .Where(e => e.Phase == PoLifecyclePhase.Draining && utcNow - e.EndedAtUtc >= drainWindow)
                .Select(ToDrain));
        }
    }

    public IReadOnlyList<PoLifecycleDrainEntry> GetClosedEntries()
    {
        if (SqlEnabled)
            return QueryByPhase(PoLifecyclePhase.Closed);

        lock (_lock)
        {
            EnsureLoaded();
            return FilterOwned(_entries.Values
                .Where(e => e.Phase == PoLifecyclePhase.Closed)
                .Select(ToDrain));
        }
    }

    private bool IsPlcLifecycleMill(int millNo) =>
        MillPoEndSourceResolver.ForMill(millNo, _options.CurrentValue) == MillPoEndSource.Plc;

    private bool Allows(int millNo) =>
        _millOwnership is null || _millOwnership.Allows(millNo);

    private bool SqlEnabled => SqlTraceabilityConnection.IsSqlEnabled(_options.CurrentValue);

    private static string MakeKey(int millNo, string po) => $"{millNo}|{po}";

    private static PoLifecycleDrainEntry ToDrain(Entry e) =>
        new(e.MillNo, e.PoNumber, e.EndedAtUtc, e.Phase);

    private IReadOnlyList<PoLifecycleDrainEntry> FilterOwned(IEnumerable<PoLifecycleDrainEntry> entries) =>
        entries.Where(e => Allows(e.MillNo)).ToList();

    private void EnsureLoaded()
    {
        if (_loaded)
            return;

        _loaded = true;
        if (!SqlEnabled)
            return;

        try
        {
            using var conn = SqlTraceabilityConnection.Create(_options.CurrentValue);
            conn.Open();
            using var cmd = new SqlCommand(
                "SELECT Mill_No, Po_Number, Phase, Ended_AtUtc, Is_Resume_Candidate FROM dbo.Po_Lifecycle;",
                conn);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var mill = reader.GetInt32(0);
                if (!Allows(mill))
                    continue;

                var po = reader.GetString(1);
                var phase = ParsePhase(reader.GetString(2));
                if (phase == PoLifecyclePhase.Running)
                    continue;

                _entries[MakeKey(mill, po)] = new Entry
                {
                    MillNo = mill,
                    PoNumber = po,
                    Phase = phase,
                    EndedAtUtc = reader.GetDateTime(3),
                    IsResumeCandidate = reader.GetBoolean(4)
                };
            }

            if (_entries.Count > 0)
            {
                _logger?.LogInformation(
                    "Restored {Count} PO lifecycle phase(s) from Po_Lifecycle: {Entries}.",
                    _entries.Count,
                    string.Join(", ", _entries.Values.Select(e => $"M{e.MillNo}/{e.PoNumber}={e.Phase}")));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to load Po_Lifecycle; starting with all POs Running.");
        }
    }

    private IReadOnlyList<PoLifecycleDrainEntry> QueryExpiredDrains(DateTime utcNow, TimeSpan drainWindow)
    {
        var cutoff = utcNow - drainWindow;
        var list = new List<PoLifecycleDrainEntry>();
        try
        {
            using var conn = SqlTraceabilityConnection.Create(_options.CurrentValue);
            conn.Open();
            using var cmd = new SqlCommand(@"
SELECT Mill_No, Po_Number, Ended_AtUtc, Phase
FROM dbo.Po_Lifecycle
WHERE Phase = N'Draining' AND Ended_AtUtc <= @Cutoff;", conn);
            cmd.Parameters.AddWithValue("@Cutoff", cutoff);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var mill = reader.GetInt32(0);
                if (!Allows(mill))
                    continue;
                list.Add(new PoLifecycleDrainEntry(
                    mill,
                    reader.GetString(1),
                    reader.GetDateTime(2),
                    ParsePhase(reader.GetString(3))));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Po_Lifecycle drain-expiry query failed.");
        }

        return list;
    }

    private IReadOnlyList<PoLifecycleDrainEntry> QueryByPhase(PoLifecyclePhase phase)
    {
        var list = new List<PoLifecycleDrainEntry>();
        try
        {
            using var conn = SqlTraceabilityConnection.Create(_options.CurrentValue);
            conn.Open();
            // Closed orphan sweep: only POs that still have Bundle_Accumulation rows.
            var sql = phase == PoLifecyclePhase.Closed
                ? @"
SELECT l.Mill_No, l.Po_Number, l.Ended_AtUtc, l.Phase
FROM dbo.Po_Lifecycle l
WHERE l.Phase = @Phase
  AND EXISTS (
      SELECT 1 FROM dbo.Bundle_Accumulation a
      WHERE a.Mill_No = l.Mill_No AND a.Po_Number = l.Po_Number);"
                : @"
SELECT Mill_No, Po_Number, Ended_AtUtc, Phase
FROM dbo.Po_Lifecycle
WHERE Phase = @Phase;";
            using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@Phase", PhaseName(phase));
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var mill = reader.GetInt32(0);
                if (!Allows(mill))
                    continue;
                list.Add(new PoLifecycleDrainEntry(
                    mill,
                    reader.GetString(1),
                    reader.GetDateTime(2),
                    ParsePhase(reader.GetString(3))));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Po_Lifecycle phase query failed.");
        }

        return list;
    }

    private void Persist(
        int millNo,
        string po,
        PoLifecyclePhase phase,
        DateTime endedAtUtc,
        bool resume,
        string eventType)
    {
        if (!SqlEnabled)
            return;

        try
        {
            using var conn = SqlTraceabilityConnection.Create(_options.CurrentValue);
            conn.Open();
            using var tx = conn.BeginTransaction();
            using (var cmd = new SqlCommand(@"
MERGE dbo.Po_Lifecycle WITH (HOLDLOCK) AS t
USING (SELECT @Mill AS Mill_No, @Po AS Po_Number) AS s
ON t.Mill_No = s.Mill_No AND t.Po_Number = s.Po_Number
WHEN MATCHED THEN UPDATE SET
    Phase = @Phase,
    Ended_AtUtc = @Ended,
    Is_Resume_Candidate = @Resume,
    Updated_AtUtc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT
    (Mill_No, Po_Number, Phase, Ended_AtUtc, Is_Resume_Candidate, Updated_AtUtc)
VALUES
    (@Mill, @Po, @Phase, @Ended, @Resume, SYSUTCDATETIME());", conn, tx))
            {
                cmd.Parameters.AddWithValue("@Mill", millNo);
                cmd.Parameters.AddWithValue("@Po", po);
                cmd.Parameters.AddWithValue("@Phase", PhaseName(phase));
                cmd.Parameters.AddWithValue("@Ended", endedAtUtc);
                cmd.Parameters.AddWithValue("@Resume", resume);
                cmd.ExecuteNonQuery();
            }

            using (var audit = new SqlCommand(@"
INSERT INTO dbo.Po_Lifecycle_Audit
    (Mill_No, Po_Number, Old_Phase, New_Phase, Ended_AtUtc, Is_Resume_Candidate, Event_Type)
VALUES (@Mill, @Po, NULL, @Phase, @Ended, @Resume, @Event);", conn, tx))
            {
                audit.Parameters.AddWithValue("@Mill", millNo);
                audit.Parameters.AddWithValue("@Po", po);
                audit.Parameters.AddWithValue("@Phase", PhaseName(phase));
                audit.Parameters.AddWithValue("@Ended", endedAtUtc);
                audit.Parameters.AddWithValue("@Resume", resume);
                audit.Parameters.AddWithValue("@Event", eventType);
                audit.ExecuteNonQuery();
            }

            tx.Commit();
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to persist Po_Lifecycle {Event} for PO {PO} Mill {Mill}.", eventType, po, millNo);
        }
    }

    private void DeleteRow(int millNo, string po, string eventType)
    {
        if (!SqlEnabled)
            return;

        try
        {
            using var conn = SqlTraceabilityConnection.Create(_options.CurrentValue);
            conn.Open();
            using var tx = conn.BeginTransaction();
            using (var del = new SqlCommand(
                       "DELETE FROM dbo.Po_Lifecycle WHERE Mill_No = @Mill AND Po_Number = @Po;", conn, tx))
            {
                del.Parameters.AddWithValue("@Mill", millNo);
                del.Parameters.AddWithValue("@Po", po);
                del.ExecuteNonQuery();
            }

            using (var audit = new SqlCommand(@"
INSERT INTO dbo.Po_Lifecycle_Audit
    (Mill_No, Po_Number, Old_Phase, New_Phase, Event_Type)
VALUES (@Mill, @Po, N'Closed', N'Running', @Event);", conn, tx))
            {
                audit.Parameters.AddWithValue("@Mill", millNo);
                audit.Parameters.AddWithValue("@Po", po);
                audit.Parameters.AddWithValue("@Event", eventType);
                audit.ExecuteNonQuery();
            }

            tx.Commit();
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to delete Po_Lifecycle for PO {PO} Mill {Mill}.", po, millNo);
        }
    }

    private static string PhaseName(PoLifecyclePhase phase) => phase switch
    {
        PoLifecyclePhase.Draining => "Draining",
        PoLifecyclePhase.Closed => "Closed",
        _ => "Running"
    };

    private static PoLifecyclePhase ParsePhase(string? raw) =>
        string.Equals(raw, "Draining", StringComparison.OrdinalIgnoreCase) ? PoLifecyclePhase.Draining
        : string.Equals(raw, "Closed", StringComparison.OrdinalIgnoreCase) ? PoLifecyclePhase.Closed
        : PoLifecyclePhase.Running;

    private sealed class Entry
    {
        public int MillNo { get; set; }
        public string PoNumber { get; set; } = string.Empty;
        public DateTime EndedAtUtc { get; set; }
        public PoLifecyclePhase Phase { get; set; }
        public bool IsResumeCandidate { get; set; }
    }
}
