using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.PlcHandshake;

namespace NdtBundleService.Services.MillInstanceStatus;

/// <summary>
/// Dashboard-only live PLC snapshot. Mill processes publish; Shared reads.
/// Production PO-end / threshold / tag print must not call this on the S7 loop.
/// </summary>
public interface IMillInstanceStatusStore
{
    IReadOnlyList<PlcHandshakeMillStatus> LoadAll();

    void Upsert(PlcHandshakeMillStatus status, Guid instanceId, string machineName, string? serviceName);
}

public static class MillInstanceStatusFreshness
{
    /// <summary>UI treats a mill as down if its instance stopped publishing (not used for bundle close).</summary>
    public static readonly TimeSpan StaleAfter = TimeSpan.FromSeconds(5);

    public static PlcHandshakeMillStatus Apply(PlcHandshakeMillStatus status, DateTimeOffset utcNow)
    {
        if (status.LastUpdateUtc + StaleAfter >= utcNow)
            return status;

        var copy = MillInstanceStatusMapper.Clone(status);
        copy.Connected = false;
        if (string.IsNullOrWhiteSpace(copy.LastError))
            copy.LastError = "mill telemetry stale (instance not publishing)";
        return copy;
    }

    public static PlcHandshakeMillStatus Placeholder(int millNo) =>
        new()
        {
            MillNo = millNo,
            MillName = $"Mill-{millNo}",
            Connected = false,
            PlcConnectionEnabled = false,
            HandshakeState = "Idle",
            LastError = "no mill telemetry",
            LastUpdateUtc = DateTimeOffset.MinValue
        };

    public static IReadOnlyList<PlcHandshakeMillStatus> PadAndApplyStale(
        IReadOnlyList<PlcHandshakeMillStatus> loaded,
        DateTimeOffset utcNow)
    {
        var byMill = new Dictionary<int, PlcHandshakeMillStatus>();
        foreach (var row in loaded)
        {
            if (row.MillNo is >= 1 and <= 4)
                byMill[row.MillNo] = Apply(row, utcNow);
        }

        var list = new List<PlcHandshakeMillStatus>(4);
        for (var mill = 1; mill <= 4; mill++)
        {
            if (byMill.TryGetValue(mill, out var row))
                list.Add(row);
            else
                list.Add(Placeholder(mill));
        }

        return list;
    }
}

internal static class MillInstanceStatusMapper
{
    public static PlcHandshakeMillStatus Clone(PlcHandshakeMillStatus m) =>
        new()
        {
            MillName = m.MillName,
            MillNo = m.MillNo,
            IpAddress = m.IpAddress,
            Connected = m.Connected,
            PlcConnectionEnabled = m.PlcConnectionEnabled,
            TriggerActive = m.TriggerActive,
            AckActive = m.AckActive,
            HandshakeState = m.HandshakeState,
            LastPoChangeUtc = m.LastPoChangeUtc,
            LastError = m.LastError,
            LastUpdateUtc = m.LastUpdateUtc,
            OkCount = m.OkCount,
            NokCount = m.NokCount,
            NdtCount = m.NdtCount,
            PoId = m.PoId,
            SlitId = m.SlitId,
            CountsUpdatedUtc = m.CountsUpdatedUtc,
            LineRunning = m.LineRunning,
            AccumulatedValue = m.AccumulatedValue,
            ThresholdValue = m.ThresholdValue,
            HooterActive = m.HooterActive,
            StuckTriggerAlarm = m.StuckTriggerAlarm,
            AckWriteFailedAlarm = m.AckWriteFailedAlarm,
            LastPoEnd = m.LastPoEnd is null
                ? null
                : new PlcHandshakeLastPoEnd
                {
                    PoId = m.LastPoEnd.PoId,
                    NdtCountFinal = m.LastPoEnd.NdtCountFinal,
                    TimestampUtc = m.LastPoEnd.TimestampUtc
                }
        };

    public static PlcHandshakeMillStatus Heartbeat(int millNo, string millName, string? lastError) =>
        new()
        {
            MillNo = millNo,
            MillName = string.IsNullOrWhiteSpace(millName) ? $"Mill-{millNo}" : millName.Trim(),
            Connected = false,
            PlcConnectionEnabled = false,
            HandshakeState = "Idle",
            LastError = lastError,
            LastUpdateUtc = DateTimeOffset.UtcNow
        };
}

internal sealed class InMemoryMillInstanceStatusStore : IMillInstanceStatusStore
{
    private readonly object _gate = new();
    private readonly Dictionary<int, PlcHandshakeMillStatus> _rows = new();

    public int UpsertCalls { get; private set; }

    public IReadOnlyList<PlcHandshakeMillStatus> LoadAll()
    {
        lock (_gate)
            return _rows.Values.Select(MillInstanceStatusMapper.Clone).OrderBy(m => m.MillNo).ToList();
    }

    public void Upsert(PlcHandshakeMillStatus status, Guid instanceId, string machineName, string? serviceName)
    {
        if (status.MillNo is < 1 or > 4)
            return;
        lock (_gate)
        {
            UpsertCalls++;
            _rows[status.MillNo] = MillInstanceStatusMapper.Clone(status);
        }
    }
}

internal sealed class SqlMillInstanceStatusStore : IMillInstanceStatusStore
{
    /// <summary>Dashboard telemetry only — fail fast so a hung SQL never sits on the mill process.</summary>
    internal const int CommandTimeoutSeconds = 2;

    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly ILogger _logger;

    public SqlMillInstanceStatusStore(IOptionsMonitor<NdtBundleOptions> options, ILogger logger)
    {
        _options = options;
        _logger = logger;
    }

    public IReadOnlyList<PlcHandshakeMillStatus> LoadAll()
    {
        var list = new List<PlcHandshakeMillStatus>(4);
        try
        {
            using var conn = SqlTraceabilityConnection.Create(_options.CurrentValue);
            conn.Open();
            using var cmd = new SqlCommand(@"
SELECT Mill_No, Mill_Name, Ip_Address, Connected, Plc_Connection_Enabled,
       Trigger_Active, Ack_Active, Handshake_State, Last_Error, Last_Update_AtUtc,
       Ok_Count, Nok_Count, Ndt_Count, Po_Id, Slit_Id, Counts_Updated_AtUtc,
       Line_Running, Accumulated_Value, Threshold_Value, Hooter_Active,
       Stuck_Trigger_Alarm, Ack_Write_Failed_Alarm,
       Last_Po_End_Po_Id, Last_Po_End_Ndt, Last_Po_End_AtUtc
FROM dbo.Mill_Instance_Status WITH (NOLOCK);", conn);
            cmd.CommandTimeout = CommandTimeoutSeconds;
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                list.Add(ReadRow(reader));
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to load Mill_Instance_Status (dashboard tiles only).");
        }

        return list;
    }

    public void Upsert(PlcHandshakeMillStatus status, Guid instanceId, string machineName, string? serviceName)
    {
        if (status.MillNo is < 1 or > 4)
            return;

        try
        {
            UpsertCore(status, instanceId, machineName, serviceName);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Failed to publish Mill_Instance_Status for mill {Mill} (dashboard only; PO-end/print continue).",
                status.MillNo);
        }
    }

    private void UpsertCore(PlcHandshakeMillStatus status, Guid instanceId, string machineName, string? serviceName)
    {
        using var conn = SqlTraceabilityConnection.Create(_options.CurrentValue);
        conn.Open();
        using var cmd = new SqlCommand(@"
MERGE dbo.Mill_Instance_Status AS t
USING (SELECT @Mill AS Mill_No) AS s
ON t.Mill_No = s.Mill_No
WHEN MATCHED THEN UPDATE SET
    Instance_Id = @InstanceId,
    Machine_Name = @Machine,
    Service_Name = @Service,
    Mill_Name = @MillName,
    Ip_Address = @Ip,
    Connected = @Connected,
    Plc_Connection_Enabled = @PlcEn,
    Trigger_Active = @Trig,
    Ack_Active = @Ack,
    Handshake_State = @State,
    Last_Error = @Err,
    Last_Update_AtUtc = SYSUTCDATETIME(),
    Ok_Count = @Ok,
    Nok_Count = @Nok,
    Ndt_Count = @Ndt,
    Po_Id = @Po,
    Slit_Id = @Slit,
    Counts_Updated_AtUtc = @CountsUtc,
    Line_Running = @Line,
    Accumulated_Value = @Acc,
    Threshold_Value = @Thr,
    Hooter_Active = @Hooter,
    Stuck_Trigger_Alarm = @Stuck,
    Ack_Write_Failed_Alarm = @AckFail,
    Last_Po_End_Po_Id = @EndPo,
    Last_Po_End_Ndt = @EndNdt,
    Last_Po_End_AtUtc = @EndUtc
WHEN NOT MATCHED THEN INSERT (
    Mill_No, Instance_Id, Machine_Name, Service_Name, Mill_Name, Ip_Address,
    Connected, Plc_Connection_Enabled, Trigger_Active, Ack_Active, Handshake_State, Last_Error,
    Ok_Count, Nok_Count, Ndt_Count, Po_Id, Slit_Id, Counts_Updated_AtUtc,
    Line_Running, Accumulated_Value, Threshold_Value, Hooter_Active,
    Stuck_Trigger_Alarm, Ack_Write_Failed_Alarm,
    Last_Po_End_Po_Id, Last_Po_End_Ndt, Last_Po_End_AtUtc)
VALUES (
    @Mill, @InstanceId, @Machine, @Service, @MillName, @Ip,
    @Connected, @PlcEn, @Trig, @Ack, @State, @Err,
    @Ok, @Nok, @Ndt, @Po, @Slit, @CountsUtc,
    @Line, @Acc, @Thr, @Hooter, @Stuck, @AckFail,
    @EndPo, @EndNdt, @EndUtc);", conn);

        cmd.Parameters.AddWithValue("@Mill", status.MillNo);
        cmd.Parameters.AddWithValue("@InstanceId", instanceId);
        cmd.Parameters.AddWithValue("@Machine", Trunc(machineName, 128));
        cmd.Parameters.AddWithValue("@Service", (object?)Trunc(serviceName, 128) ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@MillName", Trunc(status.MillName, 64));
        cmd.Parameters.AddWithValue("@Ip", (object?)Trunc(status.IpAddress, 64) ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Connected", status.Connected);
        cmd.Parameters.AddWithValue("@PlcEn", status.PlcConnectionEnabled);
        cmd.Parameters.AddWithValue("@Trig", status.TriggerActive);
        cmd.Parameters.AddWithValue("@Ack", status.AckActive);
        cmd.Parameters.AddWithValue("@State", Trunc(status.HandshakeState, 64));
        cmd.Parameters.AddWithValue("@Err", (object?)Trunc(status.LastError, 400) ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Ok", (object?)status.OkCount ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Nok", (object?)status.NokCount ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Ndt", (object?)status.NdtCount ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Po", (object?)status.PoId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Slit", (object?)status.SlitId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@CountsUtc", ToUtcDb(status.CountsUpdatedUtc));
        cmd.Parameters.AddWithValue("@Line", (object?)status.LineRunning ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Acc", (object?)status.AccumulatedValue ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Thr", (object?)status.ThresholdValue ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Hooter", status.HooterActive);
        cmd.Parameters.AddWithValue("@Stuck", status.StuckTriggerAlarm);
        cmd.Parameters.AddWithValue("@AckFail", status.AckWriteFailedAlarm);
        cmd.Parameters.AddWithValue("@EndPo", (object?)status.LastPoEnd?.PoId ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@EndNdt", (object?)status.LastPoEnd?.NdtCountFinal ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@EndUtc", ToUtcDb(status.LastPoEnd?.TimestampUtc));
        cmd.CommandTimeout = CommandTimeoutSeconds;
        cmd.ExecuteNonQuery();
    }

    private static PlcHandshakeMillStatus ReadRow(SqlDataReader reader)
    {
        var millNo = reader.GetInt32(0);
        var lastPoId = reader.IsDBNull(22) ? (int?)null : reader.GetInt32(22);
        var lastPoNdt = reader.IsDBNull(23) ? (int?)null : reader.GetInt32(23);
        var lastPoUtc = ReadUtc(reader, 24);
        return new PlcHandshakeMillStatus
        {
            MillNo = millNo,
            MillName = reader.IsDBNull(1) ? $"Mill-{millNo}" : reader.GetString(1),
            IpAddress = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
            Connected = reader.GetBoolean(3),
            PlcConnectionEnabled = reader.GetBoolean(4),
            TriggerActive = reader.GetBoolean(5),
            AckActive = reader.GetBoolean(6),
            HandshakeState = reader.IsDBNull(7) ? "Idle" : reader.GetString(7),
            LastError = reader.IsDBNull(8) ? null : reader.GetString(8),
            LastUpdateUtc = ReadUtc(reader, 9) ?? DateTimeOffset.UtcNow,
            OkCount = reader.IsDBNull(10) ? null : reader.GetInt32(10),
            NokCount = reader.IsDBNull(11) ? null : reader.GetInt32(11),
            NdtCount = reader.IsDBNull(12) ? null : reader.GetInt32(12),
            PoId = reader.IsDBNull(13) ? null : reader.GetInt32(13),
            SlitId = reader.IsDBNull(14) ? null : reader.GetInt32(14),
            CountsUpdatedUtc = ReadUtc(reader, 15),
            LineRunning = reader.IsDBNull(16) ? null : reader.GetBoolean(16),
            AccumulatedValue = reader.IsDBNull(17) ? null : reader.GetInt32(17),
            ThresholdValue = reader.IsDBNull(18) ? null : reader.GetInt32(18),
            HooterActive = reader.GetBoolean(19),
            StuckTriggerAlarm = reader.GetBoolean(20),
            AckWriteFailedAlarm = reader.GetBoolean(21),
            LastPoEnd = lastPoId is null && lastPoUtc is null
                ? null
                : new PlcHandshakeLastPoEnd
                {
                    PoId = lastPoId ?? 0,
                    NdtCountFinal = lastPoNdt ?? 0,
                    TimestampUtc = lastPoUtc ?? DateTimeOffset.UnixEpoch
                }
        };
    }

    private static DateTimeOffset? ReadUtc(SqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
            return null;
        var dt = reader.GetDateTime(ordinal);
        return DateTime.SpecifyKind(dt, DateTimeKind.Utc);
    }

    private static object ToUtcDb(DateTimeOffset? value) =>
        value is { } v ? v.UtcDateTime : DBNull.Value;

    private static string Trunc(string? value, int max)
    {
        var t = (value ?? string.Empty).Trim();
        if (t.Length <= max)
            return t;
        return t[..max];
    }
}
