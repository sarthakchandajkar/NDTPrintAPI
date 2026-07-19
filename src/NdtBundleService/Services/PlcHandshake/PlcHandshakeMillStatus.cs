namespace NdtBundleService.Services.PlcHandshake;

/// <summary>Last PO-end event captured during handshake (for dashboard).</summary>
public sealed class PlcHandshakeLastPoEnd
{
    public int PoId { get; set; }

    public int NdtCountFinal { get; set; }

    public DateTimeOffset TimestampUtc { get; set; }
}

/// <summary>Live status for one mill handshake loop (updated by <see cref="PlcHandshakeService"/>).</summary>
public sealed class PlcHandshakeMillStatus
{
    public string MillName { get; init; } = string.Empty;

    public int MillNo { get; init; }

    public string IpAddress { get; init; } = string.Empty;

    public bool Connected { get; set; }

    /// <summary>When false, the handshake loop is running but S7 connect/reconnect is suppressed (manual or config).</summary>
    public bool PlcConnectionEnabled { get; set; } = true;

    public bool TriggerActive { get; set; }

    public bool AckActive { get; set; }

    public string HandshakeState { get; set; } = "Idle";

    public DateTimeOffset? LastPoChangeUtc { get; set; }

    public string? LastError { get; set; }

    public DateTimeOffset LastUpdateUtc { get; set; } = DateTimeOffset.UtcNow;

    public int? OkCount { get; set; }

    public int? NokCount { get; set; }

    /// <summary>NDT count for dashboard (zeroed after PO end until PO ID changes).</summary>
    public int? NdtCount { get; set; }

    public int? PoId { get; set; }

    public int? SlitId { get; set; }

    public DateTimeOffset? CountsUpdatedUtc { get; set; }

    /// <summary>DB250.DBX2.0 — true when the mill line is running.</summary>
    public bool? LineRunning { get; set; }

    /// <summary>MW56 — accumulated value (Mill-1 hooter).</summary>
    public int? AccumulatedValue { get; set; }

    /// <summary>MW58 — threshold value (Mill-1 hooter).</summary>
    public int? ThresholdValue { get; set; }

    /// <summary>Q6.7 hooter output is currently ON (software pulse).</summary>
    public bool HooterActive { get; set; }

    /// <summary>F-6.3: trigger latched beyond StuckTriggerAlarmSeconds without completed handshake.</summary>
    public bool StuckTriggerAlarm { get; set; }

    /// <summary>F-6.3: ack merker write failed after retries.</summary>
    public bool AckWriteFailedAlarm { get; set; }

    public PlcHandshakeLastPoEnd? LastPoEnd { get; set; }
}

/// <summary>Thread-safe aggregate status for dashboard / <see cref="PlcHandshakeMirrorPlcClient"/>.</summary>
public sealed class PlcHandshakeStatusRegistry
{
    private readonly object _sync = new();
    private readonly Dictionary<int, PlcHandshakeMillStatus> _byMill = new();

    public void RegisterMill(int millNo, PlcHandshakeMillStatus status)
    {
        if (millNo is < 1 or > 4)
            return;
        lock (_sync)
            _byMill[millNo] = status;
    }

    public void UpdateMill(int millNo, Action<PlcHandshakeMillStatus> apply)
    {
        if (millNo is < 1 or > 4 || apply is null)
            return;
        lock (_sync)
        {
            if (_byMill.TryGetValue(millNo, out var st))
            {
                apply(st);
                st.LastUpdateUtc = DateTimeOffset.UtcNow;
            }
        }
    }

    public bool TryGetMill(int millNo, out PlcHandshakeMillStatus? status)
    {
        lock (_sync)
        {
            if (_byMill.TryGetValue(millNo, out var st))
            {
                status = st;
                return true;
            }
        }

        status = null;
        return false;
    }

    public IReadOnlyDictionary<int, bool> GetPoEndByMill()
    {
        lock (_sync)
        {
            var result = new Dictionary<int, bool> { [1] = false, [2] = false, [3] = false, [4] = false };
            foreach (var (millNo, st) in _byMill)
                result[millNo] = st.TriggerActive;
            return result;
        }
    }

    public bool AllConnected()
    {
        lock (_sync)
        {
            var active = _byMill.Values.Where(m => m.PlcConnectionEnabled).ToList();
            if (active.Count == 0)
                return _byMill.Count > 0;
            return active.All(m => m.Connected);
        }
    }

    public string? FirstError()
    {
        lock (_sync)
            return _byMill.Values.Select(m => m.LastError).FirstOrDefault(e => !string.IsNullOrWhiteSpace(e));
    }

    public IReadOnlyList<PlcHandshakeMillStatus> GetSnapshot()
    {
        lock (_sync)
            return _byMill.Values
                .OrderBy(m => m.MillNo)
                .Select(CloneStatus)
                .ToList();
    }

    private static PlcHandshakeMillStatus CloneStatus(PlcHandshakeMillStatus m) =>
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
}
