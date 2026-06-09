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
            if (_byMill.Count == 0)
                return false;
            return _byMill.Values.All(m => m.Connected);
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
