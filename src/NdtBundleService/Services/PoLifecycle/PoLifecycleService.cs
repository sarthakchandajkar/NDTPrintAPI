using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services.PoLifecycle;

/// <inheritdoc />
public sealed class PoLifecycleService : IPoLifecycleService
{
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly object _lock = new();
    private readonly Dictionary<string, Entry> _entries = new(StringComparer.OrdinalIgnoreCase);

    public PoLifecycleService(IOptionsMonitor<NdtBundleOptions> options)
    {
        _options = options;
    }

    public bool TryMarkDraining(int millNo, string poNumber, DateTime endedAtUtc)
    {
        if (!IsPlcLifecycleMill(millNo))
            return false;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po) || millNo is < 1 or > 4)
            return false;

        lock (_lock)
        {
            _entries[MakeKey(millNo, po)] = new Entry
            {
                MillNo = millNo,
                PoNumber = po,
                EndedAtUtc = endedAtUtc,
                Phase = PoLifecyclePhase.Draining
            };
        }

        return true;
    }

    public bool TryMarkClosed(int millNo, string poNumber)
    {
        if (!IsPlcLifecycleMill(millNo))
            return false;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po) || millNo is < 1 or > 4)
            return false;

        lock (_lock)
        {
            if (!_entries.TryGetValue(MakeKey(millNo, po), out var entry))
                return false;

            entry.Phase = PoLifecyclePhase.Closed;
            return true;
        }
    }

    public PoLifecyclePhase GetPhase(int millNo, string poNumber)
    {
        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po) || millNo is < 1 or > 4)
            return PoLifecyclePhase.Running;

        lock (_lock)
        {
            return _entries.TryGetValue(MakeKey(millNo, po), out var entry)
                ? entry.Phase
                : PoLifecyclePhase.Running;
        }
    }

    public IReadOnlyList<PoLifecycleDrainEntry> GetExpiredDrains(DateTime utcNow, TimeSpan drainWindow)
    {
        lock (_lock)
        {
            return _entries.Values
                .Where(e => e.Phase == PoLifecyclePhase.Draining && utcNow - e.EndedAtUtc >= drainWindow)
                .Select(e => new PoLifecycleDrainEntry(e.MillNo, e.PoNumber, e.EndedAtUtc, e.Phase))
                .ToList();
        }
    }

    public IReadOnlyList<PoLifecycleDrainEntry> GetClosedEntries()
    {
        lock (_lock)
        {
            return _entries.Values
                .Where(e => e.Phase == PoLifecyclePhase.Closed)
                .Select(e => new PoLifecycleDrainEntry(e.MillNo, e.PoNumber, e.EndedAtUtc, e.Phase))
                .ToList();
        }
    }

    private bool IsPlcLifecycleMill(int millNo) =>
        MillPoEndSourceResolver.ForMill(millNo, _options.CurrentValue) == MillPoEndSource.Plc;

    private static string MakeKey(int millNo, string po) => $"{millNo}|{po}";

    private sealed class Entry
    {
        public int MillNo { get; set; }
        public string PoNumber { get; set; } = string.Empty;
        public DateTime EndedAtUtc { get; set; }
        public PoLifecyclePhase Phase { get; set; }
    }
}
