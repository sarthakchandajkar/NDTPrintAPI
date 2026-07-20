using NdtBundleService.Services.PoLifecycle;

namespace NdtBundleService.Services.PlcHandshake.PlcPoEnd;

/// <summary>
/// Decides whether a PLC PO-end edge should run the workflow or be ack-only
/// (handshake already serviced the bit; avoid re-latch loops while Draining/Closed).
/// </summary>
public static class PlcPoEndWorkflowGate
{
    public enum Decision
    {
        RunWorkflow = 0,
        AckOnlySkip = 1
    }

    public static Decision Decide(bool poResolved, PoLifecyclePhase phase)
    {
        if (!poResolved)
            return Decision.AckOnlySkip;

        if (phase is PoLifecyclePhase.Draining or PoLifecyclePhase.Closed)
            return Decision.AckOnlySkip;

        return Decision.RunWorkflow;
    }
}

/// <summary>Rate-limits repeated ack-only skip warnings (one WRN per key per window).</summary>
public sealed class PlcPoEndAckOnlyRateLimiter
{
    private readonly object _gate = new();
    private readonly Dictionary<string, DateTime> _lastWarnUtc = new(StringComparer.OrdinalIgnoreCase);
    private readonly TimeSpan _window;

    public PlcPoEndAckOnlyRateLimiter(TimeSpan? window = null)
    {
        _window = window ?? TimeSpan.FromMinutes(5);
    }

    public bool ShouldLog(string key, DateTime utcNow)
    {
        lock (_gate)
        {
            if (_lastWarnUtc.TryGetValue(key, out var last) && utcNow - last < _window)
                return false;
            _lastWarnUtc[key] = utcNow;
            return true;
        }
    }
}
