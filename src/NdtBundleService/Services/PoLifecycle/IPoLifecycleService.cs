namespace NdtBundleService.Services.PoLifecycle;

/// <summary>
/// Tracks Running → Draining → Closed for mills with <c>PoEndSource=Plc</c>.
/// File mills never enter this store (byte-for-byte historical PO-end behavior).
/// </summary>
public interface IPoLifecycleService
{
    /// <summary>Marks the PO as draining for a Plc mill. No-op for File mills.</summary>
    bool TryMarkDraining(int millNo, string poNumber, DateTime endedAtUtc);

    /// <summary>Marks the PO closed after drain flush/sweep. No-op when not draining.</summary>
    bool TryMarkClosed(int millNo, string poNumber);

    /// <summary>Reopens a Closed PO for hold/resume cycles. No-op when not Closed or not a Plc mill.</summary>
    bool TryReopen(int millNo, string poNumber);

    /// <summary>Marks a Closed PO as a resume candidate (slit hint; reopen still requires WIP confirmation).</summary>
    bool TryMarkResumeCandidate(int millNo, string poNumber);

    bool IsResumeCandidate(int millNo, string poNumber);

    PoLifecyclePhase GetPhase(int millNo, string poNumber);

    /// <summary>Draining entries whose drain window has expired (Plc mills only).</summary>
    IReadOnlyList<PoLifecycleDrainEntry> GetExpiredDrains(DateTime utcNow, TimeSpan drainWindow);

    /// <summary>All Closed mill+PO keys currently tracked (for orphan sweep).</summary>
    IReadOnlyList<PoLifecycleDrainEntry> GetClosedEntries();
}

public sealed record PoLifecycleDrainEntry(int MillNo, string PoNumber, DateTime EndedAtUtc, PoLifecyclePhase Phase);
