using NdtBundleService.Services.PoLifecycle;

namespace NdtBundleService.Tests;

/// <summary>Minimal lifecycle stub for controller unit tests (defaults to Running).</summary>
internal sealed class TestPoLifecycleStub : IPoLifecycleService
{
    public PoLifecyclePhase Phase { get; set; } = PoLifecyclePhase.Running;

    public bool TryMarkDraining(int millNo, string poNumber, DateTime endedAtUtc) => false;
    public bool TryMarkClosed(int millNo, string poNumber) => false;
    public bool TryReopen(int millNo, string poNumber) => false;
    public bool TryMarkResumeCandidate(int millNo, string poNumber) => false;
    public bool IsResumeCandidate(int millNo, string poNumber) => false;
    public PoLifecyclePhase GetPhase(int millNo, string poNumber) => Phase;
    public IReadOnlyList<PoLifecycleDrainEntry> GetExpiredDrains(DateTime utcNow, TimeSpan drainWindow) =>
        Array.Empty<PoLifecycleDrainEntry>();
    public IReadOnlyList<PoLifecycleDrainEntry> GetClosedEntries() => Array.Empty<PoLifecycleDrainEntry>();
}
