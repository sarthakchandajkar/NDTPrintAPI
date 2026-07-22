using NdtBundleService.Configuration;

namespace NdtBundleService.Services.PoLifecycle;

/// <summary>Defense-in-depth guards for orphan auto-close (F-4 orphan sweep + backfill).</summary>
public static class OrphanSweepGuard
{
    /// <summary>
    /// Returns false when orphan auto-close must not run for this mill+PO (independent checks).
    /// Quiescence is measured from <paramref name="lastRowActivityUtc"/> (last slit contribution), not PO end time.
    /// </summary>
    public static bool ShouldSweepClosedPo(
        int millNo,
        string poNumber,
        PoLifecyclePhase phase,
        string? millRunningPo,
        DateTime lastRowActivityUtc,
        DateTime utcNow,
        int orphanQuiescenceMinutes)
    {
        if (phase == PoLifecyclePhase.Running)
            return false;

        if (phase != PoLifecyclePhase.Closed)
            return false;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (!string.IsNullOrWhiteSpace(millRunningPo)
            && InputSlitCsvParsing.PoEquals(po, millRunningPo))
            return false;

        if (lastRowActivityUtc == default)
            return false;

        var quiescence = TimeSpan.FromMinutes(Math.Max(0, orphanQuiescenceMinutes));
        if (utcNow - lastRowActivityUtc < quiescence)
            return false;

        return true;
    }
}
