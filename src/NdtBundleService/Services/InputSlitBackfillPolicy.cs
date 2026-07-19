using NdtBundleService.Configuration;
using NdtBundleService.Services.PoLifecycle;

namespace NdtBundleService.Services;

/// <summary>Disk/SQL coverage of a backfilled Input Slit file relative to existing NDT bundle artifacts.</summary>
public enum BackfillCoverageKind
{
    /// <summary>No existing bundle CSV / printed artifact for the file's PO+mill rows.</summary>
    None = 0,

    /// <summary>Per-slit NDT output for this source basename already has NDT Batch No values.</summary>
    ExactMatch = 1,

    /// <summary>PO+mill has bundle artifacts on disk/SQL but rows are not clearly attributable.</summary>
    Ambiguous = 2
}

/// <summary>Bundling action for one backfilled slit row (F-5.2).</summary>
public enum BackfillBundlingAction
{
    /// <summary>Feed the normal <c>ProcessSlitRecordAsync</c> path (Running/Draining, no coverage).</summary>
    NormalBundle = 0,

    /// <summary>Record traceability only; do not print or allocate a new close.</summary>
    TraceabilityOnly = 1,

    /// <summary>Traceability only + flag <c>Manual_Review</c>; do not print.</summary>
    ManualReview = 2,

    /// <summary>Closed PO: allow bundling then immediately close-and-print via orphan policy.</summary>
    OrphanAutoClose = 3
}

/// <summary>Pure F-5.2 / F-4.4 decision helpers for Input Slit backfill.</summary>
public static class InputSlitBackfillPolicy
{
    /// <summary>
    /// Legacy NULL <c>Source_LastWriteTimeUtc</c> = imported at any version.
    /// Otherwise imported only when stored write time is at least the file's current <c>LastWriteTimeUtc</c>.
    /// </summary>
    public static bool IsStoredVersionSufficient(DateTime? storedLastWriteTimeUtc, DateTime fileLastWriteTimeUtc)
    {
        if (!storedLastWriteTimeUtc.HasValue)
            return true;

        // Compare at 100ns tick precision; SQL datetime2(2) rounds — callers should pass truncated values when needed.
        return storedLastWriteTimeUtc.Value >= fileLastWriteTimeUtc;
    }

    public static BackfillBundlingAction Decide(
        BackfillCoverageKind coverage,
        PoLifecyclePhase phase,
        MillPoEndSource poEndSource,
        bool autoCloseOrphanBundles)
    {
        // File / TcpOpen mills: keep historical bundling; only skip print when ExactMatch (already on disk).
        if (poEndSource != MillPoEndSource.Plc)
        {
            return coverage == BackfillCoverageKind.ExactMatch
                ? BackfillBundlingAction.TraceabilityOnly
                : BackfillBundlingAction.NormalBundle;
        }

        if (coverage == BackfillCoverageKind.ExactMatch)
            return BackfillBundlingAction.TraceabilityOnly;

        if (coverage == BackfillCoverageKind.Ambiguous)
            return BackfillBundlingAction.ManualReview;

        if (phase == PoLifecyclePhase.Closed)
        {
            return autoCloseOrphanBundles
                ? BackfillBundlingAction.OrphanAutoClose
                : BackfillBundlingAction.ManualReview;
        }

        // Running / Draining + no coverage
        return BackfillBundlingAction.NormalBundle;
    }
}
