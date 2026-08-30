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

    /// <summary>
    /// Fill-to-target terminal-row gate: hold unpublished, flag <c>Manual_Review</c>, do not stamp
    /// the next incomplete bundle.
    /// </summary>
    HoldReview = 3
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
            // Closed PO late files: traceability-only (see ClosedPoSlitIngestPolicy). Orphan flush at PO end
            // is handled by PoLifecycleSweepWorker, not by opening new bundles on late ingest.
            return BackfillBundlingAction.TraceabilityOnly;
        }

        // Running / Draining + no coverage
        return BackfillBundlingAction.NormalBundle;
    }

    /// <summary>
    /// SQL printed-bundle lookup for F-5 coverage. A lookup failure must not stay <see cref="BackfillCoverageKind.None"/>
    /// (that would stamp the next incomplete on a transient SQL error).
    /// </summary>
    public static BackfillCoverageKind ApplySqlPrintedLookup(
        BackfillCoverageKind current,
        bool sqlLookupFailed,
        bool hasPrintedBundle)
    {
        if (current != BackfillCoverageKind.None)
            return current;

        return sqlLookupFailed || hasPrintedBundle
            ? BackfillCoverageKind.Ambiguous
            : BackfillCoverageKind.None;
    }

    /// <summary>
    /// Fail-closed mapping for <c>HasPrintedBundleForPoAsync</c>: a lookup error is treated as present
    /// so callers cannot downgrade coverage to <see cref="BackfillCoverageKind.None"/>.
    /// </summary>
    public static bool FailClosedHasPrintedBundle(bool found, bool lookupFailed) =>
        lookupFailed || found;
}

/// <summary>
/// Fill-to-target gate for F-5 backfill only: completed bundles exclude the current fill pointer;
/// they are never stamp targets. Live ingest does not call this.
/// </summary>
public static class InputSlitBackfillFillGate
{
    public static bool ShouldApply(
        bool isBackfill,
        BackfillBundlingAction action,
        MillPoEndSource poEndSource,
        bool isFillToTarget) =>
        isBackfill
        && action == BackfillBundlingAction.NormalBundle
        && poEndSource == MillPoEndSource.Plc
        && isFillToTarget;

    /// <summary>
    /// True → hold unpublished + Manual_Review (do not call the fill assigner).
    /// False → stamp the oldest incomplete via the existing assigner.
    /// </summary>
    public static bool ShouldHoldRatherThanStamp(
        DateTime fileLastWriteUtc,
        bool hasTerminalFillRow,
        DateTime? oldestIncompletePrintedAtUtc)
    {
        if (!hasTerminalFillRow)
            return false;

        if (oldestIncompletePrintedAtUtc is null)
            return true;

        return ToUtc(fileLastWriteUtc) < ToUtc(oldestIncompletePrintedAtUtc.Value);
    }

    private static DateTime ToUtc(DateTime value) =>
        value.Kind == DateTimeKind.Utc ? value : value.ToUniversalTime();
}
