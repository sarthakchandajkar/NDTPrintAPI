using NdtBundleService.Configuration;
using NdtBundleService.Services.PoLifecycle;

namespace NdtBundleService.Services;

/// <summary>
/// Routes slit rows for lifecycle-Closed POs to traceability-only ingestion (F-4.4 / post-close L2 lag).
/// Independent of <c>Manual_Recon</c> — the PO slot itself is settled once marked Closed.
/// </summary>
public static class ClosedPoSlitIngestPolicy
{
    /// <summary>True when slit rows must not open or accumulate bundles (Plc mills in Closed phase).</summary>
    public static bool ShouldRouteTraceabilityOnly(PoLifecyclePhase phase, MillPoEndSource poEndSource) =>
        poEndSource == MillPoEndSource.Plc && phase == PoLifecyclePhase.Closed;

    /// <summary>
    /// Bundling action for one slit row. Applies Closed-PO traceability routing on both real-time and backfill paths.
    /// </summary>
    public static BackfillBundlingAction DecideForRow(
        bool isBackfill,
        BackfillCoverageKind coverage,
        PoLifecyclePhase phase,
        MillPoEndSource poEndSource,
        bool autoCloseOrphanBundles)
    {
        if (ShouldRouteTraceabilityOnly(phase, poEndSource))
        {
            return coverage == BackfillCoverageKind.Ambiguous
                ? BackfillBundlingAction.ManualReview
                : BackfillBundlingAction.TraceabilityOnly;
        }

        if (!isBackfill)
            return BackfillBundlingAction.NormalBundle;

        return InputSlitBackfillPolicy.Decide(coverage, phase, poEndSource, autoCloseOrphanBundles);
    }
}
