using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Reconcile / printed-tags bundle list: hide in-progress open partials, keep settled bundles visible.
/// </summary>
public static class ReconcileBundleListFilter
{
    /// <summary>
    /// True when the bundle is the latest for its PO/mill and still accumulating toward threshold.
    /// </summary>
    public static bool IsOpenPartialLatest(int totalNdtPcs, int threshold, bool isLatest) =>
        isLatest && totalNdtPcs > 0 && totalNdtPcs < threshold;

    /// <summary>
    /// Printed or manual-recon bundles stay in the list even when count is below formation threshold.
    /// </summary>
    public static bool IsSettledForList(bool manualRecon, string? printStatus) =>
        manualRecon
        || string.Equals(printStatus, BundlePrintStatus.Printed, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Exclude from dropdown/list only when still an open partial and not settled.
    /// </summary>
    public static bool ShouldExcludeFromList(
        bool isLatest,
        int totalNdtPcs,
        int threshold,
        bool manualRecon,
        string? printStatus) =>
        IsOpenPartialLatest(totalNdtPcs, threshold, isLatest)
        && !IsSettledForList(manualRecon, printStatus);
}
