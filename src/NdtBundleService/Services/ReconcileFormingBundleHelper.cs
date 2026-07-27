using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Reconcile UI: include open bundles accumulating slit rows before final print/close.
/// </summary>
public static class ReconcileFormingBundleHelper
{
    /// <summary>
    /// Slit-only traceability row, pending print, or slit sum ahead of stored bundle total.
    /// </summary>
    public static bool IsForming(NdtBundleRecord bundle, int slitSum, bool slitOnlyInDatabase) =>
        slitOnlyInDatabase
        || string.Equals(bundle.PrintStatus, BundlePrintStatus.Pending, StringComparison.OrdinalIgnoreCase)
        || (slitSum > 0 && slitSum > bundle.TotalNdtPcs);

    /// <summary>
    /// Prefer live slit accumulation when reconcile is showing forming bundles.
    /// </summary>
    public static int ResolveDisplayTotal(NdtBundleRecord bundle, int slitSum, bool includeForming) =>
        includeForming ? Math.Max(bundle.TotalNdtPcs, slitSum) : bundle.TotalNdtPcs;
}
