using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Reconcile UI: include open bundles accumulating slit rows before final print/close.
/// </summary>
public static class ReconcileFormingBundleHelper
{
    /// <summary>
    /// Forming = the bundle has not produced its printed tag yet: slit-only traceability rows,
    /// a close still awaiting print (Pending), or a placeholder parent row with no print evidence.
    /// A bundle whose tag was printed (or a print was attempted) is formed — late traceability
    /// attaches pushing the slit sum above the tag total must NOT flip it back to Forming
    /// (2026-07-28 incident: Full/Partial bundles stuck "Forming"); those surface via
    /// PoMismatch / Post_Recon_Csv_Sum warnings instead.
    /// </summary>
    public static bool IsForming(NdtBundleRecord bundle, int slitSum, bool slitOnlyInDatabase)
    {
        // Operator-locked total from manual bundle reconcile — never treat as forming.
        if (bundle.ManualRecon)
            return false;

        if (slitOnlyInDatabase)
            return true;

        if (string.Equals(bundle.PrintStatus, BundlePrintStatus.Pending, StringComparison.OrdinalIgnoreCase))
            return true;

        // Placeholder NDT_Bundle parent rows (created ahead of Output_Slit_Row writes/corrections)
        // carry no print status and no PrintedAt — their bundle is still accumulating.
        return string.IsNullOrWhiteSpace(bundle.PrintStatus) && bundle.PrintedAt is null;
    }

    /// <summary>
    /// Forming bundles show live slit accumulation; formed bundles show the printed tag total.
    /// </summary>
    public static int ResolveDisplayTotal(NdtBundleRecord bundle, int slitSum, bool isForming) =>
        bundle.ManualRecon || !isForming
            ? bundle.TotalNdtPcs
            : Math.Max(bundle.TotalNdtPcs, slitSum);
}
