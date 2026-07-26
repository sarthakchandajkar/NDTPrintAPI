namespace NdtBundleService.Services;

/// <summary>
/// Visibility guard when late Closed-PO traceability rows accumulate on a Manual_Recon-locked bundle.
/// </summary>
public static class PostReconCsvSumGuard
{
    /// <summary>
    /// True when the recomputed CSV slit sum exceeds the locked printed total by more than the configured margin.
    /// </summary>
    public static bool ShouldWarn(int lockedTotalPcs, int postReconCsvSum, int warnMarginPcs) =>
        postReconCsvSum > lockedTotalPcs + Math.Max(0, warnMarginPcs);
}
