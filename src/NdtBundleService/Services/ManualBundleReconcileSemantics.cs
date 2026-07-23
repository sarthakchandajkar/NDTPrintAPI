namespace NdtBundleService.Services;

/// <summary>
/// Pure outcome for atomic manual bundle reconcile (PLC force-finalize metadata + operator lock).
/// </summary>
public readonly record struct ManualBundleReconcileApplyResult(
    bool ForceFinalized,
    bool CountDiscrepancy,
    int PlcTotalAtFinalize,
    int SlitSumAtFinalize);

public static class ManualBundleReconcileSemantics
{
    /// <summary>
    /// When a PLC-closed bundle is still awaiting CSV recon, force-finalize clears awaiting and may log discrepancy.
    /// File-closed bundles (<paramref name="closeSource"/> not Plc) or already-settled bundles are no-ops.
    /// </summary>
    public static ManualBundleReconcileApplyResult EvaluateForceFinalize(
        bool awaitingCsvRecon,
        string? closeSource,
        int plcTotal,
        int slitSum)
    {
        var isPlcAwaiting = awaitingCsvRecon
                            && string.Equals(closeSource, "Plc", StringComparison.OrdinalIgnoreCase);
        if (!isPlcAwaiting)
        {
            return new ManualBundleReconcileApplyResult(
                ForceFinalized: false,
                CountDiscrepancy: false,
                PlcTotalAtFinalize: plcTotal,
                SlitSumAtFinalize: slitSum);
        }

        return new ManualBundleReconcileApplyResult(
            ForceFinalized: true,
            CountDiscrepancy: slitSum != plcTotal,
            PlcTotalAtFinalize: plcTotal,
            SlitSumAtFinalize: slitSum);
    }
}
