namespace NdtBundleService.Services;

/// <summary>
/// Pure recon outcome helpers retained for manual-reconcile force-finalize compatibility.
/// FIFO awaiting recon is deleted; fill-to-target owns CSV completion state.
/// </summary>
public readonly record struct PlcCsvReconApplyResult(
    string BundleNo,
    int PlcTotal,
    int SlitSum,
    bool CountDiscrepancy,
    bool ClearsAwaitingCsvRecon,
    bool UpdatesStoredTotal);

/// <summary>Legacy shape kept for repository compile compatibility; list APIs return empty.</summary>
public sealed record PlcCsvReconAwaitingBundle(
    string BundleNo,
    int EngineSequence,
    int PlcTotal,
    int CurrentSlitSum,
    DateTime PrintedAtUtc);

public static class PlcCsvReconSemantics
{
    public static PlcCsvReconApplyResult Evaluate(string bundleNo, int plcTotal, int slitSum) =>
        new(
            BundleNo: bundleNo,
            PlcTotal: plcTotal,
            SlitSum: slitSum,
            CountDiscrepancy: slitSum != plcTotal,
            ClearsAwaitingCsvRecon: true,
            UpdatesStoredTotal: false);

    public static PlcCsvReconApplyResult EvaluateFinalize(
        string bundleNo,
        int plcTotal,
        int slitSum,
        DateTime printedAtUtc,
        int reconWindowMinutes,
        DateTime utcNow,
        bool force)
    {
        var windowExpired = reconWindowMinutes > 0
            && (utcNow - printedAtUtc).TotalMinutes >= reconWindowMinutes;
        var countMet = slitSum >= plcTotal;
        return new(
            BundleNo: bundleNo,
            PlcTotal: plcTotal,
            SlitSum: slitSum,
            CountDiscrepancy: slitSum != plcTotal,
            ClearsAwaitingCsvRecon: force || countMet || windowExpired,
            UpdatesStoredTotal: false);
    }
}
