namespace NdtBundleService.Services;

/// <summary>
/// Pure recon outcome for PLC-closed bundles vs late CSV slit sum.
/// Standard recon never rewrites <c>Total_NDT_Pcs</c> (printed PLC count stays until optional force-sync/reprint).
/// </summary>
public readonly record struct PlcCsvReconApplyResult(
    string BundleNo,
    int PlcTotal,
    int SlitSum,
    bool CountDiscrepancy,
    bool ClearsAwaitingCsvRecon,
    bool UpdatesStoredTotal);

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
        bool force) =>
        new(
            BundleNo: bundleNo,
            PlcTotal: plcTotal,
            SlitSum: slitSum,
            CountDiscrepancy: slitSum != plcTotal,
            ClearsAwaitingCsvRecon: force
                                   || PlcCsvReconFifo.ShouldFinalize(
                                       plcTotal,
                                       slitSum,
                                       printedAtUtc,
                                       reconWindowMinutes,
                                       utcNow),
            UpdatesStoredTotal: false);
}
