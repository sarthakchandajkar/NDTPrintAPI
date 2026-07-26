namespace NdtBundleService.Services;

/// <summary>
/// When a PLC-closed bundle's eventual slit-file sum disagrees with the printed PLC total beyond a
/// configurable percent, flag <c>Manual_Review</c> for human follow-up (never auto-correct totals).
/// </summary>
public readonly record struct PlcCsvReconFinalizeEffects(
    bool CountDiscrepancy,
    bool ManualReviewEscalated,
    bool UpdatesStoredTotal);

public static class PlcCsvReconReviewEscalation
{
    /// <summary>
    /// True when <paramref name="slitSum"/> differs from <paramref name="plcTotal"/> by more than
    /// <paramref name="thresholdPercent"/> percent of the PLC-close total (symmetric over/under).
    /// </summary>
    public static bool ExceedsManualReviewThreshold(int plcTotal, int slitSum, int thresholdPercent)
    {
        if (plcTotal == slitSum)
            return false;

        if (thresholdPercent < 0)
            thresholdPercent = 0;

        if (plcTotal <= 0)
            return slitSum != 0;

        var delta = Math.Abs(slitSum - plcTotal);
        return delta * 100.0 / plcTotal > thresholdPercent;
    }

    public static PlcCsvReconFinalizeEffects ComputeEffects(
        int plcTotal,
        int slitSum,
        bool clearsAwaitingCsvRecon,
        int thresholdPercent) =>
        new(
            CountDiscrepancy: slitSum != plcTotal,
            ManualReviewEscalated: clearsAwaitingCsvRecon
                                   && ExceedsManualReviewThreshold(plcTotal, slitSum, thresholdPercent),
            UpdatesStoredTotal: false);
}
