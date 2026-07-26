using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// PLC-close vs eventual CSV sum: large disagreement escalates Manual_Review without correcting totals.
/// </summary>
public sealed class PlcCsvReconReviewEscalationTests
{
    private const int DefaultThresholdPercent = 20;
    private const string IncidentBundleNo = "1226100001";

    [Theory]
    [InlineData(18, 46)]
    [InlineData(18, 49)]
    public void Incident_replay_plc_18_eventual_csv_46_49_escalates_manual_review(int plcTotal, int slitSum)
    {
        Assert.True(PlcCsvReconReviewEscalation.ExceedsManualReviewThreshold(
            plcTotal,
            slitSum,
            DefaultThresholdPercent));

        var applied = PlcCsvReconSemantics.EvaluateFinalize(
            IncidentBundleNo,
            plcTotal,
            slitSum,
            printedAtUtc: new DateTime(2026, 7, 26, 8, 0, 0, DateTimeKind.Utc).AddMinutes(-200),
            reconWindowMinutes: 180,
            utcNow: new DateTime(2026, 7, 26, 12, 0, 0, DateTimeKind.Utc),
            force: false);

        Assert.True(applied.ClearsAwaitingCsvRecon);
        Assert.True(applied.CountDiscrepancy);
        Assert.False(applied.UpdatesStoredTotal);
        Assert.Equal(plcTotal, applied.PlcTotal);

        var effects = PlcCsvReconReviewEscalation.ComputeEffects(
            plcTotal,
            slitSum,
            applied.ClearsAwaitingCsvRecon,
            DefaultThresholdPercent);

        Assert.True(effects.CountDiscrepancy);
        Assert.True(effects.ManualReviewEscalated);
        Assert.False(effects.UpdatesStoredTotal);
    }

    [Fact]
    public void Under_threshold_discrepancy_sets_count_discrepancy_only_not_manual_review()
    {
        const int plcTotal = 18;
        const int slitSum = 21;

        Assert.False(PlcCsvReconReviewEscalation.ExceedsManualReviewThreshold(
            plcTotal,
            slitSum,
            DefaultThresholdPercent));

        var effects = PlcCsvReconReviewEscalation.ComputeEffects(
            plcTotal,
            slitSum,
            clearsAwaitingCsvRecon: true,
            DefaultThresholdPercent);

        Assert.True(effects.CountDiscrepancy);
        Assert.False(effects.ManualReviewEscalated);
        Assert.False(effects.UpdatesStoredTotal);
    }

    [Fact]
    public void Over_count_direction_also_escalates_when_above_threshold()
    {
        const int plcTotal = 56;
        const int slitSum = 40;

        Assert.True(PlcCsvReconReviewEscalation.ExceedsManualReviewThreshold(
            plcTotal,
            slitSum,
            DefaultThresholdPercent));

        var effects = PlcCsvReconReviewEscalation.ComputeEffects(
            plcTotal,
            slitSum,
            clearsAwaitingCsvRecon: true,
            DefaultThresholdPercent);

        Assert.True(effects.ManualReviewEscalated);
        Assert.False(effects.UpdatesStoredTotal);
    }

    [Fact]
    public void Matching_totals_never_escalate()
    {
        var effects = PlcCsvReconReviewEscalation.ComputeEffects(
            plcTotal: 18,
            slitSum: 18,
            clearsAwaitingCsvRecon: true,
            DefaultThresholdPercent);

        Assert.False(effects.CountDiscrepancy);
        Assert.False(effects.ManualReviewEscalated);
    }

    [Fact]
    public void Exactly_at_threshold_does_not_escalate()
    {
        const int plcTotal = 50;
        const int slitSum = 60;

        Assert.False(PlcCsvReconReviewEscalation.ExceedsManualReviewThreshold(
            plcTotal,
            slitSum,
            thresholdPercent: 20));
    }

    [Fact]
    public void Manual_review_not_set_before_recon_finalizes()
    {
        var effects = PlcCsvReconReviewEscalation.ComputeEffects(
            plcTotal: 18,
            slitSum: 49,
            clearsAwaitingCsvRecon: false,
            DefaultThresholdPercent);

        Assert.False(effects.ManualReviewEscalated);
    }
}
