using NdtBundleService.Services;
using NdtBundleService.Services.PoLifecycle;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class CsvFillSequenceAdvanceTests
{
    [Theory]
    [InlineData(CsvFillState.CsvComplete, false, true)]
    [InlineData(CsvFillState.CsvOvershoot, false, true)]
    [InlineData(CsvFillState.CsvShort, false, true)]
    [InlineData(CsvFillState.CsvComplete, true, false)]
    [InlineData(CsvFillState.CsvOvershoot, true, false)]
    [InlineData(CsvFillState.CsvShort, true, false)]
    [InlineData(CsvFillState.CsvFilling, false, false)]
    [InlineData(CsvFillState.PlcClosed, false, false)]
    public void ShouldOpenNextStampTarget_matches_option_a(
        string fillState,
        bool anotherIncomplete,
        bool expected) =>
        Assert.Equal(expected, CsvFillSequenceAdvance.ShouldOpenNextStampTarget(fillState, anotherIncomplete));

    [Theory]
    [InlineData(15, 10, CsvFillState.CsvShort)]
    [InlineData(15, 15, CsvFillState.CsvComplete)]
    [InlineData(15, 16, CsvFillState.CsvOvershoot)]
    [InlineData(11, 0, CsvFillState.CsvShort)]
    [InlineData(0, 0, CsvFillState.PlcClosed)]
    [InlineData(0, 2, CsvFillState.CsvFilling)]
    public void ResolveForceReleaseFillState_releases_underfilled_remainder(
        int target,
        int filled,
        string expected) =>
        Assert.Equal(expected, CsvFillSequenceAdvance.ResolveForceReleaseFillState(target, filled));

    [Theory]
    [InlineData(16, 15, 16, 16, CsvFillState.CsvComplete)]
    [InlineData(16, 10, 11, 10, CsvFillState.CsvFilling)]
    [InlineData(15, 15, 15, 15, CsvFillState.CsvComplete)]
    [InlineData(15, 16, 16, 16, CsvFillState.CsvOvershoot)]
    [InlineData(0, 2, 2, 2, CsvFillState.CsvFilling)]
    public void ResolveManualReconcileFill_uses_slit_sum_when_matched(
        int corrected,
        int csvFilled,
        int slitSum,
        int expectedFilled,
        string expectedState)
    {
        var (filled, state) = CsvFillSequenceAdvance.ResolveManualReconcileFill(corrected, csvFilled, slitSum);
        Assert.Equal(expectedFilled, filled);
        Assert.Equal(expectedState, state);
    }

    [Fact]
    public void ResolveProvisionalTarget_prefers_manual_recon_original() =>
        Assert.Equal(20, CsvFillSequenceAdvance.ResolveProvisionalTarget(20, 18, 18));

    [Fact]
    public void ResolveProvisionalTarget_falls_back_to_target_then_total()
    {
        Assert.Equal(22, CsvFillSequenceAdvance.ResolveProvisionalTarget(null, 22, 20));
        Assert.Equal(15, CsvFillSequenceAdvance.ResolveProvisionalTarget(null, null, 15));
    }

    [Fact]
    public void CloseSource_CsvAdvance_constant() =>
        Assert.Equal("CsvAdvance", BundleCloseSource.CsvAdvance);

    [Theory]
    [InlineData(PoLifecyclePhase.Running, "1000062545", "1000062545", false, true)]
    [InlineData(PoLifecyclePhase.Running, "1000062545", "1000062550", false, false)]
    [InlineData(PoLifecyclePhase.Closed, "1000062545", "1000062545", false, false)]
    [InlineData(PoLifecyclePhase.Draining, "1000062545", "1000062545", false, false)]
    [InlineData(PoLifecyclePhase.Running, "1000062545", "1000062545", true, false)]
    [InlineData(PoLifecyclePhase.Running, "1000062545", null, false, true)]
    [InlineData(PoLifecyclePhase.Running, "1000062545", "", false, true)]
    [InlineData(PoLifecyclePhase.Running, null, "1000062545", false, false)]
    public void IsEligiblePoForCsvAdvance_blocks_finished_and_prior_pos(
        PoLifecyclePhase phase,
        string? bundlePo,
        string? runningPo,
        bool waitingForNewWip,
        bool expected) =>
        Assert.Equal(
            expected,
            CsvFillSequenceAdvance.IsEligiblePoForCsvAdvance(
                phase,
                bundlePo,
                runningPo,
                waitingForNewWip));

    [Theory]
    [InlineData(BundleCloseSource.CsvAdvance, CsvFillState.PlcClosed, 0, false, "1000062601", "1000062605", true)]
    [InlineData(BundleCloseSource.CsvAdvance, CsvFillState.CsvFilling, 0, false, "1000062601", "1000062605", true)]
    [InlineData(BundleCloseSource.CsvAdvance, CsvFillState.CsvShort, 0, false, "1000062601", "1000062605", true)]
    [InlineData(BundleCloseSource.CsvAdvance, CsvFillState.PlcClosed, 3, false, "1000062601", "1000062605", false)]
    [InlineData(BundleCloseSource.CsvAdvance, CsvFillState.PlcClosed, 0, false, "1000062605", "1000062605", false)]
    [InlineData(BundleCloseSource.Plc, CsvFillState.PlcClosed, 0, false, "1000062601", "1000062605", false)]
    [InlineData(BundleCloseSource.CsvAdvance, CsvFillState.CsvComplete, 0, false, "1000062601", "1000062605", false)]
    [InlineData(BundleCloseSource.CsvAdvance, CsvFillState.PlcClosed, 0, true, "1000062601", "1000062605", false)]
    public void IsEmptyOrphanCsvAdvanceStamp_matches_phantom_for_other_po(
        string? closeSource,
        string? fillState,
        int csvFilled,
        bool voided,
        string? stampPo,
        string? keepPo,
        bool expected) =>
        Assert.Equal(
            expected,
            CsvFillSequenceAdvance.IsEmptyOrphanCsvAdvanceStamp(
                closeSource, fillState, csvFilled, voided, stampPo, keepPo));

    [Theory]
    [InlineData(CsvFillState.PlcClosed, true)]
    [InlineData(CsvFillState.CsvFilling, true)]
    [InlineData(CsvFillState.CsvComplete, false)]
    [InlineData(CsvFillState.CsvOvershoot, false)]
    [InlineData(CsvFillState.CsvShort, false)]
    [InlineData(CsvFillState.Voided, false)]
    [InlineData(null, false)]
    public void IsClaimableOpenFillState_matches_incomplete_fill(string? state, bool expected) =>
        Assert.Equal(expected, CsvFillSequenceAdvance.IsClaimableOpenFillState(state));

    [Fact]
    public void ClaimCloseSourcePriority_prefers_CsvAdvance()
    {
        Assert.True(
            CsvFillSequenceAdvance.ClaimCloseSourcePriority(BundleCloseSource.CsvAdvance)
            < CsvFillSequenceAdvance.ClaimCloseSourcePriority(BundleCloseSource.Plc));
        Assert.True(
            CsvFillSequenceAdvance.ClaimCloseSourcePriority(BundleCloseSource.CsvAdvance)
            < CsvFillSequenceAdvance.ClaimCloseSourcePriority(null));
    }

    [Theory]
    [InlineData(3, 4, true)]
    [InlineData(4, 4, false)]
    [InlineData(5, 4, false)]
    [InlineData(3, 0, false)]
    public void ShouldRaiseMillSequenceToClaimed_when_sequence_lags(
        int current,
        int claimed,
        bool expected) =>
        Assert.Equal(expected, CsvFillSequenceAdvance.ShouldRaiseMillSequenceToClaimed(current, claimed));
}
