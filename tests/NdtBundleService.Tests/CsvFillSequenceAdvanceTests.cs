using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class CsvFillSequenceAdvanceTests
{
    [Theory]
    [InlineData(CsvFillState.CsvComplete, false, true)]
    [InlineData(CsvFillState.CsvOvershoot, false, true)]
    [InlineData(CsvFillState.CsvComplete, true, false)]
    [InlineData(CsvFillState.CsvOvershoot, true, false)]
    [InlineData(CsvFillState.CsvFilling, false, false)]
    [InlineData(CsvFillState.PlcClosed, false, false)]
    public void ShouldOpenNextStampTarget_matches_option_a(
        string fillState,
        bool anotherIncomplete,
        bool expected) =>
        Assert.Equal(expected, CsvFillSequenceAdvance.ShouldOpenNextStampTarget(fillState, anotherIncomplete));

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
}
