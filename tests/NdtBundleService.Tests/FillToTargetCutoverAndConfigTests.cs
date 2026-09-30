using NdtBundleService.Configuration;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class FillToTargetCutoverAndConfigTests
{
    [Fact]
    public void MillCsvBatchMode_defaults_constant_for_mills_2_to_4()
    {
        var opt = new NdtBundleOptions();
        Assert.True(MillCsvBatchModeResolver.Resolve(opt, 1).IsFillToTarget);
        Assert.True(MillCsvBatchModeResolver.Resolve(opt, 2).IsConstant);
        Assert.Equal("10001", MillCsvBatchModeResolver.Resolve(opt, 2).Value);
        Assert.True(MillCsvBatchModeResolver.Resolve(opt, 3).IsConstant);
        Assert.True(MillCsvBatchModeResolver.Resolve(opt, 4).IsConstant);
    }

    [Fact]
    public void MillCsvBatchMode_config_can_enable_fill_for_mill_2()
    {
        var opt = new NdtBundleOptions
        {
            MillCsvBatchMode =
            {
                ["2"] = new MillCsvBatchModeEntry { Mode = "FillToTarget" }
            }
        };
        Assert.True(MillCsvBatchModeResolver.Resolve(opt, 2).IsFillToTarget);
    }

    [Fact]
    public void MillCsvBatchMode_zero_ndt_defaults_10001_hollow_defaults_empty_all_mills()
    {
        var opt = new NdtBundleOptions();
        for (var m = 1; m <= 4; m++)
        {
            var entry = MillCsvBatchModeResolver.Resolve(opt, m);
            Assert.Equal("10001", entry.ZeroNdtValue);
            Assert.Equal("", entry.HollowFgValue);
            var (hollowCsv, hollowLink) = MillCsvBatchModeResolver.ResolveNonFillCsvBatch(entry, isHollowFg: true, ndtPipes: 5);
            Assert.Equal("", hollowCsv);
            Assert.False(hollowLink);
            var (zeroCsv, zeroLink) = MillCsvBatchModeResolver.ResolveNonFillCsvBatch(entry, isHollowFg: false, ndtPipes: 0);
            Assert.Equal("10001", zeroCsv);
            Assert.False(zeroLink);
        }

        var constant = MillCsvBatchModeResolver.Resolve(opt, 2);
        var (constCsv, constLink) = MillCsvBatchModeResolver.ResolveNonFillCsvBatch(constant, isHollowFg: false, ndtPipes: 10);
        Assert.Equal("10001", constCsv);
        Assert.False(constLink);
    }

    [Fact]
    public void RequireCleanFillCutover_defaults_false_cutover_guard_retired()
    {
        Assert.False(new NdtBundleOptions().RequireCleanFillCutover);
    }

    [Fact]
    public void BackfillReconciliationEnabled_defaults_false()
    {
        Assert.False(new NdtBundleOptions().BackfillReconciliationEnabled);
    }
}
