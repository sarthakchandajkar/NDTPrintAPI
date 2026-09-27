using NdtBundleService.Configuration;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class MillCsvBatchModeResolverTests
{
    [Fact]
    public void Reconcile_list_includes_FillToTarget_mills_only()
    {
        var opt = new NdtBundleOptions
        {
            MillCsvBatchMode = new Dictionary<string, MillCsvBatchModeEntry>
            {
                ["1"] = new() { Mode = "FillToTarget" },
                ["2"] = new() { Mode = "Constant", Value = "10001" },
                ["3"] = new() { Mode = "Constant", Value = "10001" },
                ["4"] = new() { Mode = "Constant", Value = "10001" },
            }
        };

        Assert.True(MillCsvBatchModeResolver.IsIncludedInReconcileBundleList(opt, 1));
        Assert.False(MillCsvBatchModeResolver.IsIncludedInReconcileBundleList(opt, 2));
        Assert.False(MillCsvBatchModeResolver.IsIncludedInReconcileBundleList(opt, 3));
        Assert.False(MillCsvBatchModeResolver.IsIncludedInReconcileBundleList(opt, 4));
    }

    [Fact]
    public void Rolling_a_mill_to_FillToTarget_includes_it_in_reconcile()
    {
        var opt = new NdtBundleOptions
        {
            MillCsvBatchMode = new Dictionary<string, MillCsvBatchModeEntry>
            {
                ["1"] = new() { Mode = "FillToTarget" },
                ["2"] = new() { Mode = "FillToTarget" },
                ["3"] = new() { Mode = "Constant", Value = "10001" },
                ["4"] = new() { Mode = "Constant", Value = "10001" },
            }
        };

        Assert.True(MillCsvBatchModeResolver.IsIncludedInReconcileBundleList(opt, 2));
        Assert.False(MillCsvBatchModeResolver.IsIncludedInReconcileBundleList(opt, 3));
    }
}
