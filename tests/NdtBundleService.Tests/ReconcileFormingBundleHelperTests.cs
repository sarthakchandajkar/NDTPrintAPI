using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class ReconcileFormingBundleHelperTests
{
    [Fact]
    public void Pending_open_partial_is_forming()
    {
        var bundle = new NdtBundleRecord
        {
            BundleNo = "1226100002",
            PrintStatus = BundlePrintStatus.Pending,
            TotalNdtPcs = 17
        };

        Assert.True(ReconcileFormingBundleHelper.IsForming(bundle, slitSum: 60, slitOnlyInDatabase: false));
        Assert.Equal(60, ReconcileFormingBundleHelper.ResolveDisplayTotal(bundle, 60, includeForming: true));
    }

    [Fact]
    public void Slit_only_batch_is_forming()
    {
        var bundle = new NdtBundleRecord
        {
            BundleNo = "1226100003",
            PrintStatus = BundlePrintStatus.Pending,
            TotalNdtPcs = 12
        };

        Assert.True(ReconcileFormingBundleHelper.IsForming(bundle, slitSum: 12, slitOnlyInDatabase: true));
    }

    [Fact]
    public void Printed_with_matching_slit_sum_is_not_forming()
    {
        var bundle = new NdtBundleRecord
        {
            BundleNo = "1226100001",
            PrintStatus = BundlePrintStatus.Printed,
            TotalNdtPcs = 46
        };

        Assert.False(ReconcileFormingBundleHelper.IsForming(bundle, slitSum: 46, slitOnlyInDatabase: false));
    }

    [Fact]
    public void Printed_but_slits_still_accumulating_is_forming()
    {
        var bundle = new NdtBundleRecord
        {
            BundleNo = "1226100002",
            PrintStatus = BundlePrintStatus.Printed,
            TotalNdtPcs = 17
        };

        Assert.True(ReconcileFormingBundleHelper.IsForming(bundle, slitSum: 60, slitOnlyInDatabase: false));
        Assert.Equal(60, ReconcileFormingBundleHelper.ResolveDisplayTotal(bundle, 60, includeForming: true));
    }
}
