using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class ReconcileBundleListFilterTests
{
    [Fact]
    public void Manual_recon_below_threshold_is_not_excluded()
    {
        Assert.False(ReconcileBundleListFilter.ShouldExcludeFromList(
            isLatest: true,
            totalNdtPcs: 46,
            threshold: 56,
            manualRecon: true,
            printStatus: BundlePrintStatus.Printed));
    }

    [Fact]
    public void Printed_below_threshold_is_not_excluded()
    {
        Assert.False(ReconcileBundleListFilter.ShouldExcludeFromList(
            isLatest: true,
            totalNdtPcs: 46,
            threshold: 56,
            manualRecon: false,
            printStatus: BundlePrintStatus.Printed));
    }

    [Fact]
    public void Pending_open_partial_latest_is_excluded()
    {
        Assert.True(ReconcileBundleListFilter.ShouldExcludeFromList(
            isLatest: true,
            totalNdtPcs: 39,
            threshold: 56,
            manualRecon: false,
            printStatus: BundlePrintStatus.Pending));
    }

    [Fact]
    public void Non_latest_below_threshold_is_not_excluded()
    {
        Assert.False(ReconcileBundleListFilter.ShouldExcludeFromList(
            isLatest: false,
            totalNdtPcs: 46,
            threshold: 56,
            manualRecon: false,
            printStatus: BundlePrintStatus.Printed));
    }

    [Fact]
    public void Incident_replay_1226100001_visible_1226100002_pending_still_hidden()
    {
        const int threshold = 56;

        Assert.False(ReconcileBundleListFilter.ShouldExcludeFromList(
            isLatest: true,
            totalNdtPcs: 46,
            threshold,
            manualRecon: true,
            printStatus: BundlePrintStatus.Printed));

        Assert.True(ReconcileBundleListFilter.ShouldExcludeFromList(
            isLatest: true,
            totalNdtPcs: 39,
            threshold,
            manualRecon: false,
            printStatus: BundlePrintStatus.Pending));
    }
}
