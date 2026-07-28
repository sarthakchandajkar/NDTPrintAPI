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
        Assert.Equal(60, ReconcileFormingBundleHelper.ResolveDisplayTotal(bundle, 60, isForming: true));
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
    public void Placeholder_parent_row_without_print_evidence_is_forming()
    {
        // EnsureBundleParentRowAsync placeholders: Total 0, no Print_Status, no PrintedAt.
        var bundle = new NdtBundleRecord
        {
            BundleNo = "1226100013",
            PrintStatus = "",
            TotalNdtPcs = 0,
            PrintedAt = null
        };

        Assert.True(ReconcileFormingBundleHelper.IsForming(bundle, slitSum: 55, slitOnlyInDatabase: false));
        Assert.Equal(55, ReconcileFormingBundleHelper.ResolveDisplayTotal(bundle, 55, isForming: true));
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
    public void Printed_with_late_slit_attaches_above_tag_total_is_not_forming()
    {
        // 2026-07-28 incident: late Closed-PO traceability attaches pushed the slit sum above the
        // printed tag total and Full/Partial bundles stayed "Forming" forever. Formed = printed.
        var bundle = new NdtBundleRecord
        {
            BundleNo = "1226100012",
            PrintStatus = BundlePrintStatus.Printed,
            TotalNdtPcs = 1,
            PrintedAt = new DateTime(2026, 7, 28, 9, 46, 36, DateTimeKind.Utc)
        };

        Assert.False(ReconcileFormingBundleHelper.IsForming(bundle, slitSum: 14, slitOnlyInDatabase: false));
        Assert.Equal(1, ReconcileFormingBundleHelper.ResolveDisplayTotal(bundle, 14, isForming: false));
    }

    [Fact]
    public void Print_failed_bundle_is_formed_not_forming()
    {
        var bundle = new NdtBundleRecord
        {
            BundleNo = "1226100005",
            PrintStatus = BundlePrintStatus.PrintFailed,
            TotalNdtPcs = 7,
            PrintedAt = new DateTime(2026, 7, 27, 23, 8, 43, DateTimeKind.Utc)
        };

        Assert.False(ReconcileFormingBundleHelper.IsForming(bundle, slitSum: 9, slitOnlyInDatabase: false));
    }
}
