using NdtBundleService.Configuration;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class NdtBundleRuntimeStateLogicTests
{
    private static readonly RuntimeStatePruningOptions DefaultPruning = new()
    {
        Enabled = true,
        RunOnStartup = true,
        GracePeriodDays = 14
    };

    [Theory]
    [InlineData(1, null, true)]
    [InlineData(0, "Default:5", true)]
    [InlineData(0, "Default:0", false)]
    [InlineData(0, null, false)]
    public void HasOpenPartialBundle_DetectsRunningTotalAndSizeCounts(int runningTotal, string? sizeCountsSpec, bool expected)
    {
        IReadOnlyDictionary<string, int>? sizeCounts = null;
        if (sizeCountsSpec is not null)
        {
            var parts = sizeCountsSpec.Split(':');
            sizeCounts = new Dictionary<string, int> { [parts[0]] = int.Parse(parts[1]) };
        }

        Assert.Equal(expected, NdtBundleRuntimeStateLogic.HasOpenPartialBundle(runningTotal, sizeCounts));
    }

    [Fact]
    public void ApplyMillFloorIfAllowed_PartialBundle_DoesNotRaiseBatchOffsetWhenMillFloorIsHigher()
    {
        var batchOffset = 34;
        var engineBatchNo = 34;
        var sizeCounts = new Dictionary<string, int> { ["Default"] = 8 };

        NdtBundleRuntimeStateLogic.ApplyMillFloorIfAllowed(
            ref batchOffset,
            ref engineBatchNo,
            runningTotal: 0,
            sizeCounts,
            millFloor: 36);

        Assert.Equal(34, batchOffset);
        Assert.Equal(34, engineBatchNo);
        Assert.Equal(35, NdtBundleRuntimeStateLogic.ResolveOpenBatchNumber(batchOffset));
    }

    [Fact]
    public void ApplyMillFloorIfAllowed_RunningTotalPartial_DoesNotRaiseBatchOffsetWhenMillFloorIsHigher()
    {
        var batchOffset = 34;
        var engineBatchNo = 34;

        NdtBundleRuntimeStateLogic.ApplyMillFloorIfAllowed(
            ref batchOffset,
            ref engineBatchNo,
            runningTotal: 8,
            sizeCounts: null,
            millFloor: 36);

        Assert.Equal(34, batchOffset);
        Assert.Equal(35, NdtBundleRuntimeStateLogic.ResolveOpenBatchNumber(batchOffset));
    }

    [Fact]
    public void ApplyMillFloorIfAllowed_IdleSlotBelowFloor_IsRaisedToMillFloor()
    {
        var batchOffset = 30;
        var engineBatchNo = 30;

        NdtBundleRuntimeStateLogic.ApplyMillFloorIfAllowed(
            ref batchOffset,
            ref engineBatchNo,
            runningTotal: 0,
            sizeCounts: null,
            millFloor: 36);

        Assert.Equal(36, batchOffset);
        Assert.Equal(36, engineBatchNo);
        Assert.Equal(37, NdtBundleRuntimeStateLogic.ResolveOpenBatchNumber(batchOffset));
    }

    [Fact]
    public void CanPruneSlot_ActivePo_IsNotPruned()
    {
        var utcNow = new DateTime(2026, 6, 14, 12, 0, 0, DateTimeKind.Utc);
        var active = new Dictionary<int, string> { [1] = "PO-100" };

        var canPrune = NdtBundleRuntimeStateLogic.CanPruneSlot(
            "PO-100",
            1,
            runningTotal: 0,
            sizeCounts: null,
            lastActivityUtc: utcNow.AddDays(-30),
            rootUpdatedUtc: utcNow.AddDays(-30),
            active,
            utcNow,
            DefaultPruning);

        Assert.False(canPrune);
    }

    [Fact]
    public void CanPruneSlot_PartialBundle_IsNotPruned()
    {
        var utcNow = new DateTime(2026, 6, 14, 12, 0, 0, DateTimeKind.Utc);

        var canPrune = NdtBundleRuntimeStateLogic.CanPruneSlot(
            "PO-OLD",
            1,
            runningTotal: 5,
            sizeCounts: null,
            lastActivityUtc: utcNow.AddDays(-30),
            rootUpdatedUtc: utcNow.AddDays(-30),
            activePoByMill: new Dictionary<int, string>(),
            utcNow,
            DefaultPruning);

        Assert.False(canPrune);
    }

    [Fact]
    public void CanPruneSlot_WithinGracePeriod_IsNotPruned()
    {
        var utcNow = new DateTime(2026, 6, 14, 12, 0, 0, DateTimeKind.Utc);

        var canPrune = NdtBundleRuntimeStateLogic.CanPruneSlot(
            "PO-OLD",
            1,
            runningTotal: 0,
            sizeCounts: null,
            lastActivityUtc: utcNow.AddDays(-3),
            rootUpdatedUtc: utcNow.AddDays(-3),
            activePoByMill: new Dictionary<int, string>(),
            utcNow,
            DefaultPruning);

        Assert.False(canPrune);
    }

    [Fact]
    public void CanPruneSlot_IdleOldCompletedSlot_CanBePruned()
    {
        var utcNow = new DateTime(2026, 6, 14, 12, 0, 0, DateTimeKind.Utc);

        var canPrune = NdtBundleRuntimeStateLogic.CanPruneSlot(
            "PO-OLD",
            1,
            runningTotal: 0,
            sizeCounts: null,
            lastActivityUtc: utcNow.AddDays(-30),
            rootUpdatedUtc: utcNow.AddDays(-30),
            activePoByMill: new Dictionary<int, string> { [1] = "PO-NEW" },
            utcNow,
            DefaultPruning);

        Assert.True(canPrune);
    }

    [Fact]
    public void CanPruneSlot_MissingLastActivity_DoesNotPrune()
    {
        var utcNow = new DateTime(2026, 6, 14, 12, 0, 0, DateTimeKind.Utc);

        var canPrune = NdtBundleRuntimeStateLogic.CanPruneSlot(
            "PO-OLD",
            1,
            runningTotal: 0,
            sizeCounts: null,
            lastActivityUtc: default,
            rootUpdatedUtc: default,
            activePoByMill: new Dictionary<int, string>(),
            utcNow,
            DefaultPruning);

        Assert.False(canPrune);
    }

    [Fact]
    public void SelectSlotsToPrune_PromotesMillMaxBeforeRemoval()
    {
        var utcNow = new DateTime(2026, 6, 14, 12, 0, 0, DateTimeKind.Utc);
        var slots = new Dictionary<string, RuntimeStateSlotSnapshot>(StringComparer.OrdinalIgnoreCase)
        {
            ["PO-OLD|1"] = new("PO-OLD", 1, 40, 0, 40, new Dictionary<string, int>(), utcNow.AddDays(-30))
        };
        var millMax = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase) { ["1"] = 35 };

        var removed = NdtBundleRuntimeStateLogic.SelectSlotsToPrune(
            slots,
            millMax,
            utcNow.AddDays(-30),
            new Dictionary<int, string> { [1] = "PO-NEW" },
            utcNow,
            DefaultPruning);

        Assert.Equal(["PO-OLD|1"], removed);
        Assert.Equal(40, millMax["1"]);
    }

    [Fact]
    public void SelectSlotsToPrune_KeepsActiveAndPartialSlots()
    {
        var utcNow = new DateTime(2026, 6, 14, 12, 0, 0, DateTimeKind.Utc);
        var slots = new Dictionary<string, RuntimeStateSlotSnapshot>(StringComparer.OrdinalIgnoreCase)
        {
            ["PO-ACTIVE|1"] = new("PO-ACTIVE", 1, 34, 8, 34, new Dictionary<string, int>(), utcNow.AddDays(-1)),
            ["PO-OLD|1"] = new("PO-OLD", 1, 20, 0, 20, new Dictionary<string, int>(), utcNow.AddDays(-30))
        };
        var millMax = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase) { ["1"] = 36 };

        var removed = NdtBundleRuntimeStateLogic.SelectSlotsToPrune(
            slots,
            millMax,
            utcNow.AddDays(-30),
            new Dictionary<int, string> { [1] = "PO-ACTIVE" },
            utcNow,
            DefaultPruning);

        Assert.Equal(["PO-OLD|1"], removed);
        Assert.Equal(36, millMax["1"]);
    }
}
