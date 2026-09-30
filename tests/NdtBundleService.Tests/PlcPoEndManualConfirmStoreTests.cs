using NdtBundleService.Services.PlcHandshake;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class PlcPoEndManualConfirmStoreTests
{
    [Fact]
    public void Arm_TryGet_TryTake_round_trip()
    {
        var store = new PlcPoEndManualConfirmStore();
        var pending = new PlcPoEndManualConfirmPending(
            1,
            "Mill-1",
            PoIdAtEdge: 1226100003,
            NdtAtEdge: 4,
            CorrelationId: Guid.NewGuid(),
            DetectedAtUtc: DateTimeOffset.UtcNow,
            RunningPoAtEdge: "1226100003");

        store.Arm(pending);
        Assert.True(store.TryGet(1, out var got));
        Assert.Equal(pending.CorrelationId, got.CorrelationId);
        Assert.Single(store.List());

        Assert.True(store.TryTake(1, out var taken));
        Assert.Equal(pending.CorrelationId, taken.CorrelationId);
        Assert.False(store.TryGet(1, out _));
        Assert.Empty(store.List());
    }

    [Fact]
    public void Arm_replaces_existing_pending_for_same_mill()
    {
        var store = new PlcPoEndManualConfirmStore();
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        store.Arm(new PlcPoEndManualConfirmPending(1, "Mill-1", 1, 2, first, DateTimeOffset.UtcNow, null));
        store.Arm(new PlcPoEndManualConfirmPending(1, "Mill-1", 3, 4, second, DateTimeOffset.UtcNow, "PO"));
        Assert.True(store.TryGet(1, out var got));
        Assert.Equal(second, got.CorrelationId);
        Assert.Equal(4, got.NdtAtEdge);
    }
}
