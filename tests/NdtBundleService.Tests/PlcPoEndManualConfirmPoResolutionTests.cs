using NdtBundleService.Configuration;
using NdtBundleService.Services.PlcHandshake;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class PlcPoEndManualConfirmPoResolutionTests
{
    private static readonly PlcPoEndOptions StrictSap = new()
    {
        MinValidPoId = 1_000_000_000,
        MaxValidPoId = 2_000_000_000,
        MinSapPoNumberDigits = 10
    };

    [Fact]
    public void Prefer_plausible_plc_po_id()
    {
        var po = PlcPoEndManualConfirmPoResolution.TryResolve(
            poIdAtEdge: 1000062596,
            runningPoAtEdge: "1000062550",
            liveRunningPo: null,
            waitingForNewWip: true,
            endedPoAtEdge: "1000062596",
            liveEndedPo: null,
            activeOrSlitPo: "1000062550",
            StrictSap,
            out var source);

        Assert.Equal("1000062596", po);
        Assert.Equal(PlcPoEndManualConfirmPoResolution.SourcePlcPoId, source);
    }

    [Fact]
    public void Ladder_po_id_ignored_uses_running_at_edge()
    {
        var po = PlcPoEndManualConfirmPoResolution.TryResolve(
            poIdAtEdge: 19,
            runningPoAtEdge: "1000062596",
            liveRunningPo: null,
            waitingForNewWip: false,
            endedPoAtEdge: null,
            liveEndedPo: null,
            activeOrSlitPo: "1000062550",
            StrictSap,
            out var source);

        Assert.Equal("1000062596", po);
        Assert.Equal(PlcPoEndManualConfirmPoResolution.SourceRunningPoAtEdge, source);
    }

    [Fact]
    public void Waiting_for_wip_with_unknown_running_uses_ended_po_at_edge()
    {
        // Today's incident: PO_Id 19, RunningPo unknown, still WaitingForNewWip after 2596.
        var po = PlcPoEndManualConfirmPoResolution.TryResolve(
            poIdAtEdge: 19,
            runningPoAtEdge: null,
            liveRunningPo: null,
            waitingForNewWip: true,
            endedPoAtEdge: "1000062596",
            liveEndedPo: "1000062596",
            activeOrSlitPo: "1000062550",
            StrictSap,
            out var source);

        Assert.Equal("1000062596", po);
        Assert.Equal(PlcPoEndManualConfirmPoResolution.SourceEndedPoAtEdge, source);
    }

    [Fact]
    public void Waiting_for_wip_uses_live_ended_when_snapshot_missing()
    {
        var po = PlcPoEndManualConfirmPoResolution.TryResolve(
            poIdAtEdge: 19,
            runningPoAtEdge: null,
            liveRunningPo: null,
            waitingForNewWip: true,
            endedPoAtEdge: null,
            liveEndedPo: "1000062596",
            activeOrSlitPo: "1000062550",
            StrictSap,
            out var source);

        Assert.Equal("1000062596", po);
        Assert.Equal(PlcPoEndManualConfirmPoResolution.SourceLiveEndedPo, source);
    }

    [Fact]
    public void Waiting_for_wip_does_not_fall_through_to_active_slit_po()
    {
        var po = PlcPoEndManualConfirmPoResolution.TryResolve(
            poIdAtEdge: 19,
            runningPoAtEdge: null,
            liveRunningPo: null,
            waitingForNewWip: true,
            endedPoAtEdge: null,
            liveEndedPo: null,
            activeOrSlitPo: "1000062550",
            StrictSap,
            out var source);

        Assert.Null(po);
        Assert.Null(source);
    }

    [Fact]
    public void Not_waiting_uses_active_or_slit_po()
    {
        var po = PlcPoEndManualConfirmPoResolution.TryResolve(
            poIdAtEdge: 19,
            runningPoAtEdge: null,
            liveRunningPo: null,
            waitingForNewWip: false,
            endedPoAtEdge: "1000062596",
            liveEndedPo: "1000062596",
            activeOrSlitPo: "1000062550",
            StrictSap,
            out var source);

        Assert.Equal("1000062550", po);
        Assert.Equal(PlcPoEndManualConfirmPoResolution.SourceActiveOrSlitPo, source);
    }

    [Fact]
    public void Pending_record_snapshots_ended_po_while_waiting()
    {
        var pending = new PlcPoEndManualConfirmPending(
            1,
            "Mill-1",
            PoIdAtEdge: 19,
            NdtAtEdge: 5,
            CorrelationId: Guid.NewGuid(),
            DetectedAtUtc: DateTimeOffset.UtcNow,
            RunningPoAtEdge: null,
            EndedPoAtEdge: "1000062596",
            WaitingForNewWipAtEdge: true);

        Assert.True(pending.WaitingForNewWipAtEdge);
        Assert.Equal("1000062596", pending.EndedPoAtEdge);
        Assert.Null(pending.RunningPoAtEdge);
    }
}
