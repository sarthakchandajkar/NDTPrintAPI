using NdtBundleService.Configuration;
using NdtBundleService.Services.PlcHandshake;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// Pins F-1 invariant: shared S7 provider must not change handshake observable timing/ordering.
/// </summary>
public sealed class PlcHandshakeBehaviorPinTests
{
    [Fact]
    public void Poll_interval_defaults_to_500ms()
    {
        var mill = new MillConfig { Name = "Mill-1", MillNo = 1, PollIntervalMs = 0 };
        var options = new PlcHandshakeOptions { PollIntervalMs = 500 };

        Assert.Equal(500, PlcHandshakePollTiming.ResolvePollIntervalMs(mill, options));
    }

    [Fact]
    public void Poll_interval_per_mill_override_wins()
    {
        var mill = new MillConfig { Name = "Mill-1", MillNo = 1, PollIntervalMs = 250 };
        var options = new PlcHandshakeOptions { PollIntervalMs = 500 };

        Assert.Equal(250, PlcHandshakePollTiming.ResolvePollIntervalMs(mill, options));
    }

    [Fact]
    public void Rising_edge_requires_rearm_false_polls_then_false_to_true()
    {
        var tracker = new PlcHandshakeEdgeTracker(new PlcHandshakeOptions
        {
            MinimumTriggerFalsePollsBeforeRearm = 2
        });
        tracker.ArmAfterTriggerClear(isStartup: false);

        Assert.False(tracker.TryDetectPoChangeRisingEdge(trigger: false));
        tracker.UpdateTriggerEdgeTracking(false);
        tracker.SetPrevTriggerActive(false);

        // Still false — not a rising edge
        Assert.False(tracker.TryDetectPoChangeRisingEdge(trigger: false));
        tracker.UpdateTriggerEdgeTracking(false);
        tracker.SetPrevTriggerActive(false);

        // Rising edge
        Assert.True(tracker.TryDetectPoChangeRisingEdge(trigger: true));
        tracker.MarkPulseHandled();
        tracker.UpdateTriggerEdgeTracking(true);
        tracker.SetPrevTriggerActive(true);

        // Held TRUE must not fire again
        Assert.False(tracker.TryDetectPoChangeRisingEdge(trigger: true));
    }

    [Fact]
    public void Rising_edge_ignored_until_minimum_false_polls_after_pulse()
    {
        var tracker = new PlcHandshakeEdgeTracker(new PlcHandshakeOptions
        {
            MinimumTriggerFalsePollsBeforeRearm = 2
        });
        tracker.ArmAfterTriggerClear(isStartup: false);
        Assert.True(tracker.TryDetectPoChangeRisingEdge(true));
        tracker.MarkPulseHandled();
        tracker.UpdateTriggerEdgeTracking(true);
        tracker.SetPrevTriggerActive(true);

        // Clear once — not yet rearmed
        tracker.UpdateTriggerEdgeTracking(false);
        tracker.SetPrevTriggerActive(false);
        Assert.False(tracker.IsRearmedForNextPoChange());
        Assert.False(tracker.TryDetectPoChangeRisingEdge(true));

        // Second false poll rearms
        tracker.UpdateTriggerEdgeTracking(false);
        tracker.SetPrevTriggerActive(false);
        Assert.True(tracker.IsRearmedForNextPoChange());
        Assert.True(tracker.TryDetectPoChangeRisingEdge(true));
    }

    [Fact]
    public void Hooter_resolve_deferred_during_ack_clear_or_handshake()
    {
        Assert.True(PlcHandshakeHooterDeferral.ShouldDeferHooterResolve(
            ackAwaitingTriggerClear: true,
            handshakeInProgress: false));
        Assert.True(PlcHandshakeHooterDeferral.ShouldDeferHooterResolve(
            ackAwaitingTriggerClear: false,
            handshakeInProgress: true));
        Assert.False(PlcHandshakeHooterDeferral.ShouldDeferHooterResolve(
            ackAwaitingTriggerClear: false,
            handshakeInProgress: false));
    }
}
