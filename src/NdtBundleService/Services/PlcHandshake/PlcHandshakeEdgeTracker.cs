using NdtBundleService.Configuration;

namespace NdtBundleService.Services.PlcHandshake;

/// <summary>
/// Rising-edge / re-arm state for PO-change trigger bits (extracted for unit tests;
/// behavior matches the former private fields on <c>PlcHandshakeService</c>).
/// </summary>
public sealed class PlcHandshakeEdgeTracker
{
    private readonly PlcHandshakeOptions _options;

    public PlcHandshakeEdgeTracker(PlcHandshakeOptions options)
    {
        _options = options;
    }

    public bool Primed { get; private set; }
    public bool PrevTriggerActive { get; private set; }
    public bool PoChangePulseHandled { get; private set; }
    public int ConsecutiveTriggerFalsePolls { get; private set; }
    public bool LoggedStartupTriggerWait { get; set; }
    public bool StartupRecoveryAttempted { get; set; }

    public bool TryDetectPoChangeRisingEdge(bool trigger) =>
        trigger &&
        !PrevTriggerActive &&
        !PoChangePulseHandled &&
        IsRearmedForNextPoChange();

    public bool IsRearmedForNextPoChange()
    {
        var required = Math.Max(1, _options.MinimumTriggerFalsePollsBeforeRearm);
        return ConsecutiveTriggerFalsePolls >= required;
    }

    public void UpdateTriggerEdgeTracking(bool trigger)
    {
        if (!trigger)
        {
            PoChangePulseHandled = false;
            ConsecutiveTriggerFalsePolls++;
        }
        else
        {
            ConsecutiveTriggerFalsePolls = 0;
        }
    }

    public void MarkPrimedForTelemetry() => Primed = true;

    public void MarkPulseHandled() => PoChangePulseHandled = true;

    public void SetPrevTriggerActive(bool trigger) => PrevTriggerActive = trigger;

    public void ArmAfterTriggerClear(bool isStartup)
    {
        Primed = true;
        LoggedStartupTriggerWait = false;
        PrevTriggerActive = false;
        PoChangePulseHandled = false;
        ConsecutiveTriggerFalsePolls = isStartup ? 1 : Math.Max(1, _options.MinimumTriggerFalsePollsBeforeRearm);
    }

    public void ResetTriggerEdgeState(bool reprime)
    {
        Primed = !reprime;
        if (reprime)
        {
            LoggedStartupTriggerWait = false;
            StartupRecoveryAttempted = false;
        }

        PrevTriggerActive = false;
        PoChangePulseHandled = false;
        ConsecutiveTriggerFalsePolls = 0;
    }

    public void BeginStartupRecovery()
    {
        StartupRecoveryAttempted = true;
        LoggedStartupTriggerWait = false;
        PrevTriggerActive = true;
        PoChangePulseHandled = true;
    }
}
