namespace NdtBundleService.Services.PlcHandshake;

/// <summary>
/// Hooter MW resolve is deferred while ack-clear or handshake is in progress
/// so Mill-1 polls stay timely (ack-first / handshake-before-hooter).
/// </summary>
public static class PlcHandshakeHooterDeferral
{
    public static bool ShouldDeferHooterResolve(bool ackAwaitingTriggerClear, bool handshakeInProgress) =>
        ackAwaitingTriggerClear || handshakeInProgress;
}
