namespace NdtBundleService.Services.PoLifecycle;

/// <summary>
/// Lightweight outbox for WIP-file PO confirmations. Producers (WIP provider) notify;
/// reopen wiring is registered after the full DI graph is built (see <see cref="PoReopenWipConfirmationBridge"/>).
/// </summary>
public interface IWipConfirmedRunningPoNotifier
{
    void NotifyWipConfirmed(int millNo, string normalizedPo);
}

/// <summary>Allows post-graph registration of the WIP-confirmed reopen handler.</summary>
public interface IWipConfirmedRunningPoNotifierRegistration
{
    void SetHandler(Action<int, string> handler);
}
