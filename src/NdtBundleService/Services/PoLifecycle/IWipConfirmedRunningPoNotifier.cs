namespace NdtBundleService.Services.PoLifecycle;

/// <summary>
/// Lightweight outbox for WIP-file PO confirmations. Producers (WIP provider) notify;
/// reopen / CSV-advance wiring is registered after the full DI graph is built.
/// </summary>
public interface IWipConfirmedRunningPoNotifier
{
    void NotifyWipConfirmed(int millNo, string normalizedPo);
}

/// <summary>Allows post-graph registration of WIP-confirmed handlers.</summary>
public interface IWipConfirmedRunningPoNotifierRegistration
{
    /// <summary>Replace all handlers with a single handler (legacy).</summary>
    void SetHandler(Action<int, string> handler);

    /// <summary>Append a handler (preferred; supports reopen + CSV-advance bridges).</summary>
    void AddHandler(Action<int, string> handler);
}
