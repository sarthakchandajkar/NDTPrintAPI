namespace NdtBundleService.Services.PoLifecycle;

/// <inheritdoc cref="IWipConfirmedRunningPoNotifier" />
public sealed class WipConfirmedRunningPoNotifier
    : IWipConfirmedRunningPoNotifier, IWipConfirmedRunningPoNotifierRegistration
{
    private readonly object _lock = new();
    private Action<int, string>? _handler;

    public void SetHandler(Action<int, string> handler)
    {
        lock (_lock)
            _handler = handler;
    }

    public void NotifyWipConfirmed(int millNo, string normalizedPo)
    {
        Action<int, string>? handler;
        lock (_lock)
            handler = _handler;

        handler?.Invoke(millNo, normalizedPo);
    }
}
