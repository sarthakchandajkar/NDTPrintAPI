namespace NdtBundleService.Services.PoLifecycle;

/// <inheritdoc cref="IWipConfirmedRunningPoNotifier" />
public sealed class WipConfirmedRunningPoNotifier
    : IWipConfirmedRunningPoNotifier, IWipConfirmedRunningPoNotifierRegistration
{
    private readonly object _lock = new();
    private readonly List<Action<int, string>> _handlers = new();

    public void SetHandler(Action<int, string> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        lock (_lock)
        {
            _handlers.Clear();
            _handlers.Add(handler);
        }
    }

    public void AddHandler(Action<int, string> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        lock (_lock)
            _handlers.Add(handler);
    }

    public void NotifyWipConfirmed(int millNo, string normalizedPo)
    {
        Action<int, string>[] snapshot;
        lock (_lock)
            snapshot = _handlers.ToArray();

        foreach (var handler in snapshot)
            handler(millNo, normalizedPo);
    }
}
