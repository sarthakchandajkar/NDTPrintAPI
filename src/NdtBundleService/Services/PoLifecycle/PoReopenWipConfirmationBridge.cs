using Microsoft.Extensions.Hosting;

namespace NdtBundleService.Services.PoLifecycle;

/// <summary>
/// Wires <see cref="PoReopenService"/> to WIP confirmations after DI construction,
/// so <see cref="WipBundleRunningPoProvider"/> never depends on reopen services.
/// </summary>
public sealed class PoReopenWipConfirmationBridge : IHostedService
{
    private readonly IWipConfirmedRunningPoNotifierRegistration _registration;
    private readonly PoReopenService _reopen;

    public PoReopenWipConfirmationBridge(
        IWipConfirmedRunningPoNotifierRegistration registration,
        PoReopenService reopen)
    {
        _registration = registration;
        _reopen = reopen;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _registration.SetHandler((millNo, po) =>
            _reopen.TryReopenIfClosed(millNo, po, po));
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
