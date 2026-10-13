using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace NdtBundleService.Services.PoLifecycle;

/// <summary>
/// On WIP-confirmed running PO, open a stamp-only fill parent for that PO (fire-and-forget scoped work).
/// Registered alongside <see cref="PoReopenWipConfirmationBridge"/> via multicast handlers.
/// </summary>
public sealed class PoCsvAdvanceOnWipConfirmationBridge : IHostedService
{
    private readonly IWipConfirmedRunningPoNotifierRegistration _registration;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<PoCsvAdvanceOnWipConfirmationBridge> _logger;

    public PoCsvAdvanceOnWipConfirmationBridge(
        IWipConfirmedRunningPoNotifierRegistration registration,
        IServiceScopeFactory scopeFactory,
        ILogger<PoCsvAdvanceOnWipConfirmationBridge> logger)
    {
        _registration = registration;
        _scopeFactory = scopeFactory;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _registration.AddHandler((millNo, po) =>
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    await using var scope = _scopeFactory.CreateAsyncScope();
                    var svc = scope.ServiceProvider.GetRequiredService<IPoCsvAdvanceOnNewRunningPoService>();
                    await svc.TryOpenStampForRunningPoAsync(millNo, po, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "WIP stamp-open handler failed for Mill {Mill} PO {PO}.",
                        millNo,
                        po);
                }
            });
        });
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
