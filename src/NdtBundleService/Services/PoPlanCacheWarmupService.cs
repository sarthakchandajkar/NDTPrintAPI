using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace NdtBundleService.Services;

/// <summary>Preloads PO plan caches on startup so dashboard APIs respond quickly after service restart.</summary>
public sealed class PoPlanCacheWarmupService : IHostedService
{
    private readonly IPipeSizeProvider _pipeSizeProvider;
    private readonly IPoPlanWipEnrichmentProvider _wipEnrichmentProvider;
    private readonly ILogger<PoPlanCacheWarmupService> _logger;

    public PoPlanCacheWarmupService(
        IPipeSizeProvider pipeSizeProvider,
        IPoPlanWipEnrichmentProvider wipEnrichmentProvider,
        ILogger<PoPlanCacheWarmupService> logger)
    {
        _pipeSizeProvider = pipeSizeProvider;
        _wipEnrichmentProvider = wipEnrichmentProvider;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _ = Task.Run(WarmCachesAsync, CancellationToken.None);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    private async Task WarmCachesAsync()
    {
        try
        {
            await _pipeSizeProvider.GetPipeSizeByPoAsync(CancellationToken.None).ConfigureAwait(false);
            await _wipEnrichmentProvider.GetEnrichmentAsync(CancellationToken.None).ConfigureAwait(false);
            _logger.LogInformation("PO plan caches warmed on startup.");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "PO plan cache warmup failed; dashboard will populate caches on first successful request.");
        }
    }
}
