using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Imports eligible PO Accepted CSVs into <c>dbo.PO_Plan_WIP</c> on startup (before cache warmup)
/// and periodically for new/changed files.
/// </summary>
public sealed class PoPlanWipImportHostedService : IHostedService
{
    private readonly NdtBundleOptions _options;
    private readonly IPoPlanWipImporter _importer;
    private readonly ILogger<PoPlanWipImportHostedService> _logger;
    private CancellationTokenSource? _stoppingCts;
    private Task? _backgroundTask;

    public PoPlanWipImportHostedService(
        IOptions<NdtBundleOptions> options,
        IPoPlanWipImporter importer,
        ILogger<PoPlanWipImportHostedService> logger)
    {
        _options = options.Value;
        _importer = importer;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!PoPlanWipImportSettings.IsEnabled(_options))
            return;

        _logger.LogInformation(
            "PO_Plan_WIP folder import starting (folder {Folder}; eligible files use PO plan date filter).",
            _options.PoPlanFolder);

        await _importer.ImportEligibleFilesAsync(cancellationToken).ConfigureAwait(false);

        if (_options.ImportPoPlanWipPollMinutes <= 0)
            return;

        _stoppingCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _backgroundTask = RunPeriodicImportAsync(_stoppingCts.Token);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_stoppingCts is null)
            return;

        await _stoppingCts.CancelAsync().ConfigureAwait(false);
        if (_backgroundTask is not null)
        {
            try
            {
                await _backgroundTask.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown.
            }
        }

        _stoppingCts.Dispose();
    }

    private async Task RunPeriodicImportAsync(CancellationToken stoppingToken)
    {
        var interval = TimeSpan.FromMinutes(_options.ImportPoPlanWipPollMinutes);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, stoppingToken).ConfigureAwait(false);
                await _importer.ImportEligibleFilesAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Periodic PO_Plan_WIP folder import failed; will retry after {Minutes} minute(s).",
                    _options.ImportPoPlanWipPollMinutes);
            }
        }
    }
}
