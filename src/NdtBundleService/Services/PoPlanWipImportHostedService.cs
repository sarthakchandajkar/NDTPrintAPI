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

    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (!PoPlanWipImportSettings.IsEnabled(_options))
            return Task.CompletedTask;

        _logger.LogInformation(
            "PO_Plan_WIP folder import scheduled in background (folder {Folder}). API stays available during folder scans.",
            _options.PoPlanFolder);

        // Do not await ImportEligibleFilesAsync here — large PO Accepted folders delay Kestrel bind.
        _stoppingCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _backgroundTask = RunImportLoopAsync(_stoppingCts.Token);
        return Task.CompletedTask;
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

    private async Task RunImportLoopAsync(CancellationToken stoppingToken)
    {
        try
        {
            await _importer.ImportEligibleFilesAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Background PO_Plan_WIP folder import failed; will retry after {Minutes} minute(s).",
                Math.Max(1, _options.ImportPoPlanWipPollMinutes));
        }

        if (_options.ImportPoPlanWipPollMinutes <= 0)
            return;

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
