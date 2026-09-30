using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Imports Slit Accepted and FG bundle CSVs into SQL on startup and periodically.
/// Registered only on Shared (via <c>InstanceRole:EnablePoPlanWipImport</c>). Mills never run this.
/// Source CSVs are strictly read-only; replication writes SQL only.
/// </summary>
public sealed class UploadLookupCsvImportHostedService : IHostedService
{
    private readonly NdtBundleOptions _options;
    private readonly ISlitAcceptedImporter _slitAcceptedImporter;
    private readonly IFgBundleImporter _fgBundleImporter;
    private readonly ILogger<UploadLookupCsvImportHostedService> _logger;
    private CancellationTokenSource? _stoppingCts;
    private Task? _backgroundTask;

    public UploadLookupCsvImportHostedService(
        IOptions<NdtBundleOptions> options,
        ISlitAcceptedImporter slitAcceptedImporter,
        IFgBundleImporter fgBundleImporter,
        ILogger<UploadLookupCsvImportHostedService> logger)
    {
        _options = options.Value;
        _slitAcceptedImporter = slitAcceptedImporter;
        _fgBundleImporter = fgBundleImporter;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!IsEnabled(_options))
            return;

        _logger.LogInformation(
            "Upload lookup CSV import starting (Slit Accepted={SlitFolder}; FG Bundle={FgFolder}/{FgAccepted}).",
            _options.SlitAcceptedFolder,
            _options.FgBundleFolder,
            _options.FgBundleAcceptedFolder);

        await RunOnceAsync(cancellationToken).ConfigureAwait(false);

        if (_options.ImportUploadLookupCsvPollMinutes <= 0)
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

    internal static bool IsEnabled(NdtBundleOptions options) =>
        SqlTraceabilityConnection.IsSqlEnabled(options)
        && (options.ImportSlitAcceptedFromFolder || options.ImportFgBundleFromFolder);

    private async Task RunOnceAsync(CancellationToken cancellationToken)
    {
        if (_options.ImportSlitAcceptedFromFolder)
            await _slitAcceptedImporter.ImportEligibleFilesAsync(cancellationToken).ConfigureAwait(false);
        if (_options.ImportFgBundleFromFolder)
            await _fgBundleImporter.ImportEligibleFilesAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task RunPeriodicImportAsync(CancellationToken stoppingToken)
    {
        var interval = TimeSpan.FromMinutes(_options.ImportUploadLookupCsvPollMinutes);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, stoppingToken).ConfigureAwait(false);
                await RunOnceAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Periodic upload-lookup CSV import failed; will retry after {Minutes} minute(s).",
                    _options.ImportUploadLookupCsvPollMinutes);
            }
        }
    }
}
