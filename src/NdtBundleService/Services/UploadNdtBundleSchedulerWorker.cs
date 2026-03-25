using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

public sealed class UploadNdtBundleSchedulerWorker : BackgroundService
{
    private readonly NdtBundleOptions _options;
    private readonly IUploadNdtBundleFileService _generator;
    private readonly ILogger<UploadNdtBundleSchedulerWorker> _logger;

    public UploadNdtBundleSchedulerWorker(
        IOptions<NdtBundleOptions> options,
        IUploadNdtBundleFileService generator,
        ILogger<UploadNdtBundleSchedulerWorker> logger)
    {
        _options = options.Value;
        _generator = generator;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.EnableUploadNdtBundleScheduler)
        {
            _logger.LogInformation("Upload NDT bundle scheduler disabled.");
            return;
        }

        var hours = _options.UploadNdtBundleIntervalHours <= 0 ? 12 : _options.UploadNdtBundleIntervalHours;
        var interval = TimeSpan.FromHours(hours);
        _logger.LogInformation("Upload NDT bundle scheduler started with interval {Hours} hour(s).", hours);

        using var timer = new PeriodicTimer(interval);
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var hasTicked = await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false);
                if (!hasTicked)
                    break;
                var result = await _generator.GenerateAsync(stoppingToken).ConfigureAwait(false);
                _logger.LogInformation("Scheduled upload bundle CSV generated at {Path} with {Rows} row(s).", result.FilePath, result.RowCount);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Scheduled upload bundle CSV generation failed.");
            }
        }
    }
}

