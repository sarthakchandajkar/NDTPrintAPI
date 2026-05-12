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
        var processFolder = (_options.NdtProcessOutputFolder ?? string.Empty).Trim();
        var uploadFolder = (_options.UploadNdtBundleFilesFolder ?? string.Empty).Trim();
        _logger.LogInformation(
            "Upload NDT bundle scheduler started every {Hours} hour(s). Output: {Upload}; reads NDT process CSVs from: {Process}",
            hours,
            string.IsNullOrEmpty(uploadFolder) ? "(NdtBundle:UploadNdtBundleFilesFolder not set)" : uploadFolder,
            string.IsNullOrEmpty(processFolder) ? "(NdtBundle:NdtProcessOutputFolder not set)" : processFolder);
        if (string.IsNullOrEmpty(uploadFolder))
            _logger.LogWarning("UploadNdtBundleFilesFolder is empty; scheduled Upload NDT Bundle CSV generation will fail until set (e.g. Z:\\To SAP\\TM\\NDT\\MES PAS NDT\\Bundle).");
        if (string.IsNullOrEmpty(processFolder) || !Directory.Exists(processFolder))
            _logger.LogWarning(
                "NdtProcessOutputFolder is missing or not reachable ({Process}). Upload bundle generation requires this folder to exist (e.g. Z:\\To SAP\\TM\\NDT\\NDT Final Output\\Bundle).",
                processFolder.Length == 0 ? "(empty)" : processFolder);

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

