using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services.FileBasedPoChange;

/// <summary>
/// Periodically scans WIP bundle folders for mills with <c>PoEndSource=File</c> and enqueues missed PO-end events.
/// </summary>
public sealed class WipBundleFileReconciliationWorker : BackgroundService
{
    private readonly IWipBundleReconciliationService _reconciliation;
    private readonly IOptions<NdtBundleOptions> _options;
    private readonly ILogger<WipBundleFileReconciliationWorker> _logger;

    public WipBundleFileReconciliationWorker(
        IWipBundleReconciliationService reconciliation,
        IOptions<NdtBundleOptions> options,
        ILogger<WipBundleFileReconciliationWorker> logger)
    {
        _reconciliation = reconciliation;
        _options = options;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var bundle = _options.Value;
        var fileBased = bundle.FileBasedPoEnd ?? new FileBasedPoEndOptions();

        if (!fileBased.ReconciliationEnabled)
        {
            _logger.LogInformation("WipBundleFileReconciliationWorker disabled (FileBasedPoEnd:ReconciliationEnabled=false).");
            return;
        }

        if (!MillPoEndSourceResolver.AnyMillUsesFilePoEnd(bundle))
        {
            _logger.LogInformation(
                "WipBundleFileReconciliationWorker idle — no mill uses PoEndSource=File.");
            return;
        }

        var fileMills = (bundle.PlcHandshake ?? new PlcHandshakeOptions()).Mills
            .Where(m => m.ResolvePoEndSource(bundle) == MillPoEndSource.File)
            .Select(m => string.IsNullOrWhiteSpace(m.Name) ? $"Mill-{m.ResolveMillNo()}" : m.Name)
            .ToList();

        var intervalMinutes = fileBased.ReconciliationIntervalMinutes <= 0
            ? 5
            : fileBased.ReconciliationIntervalMinutes;
        var interval = TimeSpan.FromMinutes(intervalMinutes);

        _logger.LogInformation(
            "WipBundleFileReconciliationWorker started — scanning every {Minutes} min for mills: {Mills}.",
            intervalMinutes,
            fileMills.Count > 0 ? string.Join(", ", fileMills) : "(none configured)");

        using var timer = new PeriodicTimer(interval);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _reconciliation.ReconcileAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "WIP bundle reconciliation pass failed.");
            }

            try
            {
                if (!await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
                    break;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }
}
