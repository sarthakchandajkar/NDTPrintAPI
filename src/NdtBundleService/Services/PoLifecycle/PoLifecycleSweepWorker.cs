using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.PoLifecycle;
using Serilog.Context;

namespace NdtBundleService.Services.PoLifecycle;

/// <summary>
/// For <c>PoEndSource=Plc</c> mills: completes AfterDrain / Immediate drain-window sweeps and optionally auto-closes orphans.
/// File mills are never touched.
/// </summary>
public sealed class PoLifecycleSweepWorker : BackgroundService
{
    private readonly IPoLifecycleService _lifecycle;
    private readonly PoEndWorkflowService _poEndWorkflow;
    private readonly INdtBatchStateService _batchState;
    private readonly INdtBundleRuntimeStateStore _runtimeState;
    private readonly IBundleEngine _bundleEngine;
    private readonly IBundleOutputWriter _outputWriter;
    private readonly IMillBundleStateLock _millLock;
    private readonly IWipBundleRunningPoProvider _wipRunningPo;
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly ILogger<PoLifecycleSweepWorker> _logger;

    public PoLifecycleSweepWorker(
        IPoLifecycleService lifecycle,
        PoEndWorkflowService poEndWorkflow,
        INdtBatchStateService batchState,
        INdtBundleRuntimeStateStore runtimeState,
        IBundleEngine bundleEngine,
        IBundleOutputWriter outputWriter,
        IMillBundleStateLock millLock,
        IWipBundleRunningPoProvider wipRunningPo,
        IOptionsMonitor<NdtBundleOptions> options,
        ILogger<PoLifecycleSweepWorker> logger)
    {
        _lifecycle = lifecycle;
        _poEndWorkflow = poEndWorkflow;
        _batchState = batchState;
        _runtimeState = runtimeState;
        _bundleEngine = bundleEngine;
        _outputWriter = outputWriter;
        _millLock = millLock;
        _wipRunningPo = wipRunningPo;
        _options = options;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("PoLifecycleSweepWorker started (Plc drain expiry + orphan sweep).");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await SweepOnceAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PO lifecycle sweep failed.");
            }

            var seconds = Math.Max(5, _options.CurrentValue.PoLifecycleSweepIntervalSeconds);
            await Task.Delay(TimeSpan.FromSeconds(seconds), stoppingToken).ConfigureAwait(false);
        }
    }

    internal async Task SweepOnceAsync(CancellationToken cancellationToken)
    {
        var opts = _options.CurrentValue;
        var drainWindow = TimeSpan.FromMinutes(Math.Max(1, opts.PoEndDrainMinutes));
        var now = DateTime.UtcNow;

        foreach (var entry in _lifecycle.GetExpiredDrains(now, drainWindow))
        {
            if (MillPoEndSourceResolver.ForMill(entry.MillNo, opts) != MillPoEndSource.Plc)
                continue;

            await CompleteDrainAsync(entry, cancellationToken).ConfigureAwait(false);
        }

        if (!opts.AutoCloseOrphanBundles)
            return;

        foreach (var entry in _lifecycle.GetClosedEntries())
        {
            if (MillPoEndSourceResolver.ForMill(entry.MillNo, opts) != MillPoEndSource.Plc)
                continue;

            await CloseOrphanIfNeededAsync(entry, now, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task CompleteDrainAsync(PoLifecycleDrainEntry entry, CancellationToken cancellationToken)
    {
        var correlationId = Guid.NewGuid();
        using (LogContext.PushProperty("CorrelationId", correlationId))
        {
            var bundleLock = await _millLock.AcquireAsync(entry.MillNo, cancellationToken).ConfigureAwait(false);
            try
            {
                // Re-check under lock: still draining?
                if (_lifecycle.GetPhase(entry.MillNo, entry.PoNumber) != PoLifecyclePhase.Draining)
                    return;

                _logger.LogInformation(
                    "Drain window expired — flushing partials for PO {PO} Mill {Mill}. CorrelationId {CorrelationId}",
                    entry.PoNumber,
                    entry.MillNo,
                    correlationId);

                await _poEndWorkflow.FlushPartialsAsync(entry.PoNumber, entry.MillNo, correlationId, cancellationToken)
                    .ConfigureAwait(false);
                await _batchState.IncrementBatchOnPoEndAsync(entry.PoNumber, entry.MillNo, cancellationToken)
                    .ConfigureAwait(false);
                await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);
                _lifecycle.TryMarkClosed(entry.MillNo, entry.PoNumber);

                _logger.LogInformation(
                    "Drain flush complete — PO {PO} Mill {Mill} marked Closed. CorrelationId {CorrelationId}",
                    entry.PoNumber,
                    entry.MillNo,
                    correlationId);
            }
            finally
            {
                bundleLock.Dispose();
            }
        }
    }

    private async Task CloseOrphanIfNeededAsync(PoLifecycleDrainEntry entry, DateTime utcNow, CancellationToken cancellationToken)
    {
        var phase = _lifecycle.GetPhase(entry.MillNo, entry.PoNumber);
        var millRunningPo = await _wipRunningPo
            .TryGetRunningPoForMillAsync(entry.MillNo, cancellationToken)
            .ConfigureAwait(false);
        var opts = _options.CurrentValue;

        if (!OrphanSweepGuard.ShouldSweepClosedPo(
                entry.MillNo,
                entry.PoNumber,
                phase,
                millRunningPo,
                _runtimeState.GetLastActivityUtc(entry.PoNumber, entry.MillNo),
                utcNow,
                opts.OrphanQuiescenceMinutes))
            return;

        await _runtimeState.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sizeCounts = _runtimeState.GetSizeCounts(entry.PoNumber, entry.MillNo);
        var running = _runtimeState.GetRunningTotal(entry.PoNumber, entry.MillNo);
        var hasOpen = running > 0 || sizeCounts.Values.Any(v => v > 0);
        if (!hasOpen)
            return;

        var correlationId = Guid.NewGuid();
        using (LogContext.PushProperty("CorrelationId", correlationId))
        {
            var bundleLock = await _millLock.AcquireAsync(entry.MillNo, cancellationToken).ConfigureAwait(false);
            try
            {
                phase = _lifecycle.GetPhase(entry.MillNo, entry.PoNumber);
                millRunningPo = await _wipRunningPo
                    .TryGetRunningPoForMillAsync(entry.MillNo, cancellationToken)
                    .ConfigureAwait(false);
                if (!OrphanSweepGuard.ShouldSweepClosedPo(
                        entry.MillNo,
                        entry.PoNumber,
                        phase,
                        millRunningPo,
                        _runtimeState.GetLastActivityUtc(entry.PoNumber, entry.MillNo),
                        utcNow,
                        opts.OrphanQuiescenceMinutes))
                    return;

                sizeCounts = _runtimeState.GetSizeCounts(entry.PoNumber, entry.MillNo);
                running = _runtimeState.GetRunningTotal(entry.PoNumber, entry.MillNo);
                hasOpen = running > 0 || sizeCounts.Values.Any(v => v > 0);
                if (!hasOpen)
                    return;

                _logger.LogWarning(
                    "Orphan open bundle detected for Closed PO {PO} Mill {Mill} (running={Running}); auto close-and-print. CorrelationId {CorrelationId}",
                    entry.PoNumber,
                    entry.MillNo,
                    running,
                    correlationId);

                await _bundleEngine.HandlePoEndAsync(
                    entry.PoNumber,
                    entry.MillNo,
                    async (contextRecord, batchNo, totalNdtPcs) =>
                    {
                        if (totalNdtPcs <= 0)
                            return;

                        await _outputWriter
                            .WriteBundleAsync(contextRecord, batchNo, totalNdtPcs, cancellationToken, correlationId)
                            .ConfigureAwait(false);
                        _logger.LogInformation(
                            "Orphan bundle closed: PO {PO} Mill {Mill} Batch {Batch} NdtPcs {Pcs}. CorrelationId {CorrelationId}",
                            entry.PoNumber,
                            entry.MillNo,
                            batchNo,
                            totalNdtPcs,
                            correlationId);
                    },
                    cancellationToken,
                    correlationId).ConfigureAwait(false);

                await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                bundleLock.Dispose();
            }
        }
    }
}
