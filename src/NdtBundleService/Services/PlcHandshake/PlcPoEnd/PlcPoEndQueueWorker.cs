using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using Serilog.Context;

namespace NdtBundleService.Services.PlcHandshake.PlcPoEnd;

/// <summary>
/// Drains <see cref="PlcPoEndQueue"/> and runs the PO end workflow asynchronously, decoupled from the PLC poll loop.
/// </summary>
public sealed class PlcPoEndQueueWorker : BackgroundService
{
    private readonly PlcPoEndQueue _queue;
    private readonly IPoEndWorkflowService _poEndWorkflow;
    private readonly IActivePoPerMillService _activePoPerMill;
    private readonly IWipBundleRunningPoProvider _wipRunningPo;
    private readonly PlcHandshakeCoordinator _coordinator;
    private readonly PlcHandshakeStatusRegistry _statusRegistry;
    private readonly IOptions<NdtBundleOptions> _options;
    private readonly ILogger<PlcPoEndQueueWorker> _logger;

    public PlcPoEndQueueWorker(
        PlcPoEndQueue queue,
        IPoEndWorkflowService poEndWorkflow,
        IActivePoPerMillService activePoPerMill,
        IWipBundleRunningPoProvider wipRunningPo,
        PlcHandshakeCoordinator coordinator,
        PlcHandshakeStatusRegistry statusRegistry,
        IOptions<NdtBundleOptions> options,
        ILogger<PlcPoEndQueueWorker> logger)
    {
        _queue = queue;
        _poEndWorkflow = poEndWorkflow;
        _activePoPerMill = activePoPerMill;
        _wipRunningPo = wipRunningPo;
        _coordinator = coordinator;
        _statusRegistry = statusRegistry;
        _options = options;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var handshake = _options.Value.PlcHandshake ?? new PlcHandshakeOptions();
        var usesTcpOpen = MillPoEndSourceResolver.AnyMillUsesTcpOpenPoEnd(_options.Value);
        var usesPlc = MillPoEndSourceResolver.AnyMillUsesPlcPoEnd(_options.Value);

        if (!usesPlc && !usesTcpOpen)
        {
            _logger.LogInformation(
                "No mill has PoEndSource=Plc or TcpOpen; PlcPoEndQueueWorker will not process PO end events.");
            return;
        }

        if (!usesTcpOpen)
        {
            if (!handshake.Enabled)
            {
                _logger.LogInformation("PlcHandshake is disabled; PlcPoEndQueueWorker will not process PLC PO end events.");
                return;
            }

            if (handshake.TelemetryOnly)
            {
                _logger.LogInformation(
                    "PlcHandshake.TelemetryOnly is true; PlcPoEndQueueWorker will not process PLC PO end events.");
                return;
            }

            if (!usesPlc)
            {
                _logger.LogInformation(
                    "No mill has PoEndSource=Plc; PlcPoEndQueueWorker will not process PLC PO end events.");
                return;
            }
        }

        _logger.LogInformation(
            "PlcPoEndQueueWorker started — PO end sources: Plc={UsesPlc}, TcpOpen={UsesTcpOpen}.",
            usesPlc,
            usesTcpOpen);

        await foreach (var request in _queue.Reader.ReadAllAsync(stoppingToken).ConfigureAwait(false))
        {
            try
            {
                await ProcessRequestAsync(request, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                using (LogContext.PushProperty("CorrelationId", request.CorrelationId))
                {
                    _logger.LogError(
                        ex,
                        "PLC PO end workflow failed — Mill {MillNo}, PO_Id {PoId}, CorrelationId {CorrelationId}.",
                        request.MillNo,
                        request.PoId,
                        request.CorrelationId);
                }
            }
            finally
            {
                _queue.MarkCompleted(request.MillNo);
            }
        }
    }

    private async Task ProcessRequestAsync(PlcPoEndRequest request, CancellationToken cancellationToken)
    {
        using (LogContext.PushProperty("CorrelationId", request.CorrelationId))
        {
            _logger.LogInformation(
                "PLC PO end event dequeued — Mill {MillNo}, PO_Id {PoId}, NDT {NdtCount}, CorrelationId {CorrelationId}.",
                request.MillNo,
                request.PoId,
                request.NdtCountFinal,
                request.CorrelationId);

            // F-6.1: resolve while RunningPo is still set — before NotifyPoEndForMill clears it in the workflow.
            var po = await ResolvePoNumberAsync(request, cancellationToken).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(po))
            {
                _logger.LogWarning(
                    "PLC PO end workflow skipped — Mill {MillNo}, PO_Id {PoId}, no PO resolved, CorrelationId {CorrelationId}.",
                    request.MillNo,
                    request.PoId,
                    request.CorrelationId);
                return;
            }

            var handshake = _options.Value.PlcHandshake ?? new PlcHandshakeOptions();
            var advancePlan = handshake.AdvancePoPlanFileOnPoEnd;

            _logger.LogInformation(
                "PLC PO end workflow started — Mill {MillNo}, PO {PO}, CorrelationId {CorrelationId}.",
                request.MillNo,
                po,
                request.CorrelationId);

            await _poEndWorkflow.ExecuteAsync(po, request.MillNo, advancePlan, cancellationToken, request.CorrelationId)
                .ConfigureAwait(false);

            _statusRegistry.UpdateMill(request.MillNo, s => s.LastPoChangeUtc = DateTimeOffset.UtcNow);
            await _coordinator.NotifyPoEndWorkflowCompletedAsync(request.MillNo, cancellationToken).ConfigureAwait(false);

            _logger.LogInformation(
                "PLC PO end workflow completed — Mill {MillNo}, PO {PO}, CorrelationId {CorrelationId}.",
                request.MillNo,
                po,
                request.CorrelationId);
        }
    }

    /// <summary>
    /// Resolves the ended SAP PO. Order: plausible PLC PO_Id → mill running PO (WIP state) → latest Input Slit CSV.
    /// Running-PO lookup must run before <see cref="IWipBundleRunningPoProvider.NotifyPoEndForMill"/>.
    /// </summary>
    internal async Task<string> ResolvePoNumberAsync(PlcPoEndRequest request, CancellationToken cancellationToken)
    {
        var plcCfg = _options.Value.PlcPoEnd ?? new PlcPoEndOptions();

        if (PlcPoNumberResolution.TryResolveFromPlcPoId(request.PoId, plcCfg, out var fromPlc))
        {
            _logger.LogDebug(
                "Mill {MillNo}: PO resolved from PLC PO_Id {PoId} → {PO}, CorrelationId {CorrelationId}.",
                request.MillNo,
                request.PoId,
                fromPlc,
                request.CorrelationId);
            return fromPlc;
        }

        if (request.PoId is not 0)
        {
            _logger.LogInformation(
                "Mill {MillNo}: PLC PO_Id {PoId} is not a plausible SAP PO (range {Min}–{Max}, min {Digits} digits); resolving from running PO / Input Slit CSV. CorrelationId {CorrelationId}.",
                request.MillNo,
                request.PoId,
                plcCfg.MinValidPoId,
                plcCfg.MaxValidPoId,
                plcCfg.MinSapPoNumberDigits,
                request.CorrelationId);
        }

        var runningPo = await _wipRunningPo.TryGetRunningPoForMillAsync(request.MillNo, cancellationToken).ConfigureAwait(false);
        runningPo = string.IsNullOrWhiteSpace(runningPo) ? null : InputSlitCsvParsing.NormalizePo(runningPo);

        string? slitPo = null;
        var poByMill = await _activePoPerMill.GetLatestPoByMillAsync(cancellationToken).ConfigureAwait(false);
        if (poByMill.TryGetValue(request.MillNo, out var fromSlit) && !string.IsNullOrWhiteSpace(fromSlit))
            slitPo = InputSlitCsvParsing.NormalizePo(fromSlit);

        if (!string.IsNullOrWhiteSpace(runningPo))
        {
            if (!string.IsNullOrWhiteSpace(slitPo) && !InputSlitCsvParsing.PoEquals(runningPo, slitPo))
            {
                _logger.LogWarning(
                    "Mill {MillNo}: running PO {RunningPo} differs from latest Input Slit CSV PO {SlitPo} (PLC PO_Id {PoId}); using running PO. CorrelationId {CorrelationId}.",
                    request.MillNo,
                    runningPo,
                    slitPo,
                    request.PoId,
                    request.CorrelationId);
            }
            else
            {
                _logger.LogInformation(
                    "Mill {MillNo}: PO resolved from running PO state as {PO} (PLC PO_Id {PoId}), CorrelationId {CorrelationId}.",
                    request.MillNo,
                    runningPo,
                    request.PoId,
                    request.CorrelationId);
            }

            return runningPo;
        }

        if (!string.IsNullOrWhiteSpace(slitPo))
        {
            _logger.LogInformation(
                "Mill {MillNo}: PO resolved from latest Input Slit CSV as {PO} (PLC PO_Id {PoId}), CorrelationId {CorrelationId}.",
                request.MillNo,
                slitPo,
                request.PoId,
                request.CorrelationId);
            return slitPo;
        }

        return string.Empty;
    }
}
