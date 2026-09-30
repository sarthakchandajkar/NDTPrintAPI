using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.PlcHandshake.PlcPoEnd;

namespace NdtBundleService.Services.PlcHandshake;

public sealed class PlcPoEndManualConfirmResult
{
    public bool Success { get; init; }
    public string Message { get; init; } = "";
    public int MillNo { get; init; }
    public string? PoNumber { get; init; }
    public int RemainderPcs { get; init; }
    public int BundlesClosed { get; init; }
    public int LiveNdtAtConfirm { get; init; }
    public Guid? CorrelationId { get; init; }
}

/// <summary>
/// Operator confirm for <see cref="PlcHandshakeOptions.ManualConfirmPoEnd"/>:
/// flush remainder (live NDT merge) then complete MES ack on the mill handshake loop.
/// </summary>
public interface IPlcPoEndManualConfirmService
{
    IReadOnlyList<PlcPoEndManualConfirmPending> ListPending();

    PlcPoEndManualConfirmPending? TryGetPending(int millNo);

    Task<PlcPoEndManualConfirmResult> ConfirmAsync(int millNo, CancellationToken cancellationToken);
}

public sealed class PlcPoEndManualConfirmService : IPlcPoEndManualConfirmService
{
    private readonly IPlcPoEndManualConfirmStore _store;
    private readonly IPoEndWorkflowService _poEndWorkflow;
    private readonly INdtBundleRuntimeStateStore _runtimeState;
    private readonly IPipeSizeProvider _pipeSizeProvider;
    private readonly IWipBundleRunningPoProvider _wipRunningPo;
    private readonly IActivePoPerMillService _activePoPerMill;
    private readonly PlcHandshakeStatusRegistry _handshakeStatus;
    private readonly PlcHandshakeCoordinator _coordinator;
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly ILogger<PlcPoEndManualConfirmService> _logger;

    public PlcPoEndManualConfirmService(
        IPlcPoEndManualConfirmStore store,
        IPoEndWorkflowService poEndWorkflow,
        INdtBundleRuntimeStateStore runtimeState,
        IPipeSizeProvider pipeSizeProvider,
        IWipBundleRunningPoProvider wipRunningPo,
        IActivePoPerMillService activePoPerMill,
        PlcHandshakeStatusRegistry handshakeStatus,
        PlcHandshakeCoordinator coordinator,
        IOptionsMonitor<NdtBundleOptions> options,
        ILogger<PlcPoEndManualConfirmService> logger)
    {
        _store = store;
        _poEndWorkflow = poEndWorkflow;
        _runtimeState = runtimeState;
        _pipeSizeProvider = pipeSizeProvider;
        _wipRunningPo = wipRunningPo;
        _activePoPerMill = activePoPerMill;
        _handshakeStatus = handshakeStatus;
        _coordinator = coordinator;
        _options = options;
        _logger = logger;
    }

    public IReadOnlyList<PlcPoEndManualConfirmPending> ListPending() => _store.List();

    public PlcPoEndManualConfirmPending? TryGetPending(int millNo) =>
        _store.TryGet(millNo, out var p) ? p : null;

    public async Task<PlcPoEndManualConfirmResult> ConfirmAsync(int millNo, CancellationToken cancellationToken)
    {
        if (millNo is < 1 or > 4)
        {
            return new PlcPoEndManualConfirmResult
            {
                Success = false,
                MillNo = millNo,
                Message = "MillNo must be 1–4."
            };
        }

        if (!_store.TryTake(millNo, out var pending))
        {
            return new PlcPoEndManualConfirmResult
            {
                Success = false,
                MillNo = millNo,
                Message = $"No pending PLC PO-end confirm for Mill {millNo}."
            };
        }

        try
        {
            var po = await ResolvePoNumberAsync(pending, cancellationToken).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(po))
            {
                _store.Arm(pending); // put back so operator can retry after WIP/PO is known
                return new PlcPoEndManualConfirmResult
                {
                    Success = false,
                    MillNo = millNo,
                    CorrelationId = pending.CorrelationId,
                    Message = "Could not resolve SAP PO for this pending PO change. Check running PO / Input Slit."
                };
            }

            await _runtimeState.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            string? pipeSize = null;
            try
            {
                pipeSize = await _pipeSizeProvider.TryGetPipeSizeForPoAsync(po, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                /* default */
            }

            var liveNdt = 0;
            if (_handshakeStatus.TryGetMill(millNo, out var st) && st?.NdtCount is int n)
                liveNdt = Math.Max(0, n);

            // Prefer a fresh edge/live max — registry may briefly lag; keep at-edge as floor.
            liveNdt = Math.Max(liveNdt, Math.Max(0, pending.NdtAtEdge));

            var remainder = PoEndRemainderResolver.ResolveForManualConfirm(
                po,
                millNo,
                pipeSize,
                _runtimeState,
                _handshakeStatus,
                liveNdt);

            var sizeKey = FormationChartLookup.NormalizePipeSizeKey(pipeSize);
            if (string.IsNullOrEmpty(sizeKey))
                sizeKey = "Default";

            if (remainder > 0)
            {
                _runtimeState.SetSizeCounts(
                    po,
                    millNo,
                    new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase) { [sizeKey] = remainder });
                await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);
            }

            _logger.LogInformation(
                "Manual PO-end confirm Mill {Mill}: PO {PO} remainder={Remainder} liveNdt={LiveNdt} sizeKey={SizeKey} CorrelationId {CorrelationId}",
                millNo,
                po,
                remainder,
                liveNdt,
                sizeKey,
                pending.CorrelationId);

            var advancePlan = _options.CurrentValue.PlcHandshake?.AdvancePoPlanFileOnPoEnd == true;
            var workflow = await _poEndWorkflow
                .ExecuteAsync(po, millNo, advancePlan, cancellationToken, pending.CorrelationId, liveNdt)
                .ConfigureAwait(false);

            var ackOk = await _coordinator
                .CompleteManualConfirmAckAsync(millNo, cancellationToken)
                .ConfigureAwait(false);

            return new PlcPoEndManualConfirmResult
            {
                Success = ackOk,
                MillNo = millNo,
                PoNumber = po,
                RemainderPcs = workflow.TotalNdtPcsClosed > 0 ? workflow.TotalNdtPcsClosed : remainder,
                BundlesClosed = workflow.BundlesClosed,
                LiveNdtAtConfirm = liveNdt,
                CorrelationId = pending.CorrelationId,
                Message = ackOk
                    ? $"PO end confirmed for {po} Mill {millNo}: printed {workflow.TotalNdtPcsClosed} pcs ({workflow.BundlesClosed} bundle(s)); MES ack sent."
                    : $"Remainder flushed for {po} Mill {millNo} but MES ack could not be completed (handshake loop not registered?)."
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Manual PO-end confirm failed for Mill {Mill}.", millNo);
            _store.Arm(pending);
            return new PlcPoEndManualConfirmResult
            {
                Success = false,
                MillNo = millNo,
                CorrelationId = pending.CorrelationId,
                Message = ex.Message
            };
        }
    }

    private async Task<string?> ResolvePoNumberAsync(
        PlcPoEndManualConfirmPending pending,
        CancellationToken cancellationToken)
    {
        var plcCfg = _options.CurrentValue.PlcPoEnd ?? new PlcPoEndOptions();
        if (PlcPoNumberResolution.TryResolveFromPlcPoId(pending.PoIdAtEdge, plcCfg, out var fromPlc))
            return fromPlc;

        if (!string.IsNullOrWhiteSpace(pending.RunningPoAtEdge))
            return InputSlitCsvParsing.NormalizePo(pending.RunningPoAtEdge);

        var running = await _wipRunningPo.TryGetRunningPoForMillAsync(pending.MillNo, cancellationToken)
            .ConfigureAwait(false);
        if (!string.IsNullOrWhiteSpace(running))
            return InputSlitCsvParsing.NormalizePo(running);

        var byMill = await _activePoPerMill.GetLatestPoByMillAsync(cancellationToken).ConfigureAwait(false);
        if (byMill.TryGetValue(pending.MillNo, out var slitPo) && !string.IsNullOrWhiteSpace(slitPo))
            return InputSlitCsvParsing.NormalizePo(slitPo);

        return null;
    }
}
