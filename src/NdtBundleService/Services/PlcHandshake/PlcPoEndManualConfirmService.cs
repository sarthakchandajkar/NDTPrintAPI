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
/// Expected outcome shown on the dashboard before the operator confirms a held PLC PO-end.
/// </summary>
public sealed class PlcPoEndManualConfirmExpectedOutcome
{
    public string? PoNumber { get; init; }
    public string? ExpectedNdtBatchNumber { get; init; }
    public int ExpectedNdtPipeCount { get; init; }
    public bool WillFormBundle { get; init; }
    public string? PipeType { get; init; }
    public string? ExpectedPrinterTarget { get; init; }
    public string? ExpectedPrinterDisplayName { get; init; }
    /// <summary>True when mill was / is waiting for a new WIP after a prior PO end.</summary>
    public bool WaitingForNewWip { get; init; }
    /// <summary>Ended SAP PO from WIP-wait context (candidate when RunningPo is unknown).</summary>
    public string? EndedPo { get; init; }
    /// <summary>Which source supplied <see cref="PoNumber"/> (diagnostics).</summary>
    public string? PoResolveSource { get; init; }
}

/// <summary>
/// Operator confirm for <see cref="PlcHandshakeOptions.ManualConfirmPoEnd"/>:
/// flush remainder (live NDT merge) then complete MES ack on the mill handshake loop.
/// </summary>
public interface IPlcPoEndManualConfirmService
{
    IReadOnlyList<PlcPoEndManualConfirmPending> ListPending();

    PlcPoEndManualConfirmPending? TryGetPending(int millNo);

    Task<PlcPoEndManualConfirmExpectedOutcome> BuildExpectedOutcomeAsync(
        PlcPoEndManualConfirmPending pending,
        CancellationToken cancellationToken);

    Task<PlcPoEndManualConfirmResult> ConfirmAsync(
        int millNo,
        CancellationToken cancellationToken,
        bool ackOnly = false);
}

public sealed class PlcPoEndManualConfirmService : IPlcPoEndManualConfirmService
{
    private readonly IPlcPoEndManualConfirmStore _store;
    private readonly IPoEndWorkflowService _poEndWorkflow;
    private readonly INdtBundleRuntimeStateStore _runtimeState;
    private readonly IPipeSizeProvider _pipeSizeProvider;
    private readonly IWipBundleRunningPoProvider _wipRunningPo;
    private readonly IActivePoPerMillService _activePoPerMill;
    private readonly IWipLabelProvider _wipLabelProvider;
    private readonly IMillSequenceService? _millSequence;
    private readonly IMillPrinterSettingsService _millPrinters;
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
        IWipLabelProvider wipLabelProvider,
        IMillPrinterSettingsService millPrinters,
        PlcHandshakeStatusRegistry handshakeStatus,
        PlcHandshakeCoordinator coordinator,
        IOptionsMonitor<NdtBundleOptions> options,
        ILogger<PlcPoEndManualConfirmService> logger,
        IMillSequenceService? millSequence = null)
    {
        _store = store;
        _poEndWorkflow = poEndWorkflow;
        _runtimeState = runtimeState;
        _pipeSizeProvider = pipeSizeProvider;
        _wipRunningPo = wipRunningPo;
        _activePoPerMill = activePoPerMill;
        _wipLabelProvider = wipLabelProvider;
        _millPrinters = millPrinters;
        _handshakeStatus = handshakeStatus;
        _coordinator = coordinator;
        _options = options;
        _logger = logger;
        _millSequence = millSequence;
    }

    public IReadOnlyList<PlcPoEndManualConfirmPending> ListPending() => _store.List();

    public PlcPoEndManualConfirmPending? TryGetPending(int millNo) =>
        _store.TryGet(millNo, out var p) ? p : null;

    public async Task<PlcPoEndManualConfirmExpectedOutcome> BuildExpectedOutcomeAsync(
        PlcPoEndManualConfirmPending pending,
        CancellationToken cancellationToken)
    {
        var (po, resolveSource, waiting, endedPo) = await ResolvePoWithContextAsync(pending, cancellationToken)
            .ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(po))
        {
            return new PlcPoEndManualConfirmExpectedOutcome
            {
                ExpectedNdtPipeCount = 0,
                WillFormBundle = false,
                WaitingForNewWip = waiting,
                EndedPo = endedPo,
                PoResolveSource = resolveSource
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
        if (_handshakeStatus.TryGetMill(pending.MillNo, out var st) && st?.NdtCount is int n)
            liveNdt = Math.Max(0, n);
        liveNdt = Math.Max(liveNdt, Math.Max(0, pending.NdtAtEdge));

        var remainder = PoEndRemainderResolver.ResolveForManualConfirm(
            po,
            pending.MillNo,
            pipeSize,
            _runtimeState,
            _handshakeStatus,
            liveNdt);

        var wip = await _wipLabelProvider
            .GetWipLabelAsync(po, pending.MillNo, cancellationToken)
            .ConfigureAwait(false);
        var pipeType = wip?.PipeType?.Trim();
        if (string.IsNullOrEmpty(pipeType))
            pipeType = null;

        var printerTarget = ReprintPrinterTarget.FromPipeType(pending.MillNo, pipeType);
        var (_, _, _, printerDisplay) = _millPrinters.ResolveReprintTarget(pending.MillNo, printerTarget);

        string? expectedBatch = null;
        if (_millSequence is { IsEnabled: true })
        {
            expectedBatch = await _millSequence
                .TryPeekClaimableOpenFillBundleNoAsync(po, pending.MillNo, cancellationToken)
                .ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(expectedBatch))
            {
                var snap = await _millSequence
                    .GetSnapshotAsync(pending.MillNo, cancellationToken)
                    .ConfigureAwait(false);
                expectedBatch = snap?.NextBundleNo;
            }
        }

        return new PlcPoEndManualConfirmExpectedOutcome
        {
            PoNumber = po,
            ExpectedNdtBatchNumber = expectedBatch,
            ExpectedNdtPipeCount = remainder,
            WillFormBundle = remainder > 0,
            PipeType = pipeType,
            ExpectedPrinterTarget = printerTarget,
            ExpectedPrinterDisplayName = printerDisplay,
            WaitingForNewWip = waiting,
            EndedPo = endedPo,
            PoResolveSource = resolveSource
        };
    }

    public async Task<PlcPoEndManualConfirmResult> ConfirmAsync(
        int millNo,
        CancellationToken cancellationToken,
        bool ackOnly = false)
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
            var (po, resolveSource, waiting, endedPo) = await ResolvePoWithContextAsync(pending, cancellationToken)
                .ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(po))
            {
                _logger.LogWarning(
                    "Manual PO-end confirm Mill {Mill}: could not resolve SAP PO "
                    + "(PoIdAtEdge={PoId}, RunningPoAtEdge={RunningAtEdge}, EndedPoAtEdge={EndedAtEdge}, "
                    + "WaitingForNewWip={Waiting}, LiveEndedPo={LiveEnded}, CorrelationId {CorrelationId}). "
                    + "Check running PO / Input Slit / WIP-wait EndedPo.",
                    millNo,
                    pending.PoIdAtEdge,
                    pending.RunningPoAtEdge ?? "(null)",
                    pending.EndedPoAtEdge ?? "(null)",
                    waiting || pending.WaitingForNewWipAtEdge,
                    endedPo ?? "(null)",
                    pending.CorrelationId);

                _store.Arm(pending); // put back so operator can retry after WIP/PO is known
                return new PlcPoEndManualConfirmResult
                {
                    Success = false,
                    MillNo = millNo,
                    CorrelationId = pending.CorrelationId,
                    Message = "Could not resolve SAP PO for this pending PO change. Check running PO / Input Slit."
                };
            }

            _logger.LogInformation(
                "Manual PO-end confirm Mill {Mill}: resolved SAP PO {PO} via {Source} "
                + "(WaitingForNewWip={Waiting}, EndedPo={EndedPo}). CorrelationId {CorrelationId}",
                millNo,
                po,
                resolveSource ?? "(unknown)",
                waiting || pending.WaitingForNewWipAtEdge,
                endedPo ?? pending.EndedPoAtEdge ?? "(none)",
                pending.CorrelationId);

            // Spurious M40.6 while still waiting for WIP after a completed PO-end: ack MES only —
            // do not invent a second remainder flush for the already-ended PO.
            if (!ackOnly
                && (waiting || pending.WaitingForNewWipAtEdge)
                && resolveSource is PlcPoEndManualConfirmPoResolution.SourceEndedPoAtEdge
                    or PlcPoEndManualConfirmPoResolution.SourceLiveEndedPo)
            {
                await _runtimeState.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
                string? probePipeSize = null;
                try
                {
                    probePipeSize = await _pipeSizeProvider.TryGetPipeSizeForPoAsync(po, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch
                {
                    /* default */
                }

                var probeLiveNdt = 0;
                if (_handshakeStatus.TryGetMill(millNo, out var st0) && st0?.NdtCount is int n0)
                    probeLiveNdt = Math.Max(0, n0);
                probeLiveNdt = Math.Max(probeLiveNdt, Math.Max(0, pending.NdtAtEdge));

                var remainderProbe = PoEndRemainderResolver.ResolveForManualConfirm(
                    po,
                    millNo,
                    probePipeSize,
                    _runtimeState,
                    _handshakeStatus,
                    probeLiveNdt);

                if (remainderProbe <= 0)
                {
                    _logger.LogWarning(
                        "Manual PO-end confirm Mill {Mill}: PO {PO} resolved from EndedPo while waiting for WIP "
                        + "and remainder=0 — switching to ack-only (no second flush). CorrelationId {CorrelationId}",
                        millNo,
                        po,
                        pending.CorrelationId);
                    ackOnly = true;
                }
            }

            if (ackOnly)
            {
                _logger.LogWarning(
                    "Manual PO-end ACK-ONLY Mill {Mill}: PO {PO} (no remainder flush). CorrelationId {CorrelationId}",
                    millNo,
                    po,
                    pending.CorrelationId);

                var ackOnlyOk = await _coordinator
                    .CompleteManualConfirmAckAsync(millNo, cancellationToken)
                    .ConfigureAwait(false);

                return new PlcPoEndManualConfirmResult
                {
                    Success = ackOnlyOk,
                    MillNo = millNo,
                    PoNumber = po,
                    RemainderPcs = 0,
                    BundlesClosed = 0,
                    LiveNdtAtConfirm = Math.Max(0, pending.NdtAtEdge),
                    CorrelationId = pending.CorrelationId,
                    Message = ackOnlyOk
                        ? $"PO end MES ack sent for {po} Mill {millNo} (ack-only; no bundle flush)."
                        : $"MES ack could not be completed for {po} Mill {millNo} (handshake loop not registered?)."
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
        var (po, _, _, _) = await ResolvePoWithContextAsync(pending, cancellationToken).ConfigureAwait(false);
        return po;
    }

    private async Task<(string? Po, string? Source, bool Waiting, string? EndedPo)> ResolvePoWithContextAsync(
        PlcPoEndManualConfirmPending pending,
        CancellationToken cancellationToken)
    {
        var plcCfg = _options.CurrentValue.PlcPoEnd ?? new PlcPoEndOptions();

        string? liveRunning = null;
        try
        {
            liveRunning = await _wipRunningPo.TryGetRunningPoForMillAsync(pending.MillNo, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Manual confirm: live running PO lookup failed for Mill {Mill}.", pending.MillNo);
        }

        var waiting = pending.WaitingForNewWipAtEdge
                      || _wipRunningPo.IsWaitingForNewWipAfterPoEnd(pending.MillNo);
        string? liveEnded = null;
        if (_wipRunningPo.TryGetPoEndWaitContext(pending.MillNo, out var waitNow, out var endedNow))
        {
            if (waitNow)
                waiting = true;
            liveEnded = endedNow;
        }

        string? activeOrSlit = null;
        if (!waiting)
        {
            try
            {
                var byMill = await _activePoPerMill.GetLatestPoByMillAsync(cancellationToken).ConfigureAwait(false);
                if (byMill.TryGetValue(pending.MillNo, out var slitPo) && !string.IsNullOrWhiteSpace(slitPo))
                    activeOrSlit = slitPo;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Manual confirm: ActivePo/slit lookup failed for Mill {Mill}.", pending.MillNo);
            }
        }

        var po = PlcPoEndManualConfirmPoResolution.TryResolve(
            pending.PoIdAtEdge,
            pending.RunningPoAtEdge,
            liveRunning,
            waiting,
            pending.EndedPoAtEdge,
            liveEnded,
            activeOrSlit,
            plcCfg,
            out var source);

        var endedForUi = InputSlitCsvParsing.NormalizePo(pending.EndedPoAtEdge)
                         ?? InputSlitCsvParsing.NormalizePo(liveEnded);
        if (string.IsNullOrWhiteSpace(endedForUi))
            endedForUi = null;

        return (po, source, waiting, endedForUi);
    }
}
