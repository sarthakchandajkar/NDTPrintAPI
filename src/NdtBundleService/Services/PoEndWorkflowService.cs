using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.PlcHandshake;
using NdtBundleService.Services.PoLifecycle;
using Serilog.Context;

namespace NdtBundleService.Services;

/// <inheritdoc />
public sealed class PoEndWorkflowService : IPoEndWorkflowService
{
    private readonly IBundleEngine _bundleEngine;
    private readonly IBundleOutputWriter _outputWriter;
    private readonly INdtBatchStateService _batchState;
    private readonly INdtBundleRuntimeStateStore _runtimeState;
    private readonly ICurrentPoPlanService? _currentPoPlanService;
    private readonly IMillSlitLiveNdtAccumulator _liveNdtAccumulator;
    private readonly IWipBundleRunningPoProvider _wipRunningPo;
    private readonly IMillBundleStateLock _millBundleStateLock;
    private readonly IPoLifecycleService _poLifecycle;
    private readonly IPipeSizeProvider _pipeSizeProvider;
    private readonly INdtBundleRepository _bundleRepository;
    private readonly PlcHandshakeStatusRegistry _handshakeStatus;
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly ILogger<PoEndWorkflowService> _logger;

    public PoEndWorkflowService(
        IBundleEngine bundleEngine,
        IBundleOutputWriter outputWriter,
        INdtBatchStateService batchState,
        INdtBundleRuntimeStateStore runtimeState,
        IMillSlitLiveNdtAccumulator liveNdtAccumulator,
        IWipBundleRunningPoProvider wipRunningPo,
        IMillBundleStateLock millBundleStateLock,
        IPoLifecycleService poLifecycle,
        IPipeSizeProvider pipeSizeProvider,
        INdtBundleRepository bundleRepository,
        PlcHandshakeStatusRegistry handshakeStatus,
        IOptionsMonitor<NdtBundleOptions> options,
        ILogger<PoEndWorkflowService> logger,
        ICurrentPoPlanService? currentPoPlanService = null)
    {
        _bundleEngine = bundleEngine;
        _outputWriter = outputWriter;
        _batchState = batchState;
        _runtimeState = runtimeState;
        _liveNdtAccumulator = liveNdtAccumulator;
        _wipRunningPo = wipRunningPo;
        _millBundleStateLock = millBundleStateLock;
        _poLifecycle = poLifecycle;
        _pipeSizeProvider = pipeSizeProvider;
        _bundleRepository = bundleRepository;
        _handshakeStatus = handshakeStatus;
        _options = options;
        _logger = logger;
        _currentPoPlanService = currentPoPlanService;
    }

    public Task<PoEndWorkflowResult> ExecuteAsync(
        string poNumber,
        int millNo,
        bool advancePoPlanFile,
        CancellationToken cancellationToken,
        Guid? correlationId = null) =>
        ExecuteAsync(poNumber, millNo, advancePoPlanFile, cancellationToken, correlationId, plcNdtCountFinal: null);

    public async Task<PoEndWorkflowResult> ExecuteAsync(
        string poNumber,
        int millNo,
        bool advancePoPlanFile,
        CancellationToken cancellationToken,
        Guid? correlationId,
        int? plcNdtCountFinal)
    {
        using (correlationId is { } id ? LogContext.PushProperty("CorrelationId", id) : null)
        {
            return await ExecuteCoreAsync(poNumber, millNo, advancePoPlanFile, cancellationToken, correlationId, plcNdtCountFinal)
                .ConfigureAwait(false);
        }
    }

    private async Task<PoEndWorkflowResult> ExecuteCoreAsync(
        string poNumber,
        int millNo,
        bool advancePoPlanFile,
        CancellationToken cancellationToken,
        Guid? correlationId,
        int? plcNdtCountFinal)
    {
        if (string.IsNullOrWhiteSpace(poNumber))
            throw new ArgumentException("PoNumber is required.", nameof(poNumber));

        if (millNo is < 1 or > 4)
            throw new ArgumentOutOfRangeException(nameof(millNo), millNo, "MillNo must be between 1 and 4.");

        var po = InputSlitCsvParsing.NormalizePo(poNumber.Trim());
        var opts = _options.CurrentValue;
        var poEndSource = MillPoEndSourceResolver.ForMill(millNo, opts);
        var flushMode = ResolveFlushMode(poEndSource, opts);

        _logger.LogInformation(
            "PO end workflow: PO {PO} Mill {Mill} (advance plan file: {Advance}, PoEndSource={Source}, FlushMode={FlushMode}) CorrelationId {CorrelationId}",
            po,
            millNo,
            advancePoPlanFile,
            MillPoEndSourceResolver.ToConfigValue(poEndSource),
            PoEndFlushModeParser.ToConfigValue(flushMode),
            correlationId);

        var bundleLock = await _millBundleStateLock.AcquireAsync(millNo, cancellationToken).ConfigureAwait(false);
        try
        {
            var bundlesClosed = 0;
            var totalNdtPcsClosed = 0;
            var flushDeferred = false;

            // File mills: always Immediate (historical). Plc AfterDrain: defer partial flush.
            if (poEndSource == MillPoEndSource.Plc && flushMode == PoEndFlushMode.AfterDrain)
            {
                flushDeferred = true;
                _poLifecycle.TryMarkDraining(millNo, po, DateTime.UtcNow);
                _logger.LogInformation(
                    "PO end flush deferred (AfterDrain): PO {PO} Mill {Mill}; accepting late slit rows for {DrainMinutes} min. CorrelationId {CorrelationId}",
                    po,
                    millNo,
                    Math.Max(1, opts.PoEndDrainMinutes),
                    correlationId);
            }
            else if (poEndSource == MillPoEndSource.Plc && flushMode == PoEndFlushMode.Immediate)
            {
                var (closed, pcs) = await FlushPlcImmediateRemainderAsync(
                        po,
                        millNo,
                        plcNdtCountFinal,
                        correlationId,
                        cancellationToken)
                    .ConfigureAwait(false);
                bundlesClosed = closed;
                totalNdtPcsClosed = pcs;

                await _batchState.IncrementBatchOnPoEndAsync(po, millNo, cancellationToken).ConfigureAwait(false);
                await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);
                _poLifecycle.TryMarkDraining(millNo, po, DateTime.UtcNow);
            }
            else
            {
                await FlushPartialsAsync(
                        po,
                        millNo,
                        correlationId,
                        cancellationToken,
                        onClosed: (pcs) =>
                        {
                            bundlesClosed++;
                            totalNdtPcsClosed += pcs;
                        })
                    .ConfigureAwait(false);

                await _batchState.IncrementBatchOnPoEndAsync(po, millNo, cancellationToken).ConfigureAwait(false);
                await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);
            }

            _liveNdtAccumulator.OnPoEndForMill(po, millNo);
            _wipRunningPo.NotifyPoEndForMill(millNo, po);

            if (advancePoPlanFile && _currentPoPlanService != null)
                await _currentPoPlanService.AdvanceToNextPoAsync(cancellationToken).ConfigureAwait(false);

            return new PoEndWorkflowResult
            {
                PoNumber = po,
                MillNo = millNo,
                BundlesClosed = bundlesClosed,
                TotalNdtPcsClosed = totalNdtPcsClosed,
                WaitingForNewWip = _wipRunningPo.IsWaitingForNewWipAfterPoEnd(millNo),
                AdvancedPoPlanFile = advancePoPlanFile && _currentPoPlanService != null,
                FlushDeferred = flushDeferred
            };
        }
        finally
        {
            bundleLock.Dispose();
        }
    }

    /// <summary>
    /// Immediate PLC PO-end: print live remainder (sizeCounts / MW56 / DB251) without waiting for Input Slit.
    /// </summary>
    private async Task<(int BundlesClosed, int TotalPcs)> FlushPlcImmediateRemainderAsync(
        string po,
        int millNo,
        int? plcNdtCountFinal,
        Guid? correlationId,
        CancellationToken cancellationToken)
    {
        await _runtimeState.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        string? pipeSize = null;
        try
        {
            pipeSize = await _pipeSizeProvider.TryGetPipeSizeForPoAsync(po, cancellationToken).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(pipeSize))
            {
                var byPo = await _pipeSizeProvider.GetPipeSizeByPoAsync(cancellationToken).ConfigureAwait(false);
                byPo.TryGetValue(po, out pipeSize);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "PO end: pipe size lookup failed for PO {PO} Mill {Mill}.", po, millNo);
        }

        var remainder = PoEndRemainderResolver.Resolve(
            po,
            millNo,
            pipeSize,
            _runtimeState,
            _handshakeStatus,
            plcNdtCountFinal);

        if (remainder <= 0)
        {
            _logger.LogInformation(
                "PO end Immediate: no live remainder for PO {PO} Mill {Mill}; skipping tag print. CorrelationId {CorrelationId}",
                po,
                millNo,
                correlationId);
            return (0, 0);
        }

        _logger.LogInformation(
            "PO end Immediate: printing live remainder {Pcs} for PO {PO} Mill {Mill} (size {Size}). CorrelationId {CorrelationId}",
            remainder,
            po,
            millNo,
            pipeSize ?? "—",
            correlationId);

        var bundlesClosed = 0;
        var totalPcs = 0;

        await _bundleEngine.CloseBundleFromPlcAsync(
            po,
            millNo,
            pipeSize,
            remainder,
            async (contextRecord, batchNo, totalNdtPcs) =>
            {
                if (totalNdtPcs <= 0)
                    return;

                bundlesClosed++;
                totalPcs += totalNdtPcs;
                await _outputWriter.WriteBundleAsync(contextRecord, batchNo, totalNdtPcs, cancellationToken, correlationId)
                    .ConfigureAwait(false);
                await _bundleRepository.TrySetPlcCloseMetadataAsync(batchNo, contextRecord.MillNo, cancellationToken)
                    .ConfigureAwait(false);
                _logger.LogInformation(
                    "PO end bundle closed: PO {PO} Mill {Mill} Batch index {Batch} NdtPcs {Pcs} Close_Source=Plc CorrelationId {CorrelationId}",
                    po,
                    millNo,
                    batchNo,
                    totalNdtPcs,
                    correlationId);
            },
            cancellationToken,
            allowPartial: true).ConfigureAwait(false);

        return (bundlesClosed, totalPcs);
    }

    /// <summary>
    /// Closes open partials and writes/prints them. Used by the PO-end path and by the Plc drain/orphan worker.
    /// Caller must hold the mill bundle lock.
    /// </summary>
    public async Task FlushPartialsAsync(
        string poNumber,
        int millNo,
        Guid? correlationId,
        CancellationToken cancellationToken,
        Action<int>? onClosed = null)
    {
        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        await _bundleEngine.HandlePoEndAsync(
            po,
            millNo,
            async (contextRecord, batchNo, totalNdtPcs) =>
            {
                if (totalNdtPcs <= 0)
                    return;

                onClosed?.Invoke(totalNdtPcs);
                await _outputWriter.WriteBundleAsync(contextRecord, batchNo, totalNdtPcs, cancellationToken, correlationId).ConfigureAwait(false);
                _logger.LogInformation(
                    "PO end bundle closed: PO {PO} Mill {Mill} Batch index {Batch} NdtPcs {Pcs} CorrelationId {CorrelationId}",
                    po,
                    millNo,
                    batchNo,
                    totalNdtPcs,
                    correlationId);
            },
            cancellationToken,
            correlationId).ConfigureAwait(false);
    }

    internal static PoEndFlushMode ResolveFlushMode(MillPoEndSource source, NdtBundleOptions opts)
    {
        // File (and TcpOpen) keep Immediate — Phase 1 scopes AfterDrain to Plc only.
        if (source != MillPoEndSource.Plc)
            return PoEndFlushMode.Immediate;

        return PoEndFlushModeParser.Parse(opts.PoEndFlushMode);
    }
}
