using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.PlcHandshake.S7;
using S7.Net;

namespace NdtBundleService.Services.PlcHandshake;

/// <summary>
/// Detects slit-end and accumulates live PLC NDT into the toward-next-bundle remainder (sizeCounts / MW56).
/// Closes and prints when the accumulated remainder meets the formation-chart threshold.
/// Detection order: optional DB/merker rising edge (<c>L1_ButtEnd</c> / M-bit), else Slit ID change (DB251.DBW10).
/// For Slit-ID mode, accumulates the peak NDT seen on the finished slit (PLC may clear NDT before Slit ID advances).
/// When Immediate PO-end deferred a remainder flush (slit in progress), completes that flush after slit-end accumulate.
/// Slit-ends while waiting for the next running PO (post PO-change) are buffered and flushed when that PO is known.
/// </summary>
public interface IPlcSlitEndBundleCloser
{
    /// <param name="liveNdtCount">Current DB251 NDT INT from this poll.</param>
    /// <param name="liveSlitId">Current DB251 Slit ID INT (DBW10) from this poll.</param>
    Task TryCloseOnSlitEndAsync(
        int millNo,
        MillConfig mill,
        IS7ConnectionProvider s7,
        int liveNdtCount,
        int liveSlitId,
        CancellationToken cancellationToken);

    /// <summary>
    /// Applies NDT captured during the post-PO-end / pre-WIP gap into the new running PO remainder (MW56 source).
    /// </summary>
    /// <returns>Pending pcs flushed (0 if none).</returns>
    Task<int> FlushPendingPoChangeNdtAsync(int millNo, string poNumber, CancellationToken cancellationToken);

    /// <summary>Drops buffered gap NDT (e.g. at PO end before a new wait window).</summary>
    void ClearPendingPoChangeNdt(int millNo);
}

public sealed class PlcSlitEndBundleCloser : IPlcSlitEndBundleCloser
{
    private readonly IOptions<NdtBundleOptions> _options;
    private readonly IBundleEngine _bundleEngine;
    private readonly IBundleOutputWriter _outputWriter;
    private readonly IActivePoPerMillService _activePo;
    private readonly IPipeSizeProvider _pipeSizeProvider;
    private readonly IFormationChartProvider _formationChart;
    private readonly INdtBundleRuntimeStateStore _runtimeState;
    private readonly IMillBundleStateLock _millLock;
    private readonly INdtBundleRepository _bundleRepository;
    private readonly IPoEndSlitEndFlushDeferral _poEndDeferral;
    private readonly IPoEndWorkflowService _poEndWorkflow;
    private readonly ILogger<PlcSlitEndBundleCloser> _logger;

    /// <summary>Peak DB251 NDT seen while on the current Slit ID (survives PLC clearing NDT before Slit ID advances).</summary>
    private readonly Dictionary<int, int> _peakNdtThisSlitByMill = new();
    private readonly Dictionary<int, int> _prevSlitIdByMill = new();
    private readonly HashSet<int> _slitIdPrimedByMill = new();
    private readonly Dictionary<int, bool> _prevSlitEndBitByMill = new();
    private readonly HashSet<int> _closedThisSlitByMill = new();
    /// <summary>NDT from slit-ends while no running PO was known (waiting for WIP after PO change).</summary>
    private readonly Dictionary<int, int> _pendingPoChangeNdtByMill = new();
    private readonly object _pendingLock = new();

    public PlcSlitEndBundleCloser(
        IOptions<NdtBundleOptions> options,
        IBundleEngine bundleEngine,
        IBundleOutputWriter outputWriter,
        IActivePoPerMillService activePo,
        IPipeSizeProvider pipeSizeProvider,
        IFormationChartProvider formationChart,
        INdtBundleRuntimeStateStore runtimeState,
        IMillBundleStateLock millLock,
        INdtBundleRepository bundleRepository,
        IPoEndSlitEndFlushDeferral poEndDeferral,
        IPoEndWorkflowService poEndWorkflow,
        ILogger<PlcSlitEndBundleCloser> logger)
    {
        _options = options;
        _bundleEngine = bundleEngine;
        _outputWriter = outputWriter;
        _activePo = activePo;
        _pipeSizeProvider = pipeSizeProvider;
        _formationChart = formationChart;
        _runtimeState = runtimeState;
        _millLock = millLock;
        _bundleRepository = bundleRepository;
        _poEndDeferral = poEndDeferral;
        _poEndWorkflow = poEndWorkflow;
        _logger = logger;
    }

    public async Task TryCloseOnSlitEndAsync(
        int millNo,
        MillConfig mill,
        IS7ConnectionProvider s7,
        int liveNdtCount,
        int liveSlitId,
        CancellationToken cancellationToken)
    {
        var bundleOpts = _options.Value;
        var handshake = bundleOpts.PlcHandshake ?? new PlcHandshakeOptions();
        var trigger = BundleCloseTriggerParser.Parse(bundleOpts.CloseTrigger);
        if (!BundleClosePolicy.AllowPlcClose(trigger, s7.IsHealthy))
            return;

        var formation = await _formationChart.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);
        var poByMill = await _activePo.GetLatestPoByMillAsync(cancellationToken).ConfigureAwait(false);
        poByMill.TryGetValue(millNo, out var po);
        var poNorm = string.IsNullOrWhiteSpace(po) ? null : InputSlitCsvParsing.NormalizePo(po.Trim());

        string? pipeSize = null;
        if (poNorm is not null)
        {
            try
            {
                pipeSize = await _pipeSizeProvider.TryGetPipeSizeForPoAsync(poNorm, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                /* use default threshold */
            }
        }

        var threshold = FormationChartLookup.ResolveThreshold(formation, pipeSize);
        var sizeKey = FormationChartLookup.NormalizePipeSizeKey(pipeSize);
        if (string.IsNullOrEmpty(sizeKey))
            sizeKey = "Default";

        PoEndSlitEndFlushPending? completeDeferral = null;

        using (await _millLock.AcquireAsync(millNo, cancellationToken).ConfigureAwait(false))
        {
            if (poNorm is null && _poEndDeferral.TryGet(millNo, out var armed))
                poNorm = armed.PoNumber;

            if (poNorm is not null)
            {
                await _runtimeState.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
                await ApplyPendingPoChangeNdtUnlockedAsync(
                        millNo, mill, poNorm, sizeKey, threshold, pipeSize, cancellationToken)
                    .ConfigureAwait(false);
                var leftoverCounts = _runtimeState.GetSizeCounts(poNorm, millNo);
                leftoverCounts.TryGetValue(sizeKey, out var leftover);
                if (leftover >= threshold && leftover > 0)
                {
                    await CloseAccumulatedAsync(
                            millNo,
                            mill,
                            poNorm,
                            pipeSize,
                            leftover,
                            "retry after failed allocate",
                            cancellationToken)
                        .ConfigureAwait(false);
                    _closedThisSlitByMill.Add(millNo);
                    if (_poEndDeferral.TryTake(millNo, out var pendingAfterLeftover))
                        completeDeferral = pendingAfterLeftover;
                    goto AfterLock;
                }
            }

            if (!TryDetectSlitEnd(millNo, handshake, s7, liveNdtCount, liveSlitId, out var edgeReason, out var slitNdtPcs))
                return;

            if (_closedThisSlitByMill.Contains(millNo))
                return;

            if (poNorm is null && _poEndDeferral.TryGet(millNo, out var pendingPo))
                poNorm = pendingPo.PoNumber;

            var slitPcs = Math.Max(0, slitNdtPcs);

            if (poNorm is null)
            {
                // PO-change gap: mill already producing next PO slits, but WIP/running PO not known yet.
                // Buffer peak NDT so it is applied when the new PO is confirmed (do not drop for hooter).
                if (slitPcs > 0)
                {
                    int buffered;
                    lock (_pendingLock)
                    {
                        _pendingPoChangeNdtByMill.TryGetValue(millNo, out var prev);
                        buffered = prev + slitPcs;
                        _pendingPoChangeNdtByMill[millNo] = buffered;
                    }

                    _logger.LogInformation(
                        "{MillName}: slit-end ({Reason}) buffered {SlitPcs} NDT while waiting for next running PO (pending total {Pending}).",
                        mill.Name,
                        edgeReason,
                        slitPcs,
                        buffered);
                }
                else
                {
                    _logger.LogDebug(
                        "{MillName}: slit-end detected ({Reason}) but no active PO and 0 NDT; nothing to buffer.",
                        mill.Name,
                        edgeReason);
                }

                _closedThisSlitByMill.Add(millNo);
                return;
            }

            // Refresh pipe size / threshold if PO came from deferral after lookup.
            if (string.IsNullOrWhiteSpace(pipeSize))
            {
                try
                {
                    pipeSize = await _pipeSizeProvider.TryGetPipeSizeForPoAsync(poNorm, cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                    /* default */
                }

                threshold = FormationChartLookup.ResolveThreshold(formation, pipeSize);
                sizeKey = FormationChartLookup.NormalizePipeSizeKey(pipeSize);
                if (string.IsNullOrEmpty(sizeKey))
                    sizeKey = "Default";
            }

            await _runtimeState.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

            // Apply any gap buffer first (e.g. first new-PO slit finished before WIP arrived).
            await ApplyPendingPoChangeNdtUnlockedAsync(
                    millNo,
                    mill,
                    poNorm,
                    sizeKey,
                    threshold,
                    pipeSize,
                    cancellationToken)
                .ConfigureAwait(false);

            var sizeCounts = _runtimeState.GetSizeCounts(poNorm, millNo);
            sizeCounts.TryGetValue(sizeKey, out var prevAccumulated);
            _runtimeState.IncrementSizeCount(poNorm, millNo, sizeKey, slitPcs);
            await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);
            var accumulated = prevAccumulated + slitPcs;

            if (slitPcs > 0 || accumulated != prevAccumulated)
            {
                _logger.LogInformation(
                    "{MillName}: slit-end ({Reason}) added {SlitPcs} NDT → remainder {Accumulated} (threshold={Threshold}, was {Prev}).",
                    mill.Name,
                    edgeReason,
                    slitPcs,
                    accumulated,
                    threshold,
                    prevAccumulated);
            }

            if (accumulated >= threshold && accumulated > 0)
            {
                await CloseAccumulatedAsync(
                        millNo,
                        mill,
                        poNorm,
                        pipeSize,
                        accumulated,
                        edgeReason,
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            _closedThisSlitByMill.Add(millNo);

            // After this slit ends, complete any Immediate PO-end flush that waited for ButtEnd / slit-end bit.
            if (_poEndDeferral.TryTake(millNo, out var pending))
                completeDeferral = pending;
        }

        AfterLock:
        if (completeDeferral is { } deferred)
        {
            await _poEndWorkflow
                .CompleteSlitEndDeferredFlushAsync(
                    deferred.PoNumber,
                    deferred.MillNo,
                    deferred.PlcNdtAtArm,
                    deferred.CorrelationId,
                    cancellationToken)
                .ConfigureAwait(false);
        }
    }

    public async Task<int> FlushPendingPoChangeNdtAsync(
        int millNo,
        string poNumber,
        CancellationToken cancellationToken)
    {
        var poNorm = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(poNorm) || millNo is < 1 or > 4)
            return 0;

        var mill = (_options.Value.PlcHandshake?.Mills ?? new List<MillConfig>())
                       .FirstOrDefault(m => m.MillNo == millNo)
                   ?? new MillConfig { Name = $"Mill-{millNo}", MillNo = millNo };

        string? pipeSize = null;
        try
        {
            pipeSize = await _pipeSizeProvider.TryGetPipeSizeForPoAsync(poNorm, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            /* default threshold */
        }

        var formation = await _formationChart.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);
        var threshold = FormationChartLookup.ResolveThreshold(formation, pipeSize);
        var sizeKey = FormationChartLookup.NormalizePipeSizeKey(pipeSize);
        if (string.IsNullOrEmpty(sizeKey))
            sizeKey = "Default";

        using (await _millLock.AcquireAsync(millNo, cancellationToken).ConfigureAwait(false))
        {
            await _runtimeState.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
            var flushed = await ApplyPendingPoChangeNdtUnlockedAsync(
                    millNo, mill, poNorm, sizeKey, threshold, pipeSize, cancellationToken)
                .ConfigureAwait(false);
            if (flushed <= 0)
                return 0;

            var counts = _runtimeState.GetSizeCounts(poNorm, millNo);
            counts.TryGetValue(sizeKey, out var accumulated);
            if (accumulated >= threshold && accumulated > 0)
            {
                await CloseAccumulatedAsync(
                        millNo,
                        mill,
                        poNorm,
                        pipeSize,
                        accumulated,
                        "flush pending PO-change slit NDT",
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            return flushed;
        }
    }

    public void ClearPendingPoChangeNdt(int millNo)
    {
        lock (_pendingLock)
        {
            if (_pendingPoChangeNdtByMill.Remove(millNo, out var dropped) && dropped > 0)
            {
                _logger.LogInformation(
                    "Cleared {Pcs} buffered PO-change slit NDT for Mill {Mill} (PO end).",
                    dropped,
                    millNo);
            }
        }
    }

    /// <summary>Caller must hold the mill lock and have initialized runtime state.</summary>
    private async Task<int> ApplyPendingPoChangeNdtUnlockedAsync(
        int millNo,
        MillConfig mill,
        string poNorm,
        string sizeKey,
        int threshold,
        string? pipeSize,
        CancellationToken cancellationToken)
    {
        int pending;
        lock (_pendingLock)
        {
            if (!_pendingPoChangeNdtByMill.TryGetValue(millNo, out pending) || pending <= 0)
                return 0;
            _pendingPoChangeNdtByMill.Remove(millNo);
        }

        var sizeCounts = _runtimeState.GetSizeCounts(poNorm, millNo);
        sizeCounts.TryGetValue(sizeKey, out var prev);
        _runtimeState.IncrementSizeCount(poNorm, millNo, sizeKey, pending);
        await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);
        var accumulated = prev + pending;

        _logger.LogInformation(
            "{MillName}: flushed {Pending} buffered PO-change slit NDT → remainder {Accumulated} for PO {PO} (threshold={Threshold}, was {Prev}).",
            mill.Name,
            pending,
            accumulated,
            poNorm,
            threshold,
            prev);

        return pending;
    }

    private async Task CloseAccumulatedAsync(
        int millNo,
        MillConfig mill,
        string poNorm,
        string? pipeSize,
        int accumulated,
        string edgeReason,
        CancellationToken cancellationToken)
    {
        await _bundleEngine.CloseBundleFromPlcAsync(
            poNorm,
            millNo,
            pipeSize,
            accumulated,
            async (contextRecord, _, totalNdtPcs) =>
            {
                if (totalNdtPcs <= 0)
                    return;

                var seq = await _outputWriter.WriteBundleAsync(contextRecord, 0, totalNdtPcs, cancellationToken)
                    .ConfigureAwait(false);
                await _bundleRepository.TrySetPlcCloseMetadataAsync(seq, contextRecord.MillNo, cancellationToken)
                    .ConfigureAwait(false);

                _logger.LogInformation(
                    "{MillName}: PLC slit-end close printed bundle sequence {BatchNo} ({Total} pcs) — {Reason}.",
                    mill.Name,
                    seq,
                    totalNdtPcs,
                    edgeReason);
            },
            cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Detects slit end. Prefer optional DB/merker rising edge when configured
    /// (e.g. <c>DB270.DBX13.0</c> L1_ButtEnd); otherwise Slit ID (DB251.DBW10) change
    /// means the previous slit finished.
    /// </summary>
    internal bool TryDetectSlitEnd(
        int millNo,
        PlcHandshakeOptions handshake,
        IS7ConnectionProvider s7,
        int liveNdtCount,
        int liveSlitId,
        out string reason,
        out int plcCountForClose)
    {
        reason = "";
        plcCountForClose = liveNdtCount;

        if (handshake.IsSlitEndBitTriggerConfigured)
        {
            var bit = Math.Clamp(handshake.SlitEndTriggerBit, 0, 7);
            var byteOffset = handshake.SlitEndTriggerByte;
            var dbNumber = handshake.SlitEndTriggerDbNumber;
            var active = s7.Read(ops =>
            {
                object? value = dbNumber > 0
                    ? ops.Read(DataType.DataBlock, dbNumber, byteOffset, VarType.Bit, 1, (byte)bit)
                    : ops.Read(DataType.Memory, 0, byteOffset, VarType.Bit, 1, (byte)bit);
                return value is true;
            });

            _prevSlitEndBitByMill.TryGetValue(millNo, out var prev);
            _prevSlitEndBitByMill[millNo] = active;
            if (active && !prev)
            {
                _closedThisSlitByMill.Remove(millNo);
                reason = dbNumber > 0
                    ? $"DB{dbNumber}.DBX{byteOffset}.{bit} rising edge"
                    : $"M{byteOffset}.{bit} rising edge";
                plcCountForClose = liveNdtCount;
                return true;
            }

            if (!active)
                _closedThisSlitByMill.Remove(millNo);
            return false;
        }

        // Default: Mills PLC Slit ID (DB251.DBW10) changed → previous slit finished.
        // Track peak NDT while on that Slit ID — PLC often clears DB251 NDT to 0 before
        // advancing Slit ID; using last-polled NDT would silently accumulate 0 and stall MW56/hooter.
        _prevSlitIdByMill.TryGetValue(millNo, out var prevSlitId);
        _peakNdtThisSlitByMill.TryGetValue(millNo, out var peakNdt);

        if (!_slitIdPrimedByMill.Contains(millNo))
        {
            _prevSlitIdByMill[millNo] = liveSlitId;
            _peakNdtThisSlitByMill[millNo] = Math.Max(0, liveNdtCount);
            _slitIdPrimedByMill.Add(millNo);
            return false;
        }

        if (liveSlitId != prevSlitId)
        {
            _closedThisSlitByMill.Remove(millNo);
            reason = $"Slit ID change ({prevSlitId}→{liveSlitId})";
            plcCountForClose = Math.Max(0, peakNdt);
            _prevSlitIdByMill[millNo] = liveSlitId;
            _peakNdtThisSlitByMill[millNo] = Math.Max(0, liveNdtCount);
            return true;
        }

        if (liveNdtCount > peakNdt)
            _peakNdtThisSlitByMill[millNo] = liveNdtCount;
        return false;
    }
}
