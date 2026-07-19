using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services.PlcHandshake.S7;
using Serilog.Context;

namespace NdtBundleService.Services;

/// <summary>
/// Aggregates NDT pipe counts per PO/mill/size and decides when bundles are complete.
/// Bundle sequence and partial totals are persisted via <see cref="INdtBundleRuntimeStateStore"/>.
/// </summary>
public sealed class NdtBundleEngine : IBundleEngine
{
    private readonly IFormationChartProvider _formationChartProvider;
    private readonly IPipeSizeProvider _pipeSizeProvider;
    private readonly INdtBundleRuntimeStateStore _runtimeState;
    private readonly IOptions<NdtBundleOptions> _options;
    private readonly IS7ConnectionProviderRegistry _s7Registry;
    private readonly ILogger<NdtBundleEngine> _logger;
    private readonly TimeProvider _time;

    /// <summary>UTC when file-side count first reached threshold while PLC path owned closes (grace clock).</summary>
    private readonly Dictionary<string, DateTimeOffset> _plcCloseGraceStartedUtc = new(StringComparer.OrdinalIgnoreCase);

    public NdtBundleEngine(
        IFormationChartProvider formationChartProvider,
        IPipeSizeProvider pipeSizeProvider,
        INdtBundleRuntimeStateStore runtimeState,
        IOptions<NdtBundleOptions> options,
        IS7ConnectionProviderRegistry s7Registry,
        ILogger<NdtBundleEngine> logger,
        TimeProvider? timeProvider = null)
    {
        _formationChartProvider = formationChartProvider;
        _pipeSizeProvider = pipeSizeProvider;
        _runtimeState = runtimeState;
        _options = options;
        _s7Registry = s7Registry;
        _logger = logger;
        _time = timeProvider ?? TimeProvider.System;
    }

    public async Task ProcessSlitRecordAsync(
        InputSlitRecord record,
        Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
        CancellationToken cancellationToken,
        string? knownPipeSize = null)
    {
        if (record.NdtPipes <= 0)
            return;

        await _runtimeState.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        string? pipeSize = knownPipeSize;
        if (string.IsNullOrWhiteSpace(pipeSize))
        {
            pipeSize = await _pipeSizeProvider.TryGetPipeSizeForPoAsync(record.PoNumber, cancellationToken).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(pipeSize))
            {
                var pipeSizeByPo = await _pipeSizeProvider.GetPipeSizeByPoAsync(cancellationToken).ConfigureAwait(false);
                pipeSizeByPo.TryGetValue(record.PoNumber, out pipeSize);
            }
        }

        var formation = await _formationChartProvider.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);
        var sizeThreshold = FormationChartLookup.ResolveThreshold(formation, pipeSize);

        _runtimeState.SetLastRecord(record.PoNumber, record.MillNo, record);

        var sizeKey = FormationChartLookup.NormalizePipeSizeKey(pipeSize);
        if (string.IsNullOrEmpty(sizeKey))
            sizeKey = "Default";

        var sizeCounts = _runtimeState.GetSizeCounts(record.PoNumber, record.MillNo);
        if (!sizeCounts.TryGetValue(sizeKey, out var currentSizeCount))
            currentSizeCount = 0;
        currentSizeCount += record.NdtPipes;

        var trigger = BundleCloseTriggerParser.Parse(_options.Value.CloseTrigger);
        var plcHealthy = _s7Registry.TryGet(record.MillNo)?.IsHealthy == true;
        var allowFileClose = BundleClosePolicy.AllowFileThresholdClose(trigger, plcHealthy);
        var graceKey = GraceKey(record.PoNumber, record.MillNo, sizeKey);
        var missedPlcClose = false;

        if (currentSizeCount < sizeThreshold)
        {
            _plcCloseGraceStartedUtc.Remove(graceKey);
        }
        else if (!allowFileClose
                 && trigger == BundleCloseTrigger.PlcWithFileFallback
                 && plcHealthy)
        {
            var now = _time.GetUtcNow();
            if (!_plcCloseGraceStartedUtc.TryGetValue(graceKey, out var started))
            {
                started = now;
                _plcCloseGraceStartedUtc[graceKey] = started;
            }

            var graceSeconds = Math.Max(0, _options.Value.PlcCloseGraceSeconds);
            if ((now - started).TotalSeconds >= graceSeconds)
            {
                allowFileClose = true;
                missedPlcClose = true;
            }
        }

        // File-driven close is gated by CloseTrigger; when PLC path owns closes, still accumulate for hooter/recon.
        if (allowFileClose && currentSizeCount >= sizeThreshold && currentSizeCount > 0)
        {
            var totalForBatch = currentSizeCount;
            currentSizeCount = 0;
            _plcCloseGraceStartedUtc.Remove(graceKey);

            var batchNo = _runtimeState.CloseBundle(record.PoNumber, record.MillNo, totalForBatch, sizeThreshold);
            if (missedPlcClose)
            {
                _logger.LogWarning(
                    "Missed PLC close for PO {PO} Mill {Mill} Size {Size}: file-side count {Count} ≥ threshold {Threshold} for {GraceSeconds}s with healthy S7; executing file-driven close (PlcCloseGraceSeconds safety-net).",
                    record.PoNumber,
                    record.MillNo,
                    sizeKey,
                    totalForBatch,
                    sizeThreshold,
                    Math.Max(0, _options.Value.PlcCloseGraceSeconds));
            }

            _logger.LogInformation(
                "Closing size-based bundle {BatchNo} for PO {PO} Mill {Mill} Size {Size} threshold={Threshold} total={Total} (includes slit overshoot)",
                batchNo,
                record.PoNumber,
                record.MillNo,
                sizeKey,
                sizeThreshold,
                totalForBatch);

            sizeCounts[sizeKey] = currentSizeCount;
            _runtimeState.SetSizeCounts(record.PoNumber, record.MillNo, sizeCounts);
            await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);

            await onBundleClosedAsync(record, batchNo, totalForBatch).ConfigureAwait(false);
            return;
        }

        sizeCounts[sizeKey] = currentSizeCount;
        _runtimeState.SetSizeCounts(record.PoNumber, record.MillNo, sizeCounts);
        await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task CloseBundleFromPlcAsync(
        string poNumber,
        int millNo,
        string? pipeSize,
        int plcCount,
        Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
        CancellationToken cancellationToken)
    {
        if (plcCount <= 0)
            return;

        await _runtimeState.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        if (string.IsNullOrWhiteSpace(pipeSize))
        {
            pipeSize = await _pipeSizeProvider.TryGetPipeSizeForPoAsync(poNumber, cancellationToken).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(pipeSize))
            {
                var pipeSizeByPo = await _pipeSizeProvider.GetPipeSizeByPoAsync(cancellationToken).ConfigureAwait(false);
                pipeSizeByPo.TryGetValue(poNumber, out pipeSize);
            }
        }

        var formation = await _formationChartProvider.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);
        var sizeThreshold = FormationChartLookup.ResolveThreshold(formation, pipeSize);
        if (plcCount < sizeThreshold)
        {
            _logger.LogDebug(
                "PLC close skipped for PO {PO} Mill {Mill}: plcCount={PlcCount} < threshold={Threshold}.",
                poNumber,
                millNo,
                plcCount,
                sizeThreshold);
            return;
        }

        var sizeKey = FormationChartLookup.NormalizePipeSizeKey(pipeSize);
        if (string.IsNullOrEmpty(sizeKey))
            sizeKey = "Default";

        var sizeCounts = _runtimeState.GetSizeCounts(poNumber, millNo);
        sizeCounts[sizeKey] = 0;
        _runtimeState.SetSizeCounts(poNumber, millNo, sizeCounts);
        ClearPlcCloseGrace(poNumber, millNo, sizeKey);

        var contextRecord = _runtimeState.GetLastRecord(poNumber, millNo) ?? CreateSyntheticRecord(poNumber, millNo);
        var batchNo = _runtimeState.CloseBundle(poNumber, millNo, plcCount, sizeThreshold);
        // Align RunningTotal/BatchOffset so late CSV rows can attach without burning a new sequence
        // until ApplySlitContribution rolls past threshold (recon override uses the closed batch).
        _runtimeState.AdvanceOnPoEnd(poNumber, millNo, sizeThreshold);
        await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "Closing size-based bundle {BatchNo} for PO {PO} Mill {Mill} Size {Size} threshold={Threshold} total={Total} Close_Source=Plc",
            batchNo,
            poNumber,
            millNo,
            sizeKey,
            sizeThreshold,
            plcCount);

        await onBundleClosedAsync(contextRecord, batchNo, plcCount).ConfigureAwait(false);
    }

    public async Task HandlePoEndAsync(
        string poNumber,
        int millNo,
        Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
        CancellationToken cancellationToken,
        Guid? correlationId = null)
    {
        using (correlationId is { } id ? LogContext.PushProperty("CorrelationId", id) : null)
        {
            await HandlePoEndCoreAsync(poNumber, millNo, onBundleClosedAsync, cancellationToken, correlationId).ConfigureAwait(false);
        }
    }

    private async Task HandlePoEndCoreAsync(
        string poNumber,
        int millNo,
        Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
        CancellationToken cancellationToken,
        Guid? correlationId)
    {
        await _runtimeState.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var pipeSizeByPo = await _pipeSizeProvider.GetPipeSizeByPoAsync(cancellationToken).ConfigureAwait(false);
        pipeSizeByPo.TryGetValue(poNumber, out var pipeSize);

        var formation = await _formationChartProvider.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);
        var sizeThreshold = FormationChartLookup.ResolveThreshold(formation, pipeSize);

        var contextRecord = _runtimeState.GetLastRecord(poNumber, millNo) ?? CreateSyntheticRecord(poNumber, millNo);
        var sizeCounts = _runtimeState.GetSizeCounts(poNumber, millNo);
        var closedFromSizeCounts = false;

        foreach (var sizeKey in sizeCounts.Keys.ToList())
        {
            var count = sizeCounts[sizeKey];
            if (count <= 0)
                continue;

            closedFromSizeCounts = true;
            var batchNo = _runtimeState.CloseBundle(poNumber, millNo, count, sizeThreshold);
            sizeCounts[sizeKey] = 0;
            _logger.LogInformation(
                "Closing partial size-based bundle {BatchNo} for PO {PO} Mill {Mill} Size {Size} due to PO end. CorrelationId {CorrelationId}",
                batchNo,
                poNumber,
                millNo,
                sizeKey,
                correlationId);
            await onBundleClosedAsync(contextRecord, batchNo, count).ConfigureAwait(false);
        }

        if (!closedFromSizeCounts)
        {
            var runningTotal = _runtimeState.GetRunningTotal(poNumber, millNo);
            if (runningTotal > 0)
            {
                var batchNo = _runtimeState.CloseBundle(poNumber, millNo, runningTotal, sizeThreshold);
                _logger.LogInformation(
                    "Closing partial running-total bundle {BatchNo} for PO {PO} Mill {Mill} ({Total} pcs) due to PO end. CorrelationId {CorrelationId}",
                    batchNo,
                    poNumber,
                    millNo,
                    runningTotal,
                    correlationId);
                await onBundleClosedAsync(contextRecord, batchNo, runningTotal).ConfigureAwait(false);
            }
        }

        _runtimeState.SetSizeCounts(poNumber, millNo, sizeCounts);
        _runtimeState.ClearRunningTotal(poNumber, millNo);
        await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);
    }

    private void ClearPlcCloseGrace(string poNumber, int millNo, string sizeKey) =>
        _plcCloseGraceStartedUtc.Remove(GraceKey(poNumber, millNo, sizeKey));

    private static string GraceKey(string poNumber, int millNo, string sizeKey) =>
        $"{InputSlitCsvParsing.NormalizePo(poNumber)}|{millNo}|{sizeKey}";

    private static InputSlitRecord CreateSyntheticRecord(string poNumber, int millNo)
    {
        return new InputSlitRecord
        {
            PoNumber = poNumber,
            MillNo = millNo,
            SlitNo = "",
            NdtPipes = 0,
            RejectedPipes = 0,
            NdtShortLengthPipe = "",
            RejectedShortLengthPipe = ""
        };
    }
}
