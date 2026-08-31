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
/// Accumulation is cleared in the same SQL transaction as Mill_Sequence allocate + NDT_Bundle insert
/// when mill sequence is enabled; otherwise after the close callback succeeds.
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

    /// <summary>
    /// File-side slit sum for <c>PlcWithFileFallback</c> grace only — never merged into sizeCounts when PLC is healthy.
    /// </summary>
    private readonly Dictionary<string, int> _plcCloseGraceFileSum = new(StringComparer.OrdinalIgnoreCase);

    public NdtBundleEngine(
        IFormationChartProvider formationChartProvider,
        IPipeSizeProvider pipeSizeProvider,
        INdtBundleRuntimeStateStore runtimeState,
        IOptions<NdtBundleOptions> options,
        IS7ConnectionProviderRegistry s7Registry,
        ILogger<NdtBundleEngine> logger,
        TimeProvider? timeProvider = null,
        IMillSequenceService? millSequence = null)
    {
        _formationChartProvider = formationChartProvider;
        _pipeSizeProvider = pipeSizeProvider;
        _runtimeState = runtimeState;
        _options = options;
        _s7Registry = s7Registry;
        _logger = logger;
        _time = timeProvider ?? TimeProvider.System;
        _ = millSequence;
    }

    public async Task ProcessSlitRecordAsync(
        InputSlitRecord record,
        Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
        CancellationToken cancellationToken,
        string? knownPipeSize = null)
    {
        if (record.NdtPipes < 0)
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

        if (record.NdtPipes > 0)
            _runtimeState.SetLastRecord(record.PoNumber, record.MillNo, record);

        var sizeKey = FormationChartLookup.NormalizePipeSizeKey(pipeSize);
        if (string.IsNullOrEmpty(sizeKey))
            sizeKey = "Default";

        var trigger = BundleCloseTriggerParser.Parse(_options.Value.CloseTrigger);
        var plcHealthy = _s7Registry.TryGet(record.MillNo)?.IsHealthy == true;

        // PLC leads open accumulation (sizeCounts/MW56); CSV reconciles only — never adds to the same counters.
        if (PlcOpenCsvIngestPolicy.ShouldIngestTraceabilityOnly(trigger, plcHealthy))
        {
            if (trigger == BundleCloseTrigger.PlcWithFileFallback && record.NdtPipes > 0)
            {
                await TryPlcGraceFileSideCloseAsync(
                        record,
                        sizeKey,
                        sizeThreshold,
                        onBundleClosedAsync,
                        cancellationToken)
                    .ConfigureAwait(false);
            }

            return;
        }

        var allowFileClose = BundleClosePolicy.AllowFileThresholdClose(trigger, plcHealthy);

        if (allowFileClose)
        {
            await TryCommitPendingFileCloseAsync(
                    record,
                    sizeKey,
                    sizeThreshold,
                    onBundleClosedAsync,
                    missedPlcClose: false,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        if (record.NdtPipes <= 0)
            return;

        _runtimeState.IncrementSizeCount(record.PoNumber, record.MillNo, sizeKey, record.NdtPipes);
        await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);
        var sizeCounts = _runtimeState.GetSizeCounts(record.PoNumber, record.MillNo);
        sizeCounts.TryGetValue(sizeKey, out var currentSizeCount);

        var missedPlcClose = false;
        var graceKey = GraceKey(record.PoNumber, record.MillNo, sizeKey);

        if (currentSizeCount < sizeThreshold)
        {
            _plcCloseGraceStartedUtc.Remove(graceKey);
            _plcCloseGraceFileSum.Remove(graceKey);
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

        if (allowFileClose && currentSizeCount >= sizeThreshold && currentSizeCount > 0)
        {
            await TryCommitPendingFileCloseAsync(
                    record,
                    sizeKey,
                    sizeThreshold,
                    onBundleClosedAsync,
                    missedPlcClose,
                    cancellationToken)
                .ConfigureAwait(false);
        }
    }

    public async Task CloseBundleFromPlcAsync(
        string poNumber,
        int millNo,
        string? pipeSize,
        int plcCount,
        Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
        CancellationToken cancellationToken,
        bool allowPartial = false)
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
        if (!allowPartial && plcCount < sizeThreshold)
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

        var contextRecord = _runtimeState.GetLastRecord(poNumber, millNo) ?? CreateSyntheticRecord(poNumber, millNo);

        _logger.LogInformation(
            allowPartial
                ? "Closing PO-end remainder bundle for PO {PO} Mill {Mill} Size {Size} threshold={Threshold} total={Total} Close_Source=Plc (partialAllowed)"
                : "Closing size-based bundle for PO {PO} Mill {Mill} Size {Size} threshold={Threshold} total={Total} Close_Source=Plc",
            poNumber,
            millNo,
            sizeKey,
            sizeThreshold,
            plcCount);

        await CommitCloseAfterWriteAsync(
                contextRecord,
                poNumber,
                millNo,
                plcCount,
                sizeThreshold,
                sizeKey,
                onBundleClosedAsync,
                cancellationToken,
                advanceOnPoEnd: true)
            .ConfigureAwait(false);
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

        foreach (var sizeKey in sizeCounts.Keys.ToList())
        {
            var count = sizeCounts[sizeKey];
            if (count <= 0)
                continue;

            _logger.LogInformation(
                "Closing partial size-based bundle for PO {PO} Mill {Mill} Size {Size} due to PO end. CorrelationId {CorrelationId}",
                poNumber,
                millNo,
                sizeKey,
                correlationId);
            await CommitCloseAfterWriteAsync(
                    contextRecord,
                    poNumber,
                    millNo,
                    count,
                    sizeThreshold,
                    sizeKey,
                    onBundleClosedAsync,
                    cancellationToken)
                .ConfigureAwait(false);
            sizeCounts = _runtimeState.GetSizeCounts(poNumber, millNo);
        }

        await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// File-close leftover: sizeCounts still at threshold after a failed allocate. Retry the same pipes
    /// before adding another slit.
    /// </summary>
    private async Task TryCommitPendingFileCloseAsync(
        InputSlitRecord record,
        string sizeKey,
        int sizeThreshold,
        Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
        bool missedPlcClose,
        CancellationToken cancellationToken)
    {
        var sizeCounts = _runtimeState.GetSizeCounts(record.PoNumber, record.MillNo);
        if (!sizeCounts.TryGetValue(sizeKey, out var totalForBatch) || totalForBatch < sizeThreshold || totalForBatch <= 0)
            return;

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
            "Closing size-based bundle for PO {PO} Mill {Mill} Size {Size} threshold={Threshold} total={Total} (includes slit overshoot)",
            record.PoNumber,
            record.MillNo,
            sizeKey,
            sizeThreshold,
            totalForBatch);

        _plcCloseGraceStartedUtc.Remove(GraceKey(record.PoNumber, record.MillNo, sizeKey));
        await CommitCloseAfterWriteAsync(
                record,
                record.PoNumber,
                record.MillNo,
                totalForBatch,
                sizeThreshold,
                sizeKey,
                onBundleClosedAsync,
                cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Arm accumulation size, then allocate+insert+DELETE in the writer's SQL TX.
    /// On callback failure, accumulation stays. Cache is synced after success (idempotent if SQL already deleted).
    /// </summary>
    private async Task CommitCloseAfterWriteAsync(
        InputSlitRecord contextRecord,
        string poNumber,
        int millNo,
        int totalPcs,
        int sizeThreshold,
        string? sizeKeyToZero,
        Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
        CancellationToken cancellationToken,
        bool advanceOnPoEnd = false)
    {
        _runtimeState.ArmCloseSize(poNumber, millNo, sizeKeyToZero);

        try
        {
            await onBundleClosedAsync(contextRecord, 0, totalPcs).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _runtimeState.ArmCloseSize(poNumber, millNo, null);
            _logger.LogError(
                ex,
                "Bundle close failed for PO {PO} Mill {Mill} ({Pcs} pcs); accumulation left intact.",
                poNumber,
                millNo,
                totalPcs);
            throw;
        }

        if (!string.IsNullOrEmpty(sizeKeyToZero))
        {
            var sizeCounts = _runtimeState.GetSizeCounts(poNumber, millNo);
            sizeCounts.Remove(sizeKeyToZero);
            _runtimeState.SetSizeCounts(poNumber, millNo, sizeCounts);
            ClearPlcCloseGrace(poNumber, millNo, sizeKeyToZero);
        }

        await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);
    }

    private void ClearPlcCloseGrace(string poNumber, int millNo, string sizeKey)
    {
        var key = GraceKey(poNumber, millNo, sizeKey);
        _plcCloseGraceStartedUtc.Remove(key);
        _plcCloseGraceFileSum.Remove(key);
    }

    /// <summary>
    /// PlcWithFileFallback safety-net: track file-side threshold in isolation from PLC sizeCounts.
    /// </summary>
    private async Task TryPlcGraceFileSideCloseAsync(
        InputSlitRecord record,
        string sizeKey,
        int sizeThreshold,
        Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
        CancellationToken cancellationToken)
    {
        var graceKey = GraceKey(record.PoNumber, record.MillNo, sizeKey);
        _plcCloseGraceFileSum.TryGetValue(graceKey, out var fileSum);
        fileSum += record.NdtPipes;
        _plcCloseGraceFileSum[graceKey] = fileSum;

        if (fileSum < sizeThreshold)
        {
            _plcCloseGraceStartedUtc.Remove(graceKey);
            return;
        }

        var now = _time.GetUtcNow();
        if (!_plcCloseGraceStartedUtc.TryGetValue(graceKey, out var started))
        {
            started = now;
            _plcCloseGraceStartedUtc[graceKey] = started;
        }

        var graceSeconds = Math.Max(0, _options.Value.PlcCloseGraceSeconds);
        if ((now - started).TotalSeconds < graceSeconds)
            return;

        _plcCloseGraceStartedUtc.Remove(graceKey);
        _plcCloseGraceFileSum.Remove(graceKey);

        var totalForBatch = fileSum;

        _logger.LogWarning(
            "Missed PLC close for PO {PO} Mill {Mill} Size {Size}: file-side count {Count} ≥ threshold {Threshold} for {GraceSeconds}s with healthy S7; executing file-driven close (PlcCloseGraceSeconds safety-net).",
            record.PoNumber,
            record.MillNo,
            sizeKey,
            totalForBatch,
            sizeThreshold,
            graceSeconds);

        _logger.LogInformation(
            "Closing size-based bundle for PO {PO} Mill {Mill} Size {Size} threshold={Threshold} total={Total} (PlcCloseGrace file-side safety-net)",
            record.PoNumber,
            record.MillNo,
            sizeKey,
            sizeThreshold,
            totalForBatch);

        await CommitCloseAfterWriteAsync(
                record,
                record.PoNumber,
                record.MillNo,
                totalForBatch,
                sizeThreshold,
                sizeKeyToZero: null,
                onBundleClosedAsync,
                cancellationToken)
            .ConfigureAwait(false);
    }

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
