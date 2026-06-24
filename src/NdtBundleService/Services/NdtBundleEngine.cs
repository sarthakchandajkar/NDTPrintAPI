using Microsoft.Extensions.Logging;
using NdtBundleService.Models;

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
    private readonly ILogger<NdtBundleEngine> _logger;

    public NdtBundleEngine(
        IFormationChartProvider formationChartProvider,
        IPipeSizeProvider pipeSizeProvider,
        INdtBundleRuntimeStateStore runtimeState,
        ILogger<NdtBundleEngine> logger)
    {
        _formationChartProvider = formationChartProvider;
        _pipeSizeProvider = pipeSizeProvider;
        _runtimeState = runtimeState;
        _logger = logger;
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

        if (currentSizeCount >= sizeThreshold && currentSizeCount > 0)
        {
            var totalForBatch = currentSizeCount;
            currentSizeCount = 0;

            var batchNo = _runtimeState.CloseBundle(record.PoNumber, record.MillNo, totalForBatch, sizeThreshold);
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

    public async Task HandlePoEndAsync(
        string poNumber,
        int millNo,
        Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
        CancellationToken cancellationToken)
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
                "Closing partial size-based bundle {BatchNo} for PO {PO} Mill {Mill} Size {Size} due to PO end.",
                batchNo,
                poNumber,
                millNo,
                sizeKey);
            await onBundleClosedAsync(contextRecord, batchNo, count).ConfigureAwait(false);
        }

        if (!closedFromSizeCounts)
        {
            var runningTotal = _runtimeState.GetRunningTotal(poNumber, millNo);
            if (runningTotal > 0)
            {
                var batchNo = _runtimeState.CloseBundle(poNumber, millNo, runningTotal, sizeThreshold);
                _logger.LogInformation(
                    "Closing partial running-total bundle {BatchNo} for PO {PO} Mill {Mill} ({Total} pcs) due to PO end.",
                    batchNo,
                    poNumber,
                    millNo,
                    runningTotal);
                await onBundleClosedAsync(contextRecord, batchNo, runningTotal).ConfigureAwait(false);
            }
        }

        _runtimeState.SetSizeCounts(poNumber, millNo, sizeCounts);
        await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);
    }

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
