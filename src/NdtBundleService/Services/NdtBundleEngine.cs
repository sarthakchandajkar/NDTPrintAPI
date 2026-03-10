using Microsoft.Extensions.Logging;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Aggregates NDT pipe counts per PO/mill/size and decides when bundles are complete.
/// This is a simplified first implementation based on NDTPcsPerBundle and size thresholds.
/// </summary>
public sealed class NdtBundleEngine : IBundleEngine
{
    private readonly IFormationChartProvider _formationChartProvider;
    private readonly IPipeSizeProvider _pipeSizeProvider;
    private readonly ILogger<NdtBundleEngine> _logger;

    // State per (PO, Mill)
    private readonly Dictionary<(string Po, int Mill), BundleState> _bundleStates = new();

    public NdtBundleEngine(IFormationChartProvider formationChartProvider, IPipeSizeProvider pipeSizeProvider, ILogger<NdtBundleEngine> logger)
    {
        _formationChartProvider = formationChartProvider;
        _pipeSizeProvider = pipeSizeProvider;
        _logger = logger;
    }

    public async Task ProcessSlitRecordAsync(
        InputSlitRecord record,
        Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
        CancellationToken cancellationToken)
    {
        if (record.NdtPipes <= 0)
            return;

        var key = (record.PoNumber, record.MillNo);
        var state = GetOrCreateState(key);

        // Pipe size from external file (by PO Number), not from Input Slit CSV
        var pipeSizeByPo = await _pipeSizeProvider.GetPipeSizeByPoAsync(cancellationToken).ConfigureAwait(false);
        pipeSizeByPo.TryGetValue(record.PoNumber, out var pipeSize);
        pipeSize ??= string.Empty;

        var formation = await _formationChartProvider.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);
        formation.TryGetValue(pipeSize, out var formationEntry);
        formationEntry ??= formation.TryGetValue("Default", out var defaultEntry) ? defaultEntry : null;
        var sizeThreshold = formationEntry?.RequiredNdtPcs ?? 0;
        if (sizeThreshold <= 0)
            sizeThreshold = 10;

        // Keep last record as context for bundle export
        state.LastRecord = record;

        // Use a consistent key for lookup so we always track count (enables PO End to close partial bundles)
        var sizeKey = string.IsNullOrWhiteSpace(pipeSize) ? "Default" : pipeSize;

        if (!state.SizeCounts.TryGetValue(sizeKey, out var currentSizeCount))
            currentSizeCount = 0;
        currentSizeCount += record.NdtPipes;
        state.SizeCounts[sizeKey] = currentSizeCount;

        // Size-based scenario: if count for this size reaches Formation Chart threshold, close a bundle
        if (state.SizeCounts.TryGetValue(sizeKey, out var sizeCount) && sizeCount >= sizeThreshold)
        {
            state.CurrentBatchNo++;
            var totalForBatch = sizeCount;
            state.SizeCounts[sizeKey] = 0;
            _logger.LogInformation("Closing size-based bundle {BatchNo} for PO {PO} Mill {Mill} Size {Size} threshold={Threshold}", state.CurrentBatchNo, record.PoNumber, record.MillNo, sizeKey, sizeThreshold);
            await onBundleClosedAsync(state.LastRecord!, state.CurrentBatchNo, totalForBatch).ConfigureAwait(false);
        }
    }

    public async Task HandlePoEndAsync(
        string poNumber,
        int millNo,
        Func<InputSlitRecord, int, int, Task> onBundleClosedAsync,
        CancellationToken cancellationToken)
    {
        var key = (poNumber, millNo);
        if (!_bundleStates.TryGetValue(key, out var state))
            return;

        var contextRecord = state.LastRecord ?? CreateSyntheticRecord(poNumber, millNo);

        // Reset size-based counts on PO end (close any partial size-based bundles)
        foreach (var sizeKey in state.SizeCounts.Keys.ToList())
        {
            var count = state.SizeCounts[sizeKey];
            if (count <= 0)
                continue;

            state.CurrentBatchNo++;
            var totalForBatch = count;
            state.SizeCounts[sizeKey] = 0;
            _logger.LogInformation("Closing partial size-based bundle {BatchNo} for PO {PO} Mill {Mill} Size {Size} due to PO end.", state.CurrentBatchNo, poNumber, millNo, sizeKey);
            await onBundleClosedAsync(contextRecord, state.CurrentBatchNo, totalForBatch).ConfigureAwait(false);
        }
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

    private BundleState GetOrCreateState((string Po, int Mill) key)
    {
        if (_bundleStates.TryGetValue(key, out var state))
            return state;

        state = new BundleState();
        _bundleStates[key] = state;
        return state;
    }

    private sealed class BundleState
    {
        public int CurrentBatchNo { get; set; }
        public Dictionary<string, int> SizeCounts { get; } = new(StringComparer.OrdinalIgnoreCase);
        public InputSlitRecord? LastRecord { get; set; }
    }
}

