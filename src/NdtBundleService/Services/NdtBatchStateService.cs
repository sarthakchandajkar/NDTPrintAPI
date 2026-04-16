namespace NdtBundleService.Services;

/// <summary>
/// Holds running NDT total and batch offset per (PO, Mill). When total reaches the formation chart
/// threshold, the current bundle closes. If the closing slit overshoots threshold, the overshoot remains
/// in that bundle total (no carry split), matching <see cref="NdtBundleEngine"/>.
/// </summary>
public sealed class NdtBatchStateService : INdtBatchStateService
{
    private readonly IFormationChartProvider _formationChartProvider;
    private readonly IPipeSizeProvider _pipeSizeProvider;
    private readonly object _lock = new();

    private readonly Dictionary<(string Po, int Mill), int> _totalNdtByKey = new();
    private readonly Dictionary<(string Po, int Mill), int> _batchOffsetByKey = new();

    public NdtBatchStateService(
        IFormationChartProvider formationChartProvider,
        IPipeSizeProvider pipeSizeProvider)
    {
        _formationChartProvider = formationChartProvider;
        _pipeSizeProvider = pipeSizeProvider;
    }

    public async Task<(int BatchNumber, int TotalSoFar, int Threshold)> GetBatchForRecordAsync(string poNumber, int millNo, int ndtPipes, CancellationToken cancellationToken)
    {
        // Pipe Size from TM folder (PoPlanFolder or PipeSizeCsvPath) → Formation Chart → pieces per bundle
        var pipeSizeByPo = await _pipeSizeProvider.GetPipeSizeByPoAsync(cancellationToken).ConfigureAwait(false);
        pipeSizeByPo.TryGetValue(poNumber, out var pipeSize);

        var formation = await _formationChartProvider.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);
        var threshold = FormationChartLookup.ResolveThreshold(formation, pipeSize);

        int batchNumber;
        int totalSoFar;
        lock (_lock)
        {
            var key = (poNumber, millNo);
            if (!_totalNdtByKey.TryGetValue(key, out var total))
                total = 0;
            if (!_batchOffsetByKey.TryGetValue(key, out var offset))
                offset = 0;

            total += ndtPipes;
            totalSoFar = total;

            // Current bundle index (1-based): rows share this number until a full bundle (threshold pcs) is completed.
            batchNumber = offset + 1;

            // Close exactly one bundle when threshold is reached; keep overshoot in that bundle.
            if (total >= threshold)
            {
                offset += 1;
                total = 0;
            }

            _totalNdtByKey[key] = total;
            _batchOffsetByKey[key] = offset;
        }

        return (batchNumber, totalSoFar, threshold);
    }

    public async Task IncrementBatchOnPoEndAsync(string poNumber, int millNo, CancellationToken cancellationToken)
    {
        var pipeSizeByPo = await _pipeSizeProvider.GetPipeSizeByPoAsync(cancellationToken).ConfigureAwait(false);
        pipeSizeByPo.TryGetValue(poNumber, out var pipeSize);

        var formation = await _formationChartProvider.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);
        var threshold = FormationChartLookup.ResolveThreshold(formation, pipeSize);

        lock (_lock)
        {
            var key = (poNumber, millNo);
            if (!_totalNdtByKey.TryGetValue(key, out var total))
                total = 0;
            if (!_batchOffsetByKey.TryGetValue(key, out var offset))
                offset = 0;

            // Count how many bundle slots (including a partial remainder) this PO/mill had open.
            var sequence = total == 0 ? 0 : Math.Max(1, ((total - 1) / threshold) + 1);
            // PO end with no slit rows processed yet (offset 0, total 0): still advance so the next Input Slit file gets the next NDT Batch No sequence.
            if (sequence == 0 && total == 0 && offset == 0)
                sequence = 1;

            _batchOffsetByKey[key] = offset + sequence;
            _totalNdtByKey[key] = 0;
        }
    }
}
