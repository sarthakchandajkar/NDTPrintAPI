namespace NdtBundleService.Services;

/// <summary>
/// Holds running NDT total and batch offset per (PO, Mill). When total reaches or exceeds the
/// formation chart threshold, the bundle is closed (all rows in that bundle get the same NDT Batch No.;
/// the tag is printed with the actual total, e.g. 11 pipes; no carry-forward). Then total resets and the next batch starts.
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
        pipeSize ??= string.Empty;

        var formation = await _formationChartProvider.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);
        formation.TryGetValue(pipeSize, out var formationEntry);
        formationEntry ??= formation.TryGetValue("Default", out var defaultEntry) ? defaultEntry : null;
        var threshold = formationEntry?.RequiredNdtPcs ?? 10;
        if (threshold <= 0)
            threshold = 10;

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

            // Current bundle index (1-based): all rows that contribute to this bundle get the same batch number.
            batchNumber = offset + 1;

            // When total reaches or exceeds threshold, close this bundle (tag shows actual total, e.g. 11). Reset and advance for next bundle.
            if (total >= threshold)
            {
                _batchOffsetByKey[key] = offset + 1;
                _totalNdtByKey[key] = 0;
            }
            else
            {
                _totalNdtByKey[key] = total;
            }
        }

        return (batchNumber, totalSoFar, threshold);
    }

    public async Task IncrementBatchOnPoEndAsync(string poNumber, int millNo, CancellationToken cancellationToken)
    {
        var pipeSizeByPo = await _pipeSizeProvider.GetPipeSizeByPoAsync(cancellationToken).ConfigureAwait(false);
        pipeSizeByPo.TryGetValue(poNumber, out var pipeSize);
        pipeSize ??= string.Empty;

        var formation = await _formationChartProvider.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);
        formation.TryGetValue(pipeSize, out var formationEntry);
        formationEntry ??= formation.TryGetValue("Default", out var defaultEntry) ? defaultEntry : null;
        var threshold = formationEntry?.RequiredNdtPcs ?? 10;
        if (threshold <= 0) threshold = 10;

        lock (_lock)
        {
            var key = (poNumber, millNo);
            if (!_totalNdtByKey.TryGetValue(key, out var total))
                total = 0;
            if (!_batchOffsetByKey.TryGetValue(key, out var offset))
                offset = 0;

            var sequence = total == 0 ? 0 : Math.Max(1, ((total - 1) / threshold) + 1);
            _batchOffsetByKey[key] = offset + sequence;
            _totalNdtByKey[key] = 0;
        }
    }
}
