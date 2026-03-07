namespace NdtBundleService.Services;

/// <summary>
/// Holds running NDT total and batch offset per (PO, Mill). Batch number increments when
/// total reaches the formation chart threshold for that pipe size, or when PO End is called.
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
            _totalNdtByKey[key] = total;
            totalSoFar = total;

            var sequence = Math.Max(1, ((total - 1) / threshold) + 1);
            batchNumber = sequence + offset;
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
