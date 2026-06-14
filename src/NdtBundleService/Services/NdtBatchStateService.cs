namespace NdtBundleService.Services;

/// <summary>
/// Running NDT total and bundle sequence per (PO, Mill), backed by <see cref="INdtBundleRuntimeStateStore"/>.
/// </summary>
public sealed class NdtBatchStateService : INdtBatchStateService
{
    private readonly IFormationChartProvider _formationChartProvider;
    private readonly IPipeSizeProvider _pipeSizeProvider;
    private readonly INdtBundleRuntimeStateStore _runtimeState;

    public NdtBatchStateService(
        IFormationChartProvider formationChartProvider,
        IPipeSizeProvider pipeSizeProvider,
        INdtBundleRuntimeStateStore runtimeState)
    {
        _formationChartProvider = formationChartProvider;
        _pipeSizeProvider = pipeSizeProvider;
        _runtimeState = runtimeState;
    }

    public Task<(int BatchNumber, int TotalSoFar, int Threshold)> GetBatchForRecordAsync(
        string poNumber,
        int millNo,
        int ndtPipes,
        CancellationToken cancellationToken,
        string? knownPipeSize = null) =>
        GetBatchForRecordCoreAsync(poNumber, millNo, ndtPipes, cancellationToken, knownPipeSize);

    private async Task<(int BatchNumber, int TotalSoFar, int Threshold)> GetBatchForRecordCoreAsync(
        string poNumber,
        int millNo,
        int ndtPipes,
        CancellationToken cancellationToken,
        string? knownPipeSize)
    {
        await _runtimeState.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var threshold = await ResolveThresholdAsync(poNumber, knownPipeSize, cancellationToken).ConfigureAwait(false);
        _runtimeState.ApplySlitContribution(poNumber, millNo, ndtPipes, threshold, out var batchNumber, out var totalSoFar);
        await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);
        return (batchNumber, totalSoFar, threshold);
    }

    public async Task IncrementBatchOnPoEndAsync(string poNumber, int millNo, CancellationToken cancellationToken)
    {
        await _runtimeState.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var threshold = await ResolveThresholdAsync(poNumber, knownPipeSize: null, cancellationToken).ConfigureAwait(false);
        _runtimeState.AdvanceOnPoEnd(poNumber, millNo, threshold);
        await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<int> ResolveThresholdAsync(
        string poNumber,
        string? knownPipeSize,
        CancellationToken cancellationToken)
    {
        string? pipeSize = knownPipeSize;
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
        return FormationChartLookup.ResolveThreshold(formation, pipeSize);
    }
}
