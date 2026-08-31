namespace NdtBundleService.Services;

/// <summary>
/// Historical PO-end numbering hook. Fill-to-target allocates from Mill_Sequence at close;
/// this service is retained so existing workflow call sites stay no-ops.
/// </summary>
public sealed class NdtBatchStateService : INdtBatchStateService
{
    public NdtBatchStateService(
        IFormationChartProvider formationChartProvider,
        IPipeSizeProvider pipeSizeProvider,
        INdtBundleRuntimeStateStore runtimeState)
    {
        _ = formationChartProvider;
        _ = pipeSizeProvider;
        _ = runtimeState;
    }

    public Task IncrementBatchOnPoEndAsync(string poNumber, int millNo, CancellationToken cancellationToken) =>
        Task.CompletedTask;
}
