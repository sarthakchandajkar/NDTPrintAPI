using Microsoft.Extensions.Logging;

namespace NdtBundleService.Services;
/// <inheritdoc />
public sealed class PoEndWorkflowService : IPoEndWorkflowService
{
    private readonly IBundleEngine _bundleEngine;
    private readonly IBundleOutputWriter _outputWriter;
    private readonly INdtBatchStateService _batchState;
    private readonly ICurrentPoPlanService? _currentPoPlanService;
    private readonly ILogger<PoEndWorkflowService> _logger;

    public PoEndWorkflowService(
        IBundleEngine bundleEngine,
        IBundleOutputWriter outputWriter,
        INdtBatchStateService batchState,
        ILogger<PoEndWorkflowService> logger,
        ICurrentPoPlanService? currentPoPlanService = null)
    {
        _bundleEngine = bundleEngine;
        _outputWriter = outputWriter;
        _batchState = batchState;
        _currentPoPlanService = currentPoPlanService;
        _logger = logger;
    }

    public async Task ExecuteAsync(string poNumber, int millNo, bool advancePoPlanFile, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(poNumber))
            throw new ArgumentException("PoNumber is required.", nameof(poNumber));

        if (millNo is < 1 or > 4)
            throw new ArgumentOutOfRangeException(nameof(millNo), millNo, "MillNo must be between 1 and 4.");

        var po = InputSlitCsvParsing.NormalizePo(poNumber.Trim());
        _logger.LogInformation("PO end workflow: PO {PO} Mill {Mill} (advance plan file: {Advance})", po, millNo, advancePoPlanFile);

        await _bundleEngine.HandlePoEndAsync(
            po,
            millNo,
            async (contextRecord, batchNo, totalNdtPcs) =>
            {
                await _outputWriter.WriteBundleAsync(contextRecord, batchNo, totalNdtPcs, cancellationToken).ConfigureAwait(false);
                _logger.LogInformation(
                    "PO end bundle closed: PO {PO} Mill {Mill} Batch index {Batch} NdtPcs {Pcs}",
                    po,
                    millNo,
                    batchNo,
                    totalNdtPcs);
            },
            cancellationToken).ConfigureAwait(false);

        await _batchState.IncrementBatchOnPoEndAsync(po, millNo, cancellationToken).ConfigureAwait(false);

        if (advancePoPlanFile && _currentPoPlanService != null)
            await _currentPoPlanService.AdvanceToNextPoAsync(cancellationToken).ConfigureAwait(false);
    }
}
