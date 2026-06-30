using Microsoft.Extensions.Logging;
using Serilog.Context;

namespace NdtBundleService.Services;
/// <inheritdoc />
public sealed class PoEndWorkflowService : IPoEndWorkflowService
{
    private readonly IBundleEngine _bundleEngine;
    private readonly IBundleOutputWriter _outputWriter;
    private readonly INdtBatchStateService _batchState;
    private readonly INdtBundleRuntimeStateStore _runtimeState;
    private readonly ICurrentPoPlanService? _currentPoPlanService;
    private readonly IMillSlitLiveNdtAccumulator _liveNdtAccumulator;
    private readonly IWipBundleRunningPoProvider _wipRunningPo;
    private readonly IMillBundleStateLock _millBundleStateLock;
    private readonly ILogger<PoEndWorkflowService> _logger;

    public PoEndWorkflowService(
        IBundleEngine bundleEngine,
        IBundleOutputWriter outputWriter,
        INdtBatchStateService batchState,
        INdtBundleRuntimeStateStore runtimeState,
        IMillSlitLiveNdtAccumulator liveNdtAccumulator,
        IWipBundleRunningPoProvider wipRunningPo,
        IMillBundleStateLock millBundleStateLock,
        ILogger<PoEndWorkflowService> logger,
        ICurrentPoPlanService? currentPoPlanService = null)
    {
        _bundleEngine = bundleEngine;
        _outputWriter = outputWriter;
        _batchState = batchState;
        _runtimeState = runtimeState;
        _liveNdtAccumulator = liveNdtAccumulator;
        _wipRunningPo = wipRunningPo;
        _millBundleStateLock = millBundleStateLock;
        _logger = logger;
        _currentPoPlanService = currentPoPlanService;
    }

    public async Task<PoEndWorkflowResult> ExecuteAsync(string poNumber, int millNo, bool advancePoPlanFile, CancellationToken cancellationToken, Guid? correlationId = null)
    {
        using (correlationId is { } id ? LogContext.PushProperty("CorrelationId", id) : null)
        {
            return await ExecuteCoreAsync(poNumber, millNo, advancePoPlanFile, cancellationToken, correlationId).ConfigureAwait(false);
        }
    }

    private async Task<PoEndWorkflowResult> ExecuteCoreAsync(string poNumber, int millNo, bool advancePoPlanFile, CancellationToken cancellationToken, Guid? correlationId)
    {
        if (string.IsNullOrWhiteSpace(poNumber))
            throw new ArgumentException("PoNumber is required.", nameof(poNumber));

        if (millNo is < 1 or > 4)
            throw new ArgumentOutOfRangeException(nameof(millNo), millNo, "MillNo must be between 1 and 4.");

        var po = InputSlitCsvParsing.NormalizePo(poNumber.Trim());
        _logger.LogInformation(
            "PO end workflow: PO {PO} Mill {Mill} (advance plan file: {Advance}) CorrelationId {CorrelationId}",
            po,
            millNo,
            advancePoPlanFile,
            correlationId);

        var bundleLock = await _millBundleStateLock.AcquireAsync(millNo, cancellationToken).ConfigureAwait(false);
        try
        {
            var bundlesClosed = 0;
            var totalNdtPcsClosed = 0;

            await _bundleEngine.HandlePoEndAsync(
                po,
                millNo,
                async (contextRecord, batchNo, totalNdtPcs) =>
                {
                    if (totalNdtPcs <= 0)
                        return;

                    bundlesClosed++;
                    totalNdtPcsClosed += totalNdtPcs;
                    await _outputWriter.WriteBundleAsync(contextRecord, batchNo, totalNdtPcs, cancellationToken, correlationId).ConfigureAwait(false);
                    _logger.LogInformation(
                        "PO end bundle closed: PO {PO} Mill {Mill} Batch index {Batch} NdtPcs {Pcs} CorrelationId {CorrelationId}",
                        po,
                        millNo,
                        batchNo,
                        totalNdtPcs,
                        correlationId);
                },
                cancellationToken,
                correlationId).ConfigureAwait(false);

            await _batchState.IncrementBatchOnPoEndAsync(po, millNo, cancellationToken).ConfigureAwait(false);
            await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);

            _liveNdtAccumulator.OnPoEndForMill(po, millNo);
            _wipRunningPo.NotifyPoEndForMill(millNo, po);

            if (advancePoPlanFile && _currentPoPlanService != null)
                await _currentPoPlanService.AdvanceToNextPoAsync(cancellationToken).ConfigureAwait(false);

            return new PoEndWorkflowResult
            {
                PoNumber = po,
                MillNo = millNo,
                BundlesClosed = bundlesClosed,
                TotalNdtPcsClosed = totalNdtPcsClosed,
                WaitingForNewWip = _wipRunningPo.IsWaitingForNewWipAfterPoEnd(millNo),
                AdvancedPoPlanFile = advancePoPlanFile && _currentPoPlanService != null
            };
        }
        finally
        {
            bundleLock.Dispose();
        }
    }
}
