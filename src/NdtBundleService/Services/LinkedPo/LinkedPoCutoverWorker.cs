using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.PoLifecycle;
using Serilog.Context;

namespace NdtBundleService.Services.LinkedPo;

/// <summary>
/// Runs PO-end for the prior PO when linked-PO evidence is present (no PLC M40.6 trigger),
/// then accepts the new running PO from the WIP file — same shape as file-based PO change.
/// </summary>
public sealed class LinkedPoCutoverWorker : BackgroundService
{
    private readonly LinkedPoCutoverQueue _queue;
    private readonly LinkedPoCutoverEvidence _evidence;
    private readonly IPoEndWorkflowService _poEndWorkflow;
    private readonly IWipBundleRunningPoProvider _wipRunningPo;
    private readonly IPoLifecycleService _poLifecycle;
    private readonly IMillOwnership _millOwnership;
    private readonly IOptions<NdtBundleOptions> _options;
    private readonly ILogger<LinkedPoCutoverWorker> _logger;

    public LinkedPoCutoverWorker(
        LinkedPoCutoverQueue queue,
        LinkedPoCutoverEvidence evidence,
        IPoEndWorkflowService poEndWorkflow,
        IWipBundleRunningPoProvider wipRunningPo,
        IPoLifecycleService poLifecycle,
        IMillOwnership millOwnership,
        IOptions<NdtBundleOptions> options,
        ILogger<LinkedPoCutoverWorker> logger)
    {
        _queue = queue;
        _evidence = evidence;
        _poEndWorkflow = poEndWorkflow;
        _wipRunningPo = wipRunningPo;
        _poLifecycle = poLifecycle;
        _millOwnership = millOwnership;
        _options = options;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.Value.EnableLinkedPoCutover)
        {
            _logger.LogInformation("LinkedPoCutoverWorker disabled (EnableLinkedPoCutover=false).");
            return;
        }

        _logger.LogInformation("LinkedPoCutoverWorker started.");

        await foreach (var request in _queue.Reader.ReadAllAsync(stoppingToken).ConfigureAwait(false))
        {
            using (LogContext.PushProperty("CorrelationId", request.CorrelationId))
            using (LogContext.PushProperty("Mill", request.MillNo))
            {
                try
                {
                    await ProcessAsync(request, stoppingToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogError(
                        ex,
                        "Linked-PO cutover failed — Mill {Mill}, {OldPo} → {NewPo}, Trigger {Trigger}, CorrelationId {CorrelationId}.",
                        request.MillNo,
                        request.EndedPo,
                        request.NewPo,
                        request.Trigger,
                        request.CorrelationId);
                }
                finally
                {
                    _queue.MarkCompleted(request.MillNo);
                }
            }
        }
    }

    internal async Task ProcessAsync(LinkedPoCutoverRequest request, CancellationToken cancellationToken)
    {
        if (!_options.Value.EnableLinkedPoCutover)
            return;

        if (!_millOwnership.Owns(request.MillNo))
        {
            _logger.LogDebug(
                "Mill {Mill}: linked-PO cutover skipped — mill not owned.",
                request.MillNo);
            return;
        }

        var source = MillPoEndSourceResolver.ForMill(request.MillNo, _options.Value);
        if (source is not (MillPoEndSource.Plc or MillPoEndSource.TcpOpen))
        {
            _logger.LogDebug(
                "Mill {Mill}: linked-PO cutover skipped — PoEndSource={Source} (expected Plc/TcpOpen).",
                request.MillNo,
                MillPoEndSourceResolver.ToConfigValue(source));
            return;
        }

        var endedPo = InputSlitCsvParsing.NormalizePo(request.EndedPo);
        var newPo = InputSlitCsvParsing.NormalizePo(request.NewPo);
        if (string.IsNullOrWhiteSpace(endedPo) || string.IsNullOrWhiteSpace(newPo))
            return;
        if (InputSlitCsvParsing.PoEquals(endedPo, newPo))
            return;

        if (_poLifecycle.GetPhase(request.MillNo, endedPo) == PoLifecyclePhase.Closed)
        {
            _logger.LogInformation(
                "Mill {Mill}: linked-PO cutover {OldPo} → {NewPo} skipped — ended PO already Closed. Trigger {Trigger}.",
                request.MillNo,
                endedPo,
                newPo,
                request.Trigger);
            TryAcceptNewRunningPo(request, newPo);
            return;
        }

        var evidence = await _evidence
            .TryFindAsync(request.MillNo, endedPo, newPo, request.SlitNoHint, cancellationToken)
            .ConfigureAwait(false);
        if (evidence is null)
        {
            _logger.LogInformation(
                "Mill {Mill}: WIP/slit PO change {OldPo} → {NewPo} has no linked-PO evidence yet — awaiting PLC PO end or second-half slit. Trigger {Trigger}.",
                request.MillNo,
                endedPo,
                newPo,
                request.Trigger);
            return;
        }

        var advancePlan = (_options.Value.PlcHandshake ?? new PlcHandshakeOptions()).AdvancePoPlanFileOnPoEnd;

        _logger.LogInformation(
            "Mill {Mill}: linked-PO cutover {OldPo} → {NewPo} slit {Slit} ({Detail}); running PO end without PLC trigger. Trigger {Trigger}. CorrelationId {CorrelationId}",
            request.MillNo,
            endedPo,
            newPo,
            evidence.BaseSlitNo,
            evidence.Detail,
            request.Trigger,
            request.CorrelationId);

        await _poEndWorkflow
            .ExecuteAsync(endedPo, request.MillNo, advancePlan, cancellationToken, request.CorrelationId)
            .ConfigureAwait(false);

        TryAcceptNewRunningPo(request, newPo);

        _logger.LogInformation(
            "Mill {Mill}: linked-PO cutover completed {OldPo} → {NewPo}. CorrelationId {CorrelationId}",
            request.MillNo,
            endedPo,
            newPo,
            request.CorrelationId);
    }

    private void TryAcceptNewRunningPo(LinkedPoCutoverRequest request, string newPo)
    {
        if (!string.IsNullOrWhiteSpace(request.WipFileName) && request.WipStampUtc != default)
        {
            if (_wipRunningPo.TrySetRunningPoFromWipFile(
                    request.MillNo,
                    newPo,
                    request.WipStampUtc,
                    request.WipFileName))
            {
                _logger.LogInformation(
                    "Mill {Mill}: accepted new running PO {Po} from WIP file {File} after linked-PO cutover.",
                    request.MillNo,
                    newPo,
                    request.WipFileName);
            }

            return;
        }

        // InputSlit-triggered cutover: PoEnd armed WIP-wait; re-seed from latest WIP (should be newPo).
        if (_wipRunningPo.IsWaitingForNewWipAfterPoEnd(request.MillNo) &&
            _wipRunningPo.ResumeRunningWipForMill(request.MillNo))
        {
            _logger.LogInformation(
                "Mill {Mill}: re-seeded running PO after linked-PO cutover (trigger {Trigger}, expected {Po}).",
                request.MillNo,
                request.Trigger,
                newPo);
        }
    }
}
