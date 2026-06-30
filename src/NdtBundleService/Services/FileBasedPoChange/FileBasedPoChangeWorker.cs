using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using Serilog.Context;

namespace NdtBundleService.Services.FileBasedPoChange;

/// <summary>
/// Processes PO changes detected from TM Bundle WIP filenames by running the standard PO end workflow.
/// </summary>
public sealed class FileBasedPoChangeWorker : BackgroundService
{
    private readonly FileBasedPoChangeQueue _queue;
    private readonly IPoEndWorkflowService _poEndWorkflow;
    private readonly IWipBundleRunningPoProvider _wipRunningPo;
    private readonly IActivePoPerMillService _activePoPerMill;
    private readonly IOptions<NdtBundleOptions> _options;
    private readonly ILogger<FileBasedPoChangeWorker> _logger;

    public FileBasedPoChangeWorker(
        FileBasedPoChangeQueue queue,
        IPoEndWorkflowService poEndWorkflow,
        IWipBundleRunningPoProvider wipRunningPo,
        IActivePoPerMillService activePoPerMill,
        IOptions<NdtBundleOptions> options,
        ILogger<FileBasedPoChangeWorker> logger)
    {
        _queue = queue;
        _poEndWorkflow = poEndWorkflow;
        _wipRunningPo = wipRunningPo;
        _activePoPerMill = activePoPerMill;
        _options = options;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var bundle = _options.Value;
        if (MillPoEndSourceResolver.AnyMillUsesFilePoEnd(bundle))
        {
            var fileMills = (bundle.PlcHandshake ?? new PlcHandshakeOptions()).Mills
                .Where(m => m.ResolvePoEndSource(bundle) == MillPoEndSource.File)
                .Select(m => m.Name)
                .ToList();
            _logger.LogInformation(
                "FileBasedPoChangeWorker started — PO end from WIP filenames for mills: {Mills}.",
                fileMills.Count > 0 ? string.Join(", ", fileMills) : "(none configured)");
        }
        else
        {
            _logger.LogInformation(
                "FileBasedPoChangeWorker started — no mill uses PoEndSource=File; queue will drain if WIP events arrive.");
        }

        await foreach (var request in _queue.Reader.ReadAllAsync(stoppingToken).ConfigureAwait(false))
        {
            try
            {
                await ProcessRequestAsync(request, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "File-based PO change failed for Mill {Mill} ({OldPo} → {NewPo}, file {File}).",
                    request.MillNo,
                    request.EndedPo,
                    request.NewPo,
                    request.WipFileName);
            }
            finally
            {
                _queue.MarkCompleted(request.MillNo);
            }
        }
    }

    private async Task ProcessRequestAsync(FileBasedPoChangeRequest request, CancellationToken cancellationToken)
    {
        var correlationId = request.CorrelationId ?? Guid.NewGuid();
        using (LogContext.PushProperty("CorrelationId", correlationId))
        {
            await ProcessRequestCoreAsync(request, correlationId, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ProcessRequestCoreAsync(
        FileBasedPoChangeRequest request,
        Guid correlationId,
        CancellationToken cancellationToken)
    {
        var poEndSource = MillPoEndSourceResolver.ForMill(request.MillNo, _options.Value);
        if (poEndSource != MillPoEndSource.File)
        {
            _logger.LogWarning(
                "Mill {Mill}: stale file-based PO change from {File} ({OldPo} → {NewPo}) skipped — PoEndSource={Source} (expected File).",
                request.MillNo,
                request.WipFileName,
                request.EndedPo,
                request.NewPo,
                MillPoEndSourceResolver.ToConfigValue(poEndSource));
            return;
        }

        var endedPo = InputSlitCsvParsing.NormalizePo(request.EndedPo);
        if (string.IsNullOrWhiteSpace(endedPo))
        {
            endedPo = await ResolveEndedPoFallbackAsync(request.MillNo, cancellationToken).ConfigureAwait(false);
        }

        var newPo = InputSlitCsvParsing.NormalizePo(request.NewPo);
        if (string.IsNullOrWhiteSpace(newPo))
        {
            _logger.LogWarning(
                "Mill {Mill}: file-based PO change from {File} has no new PO; skipped.",
                request.MillNo,
                request.WipFileName);
            return;
        }

        if (!string.IsNullOrWhiteSpace(endedPo) && InputSlitCsvParsing.PoEquals(endedPo, newPo))
        {
            _logger.LogDebug(
                "Mill {Mill}: file-based PO change ignored — ended and new PO are both {Po}.",
                request.MillNo,
                endedPo);
            return;
        }

        if (!string.IsNullOrWhiteSpace(endedPo))
        {
            var advancePlan = (_options.Value.FileBasedPoEnd ?? new FileBasedPoEndOptions()).AdvancePoPlanFileOnPoEnd;
            if (request.FromReconciliation)
            {
                _logger.LogInformation(
                    "Mill {Mill}: reconciliation-triggered file-based PO change {OldPo} → {NewPo} from WIP file {File}; running PO end workflow. CorrelationId {CorrelationId}",
                    request.MillNo,
                    endedPo,
                    newPo,
                    request.WipFileName,
                    correlationId);
            }
            else
            {
                _logger.LogInformation(
                    "Mill {Mill}: file-based PO change {OldPo} → {NewPo} from WIP file {File}; running PO end workflow. CorrelationId {CorrelationId}",
                    request.MillNo,
                    endedPo,
                    newPo,
                    request.WipFileName,
                    correlationId);
            }

            await _poEndWorkflow.ExecuteAsync(endedPo, request.MillNo, advancePlan, cancellationToken, correlationId)
                .ConfigureAwait(false);
        }
        else
        {
            _logger.LogInformation(
                "Mill {Mill}: first WIP bundle PO {Po} from {File}; no previous PO to close.",
                request.MillNo,
                newPo,
                request.WipFileName);
        }

        if (_wipRunningPo.TrySetRunningPoFromWipFile(
                request.MillNo,
                newPo,
                request.WipStampUtc,
                request.WipFileName))
        {
            _logger.LogInformation(
                "Mill {Mill}: accepted new running PO {Po} from WIP file {File} after file-based PO change.",
                request.MillNo,
                newPo,
                request.WipFileName);
        }
    }

    private async Task<string> ResolveEndedPoFallbackAsync(int millNo, CancellationToken cancellationToken)
    {
        var poByMill = await _activePoPerMill.GetLatestPoByMillAsync(cancellationToken).ConfigureAwait(false);
        if (poByMill.TryGetValue(millNo, out var slitPo) && !string.IsNullOrWhiteSpace(slitPo))
        {
            _logger.LogInformation(
                "Mill {Mill}: ended PO resolved from latest Input Slit CSV as {Po}.",
                millNo,
                slitPo);
            return InputSlitCsvParsing.NormalizePo(slitPo);
        }

        return string.Empty;
    }
}
