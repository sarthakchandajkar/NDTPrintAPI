using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

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
        var cfg = _options.Value.FileBasedPoEnd ?? new FileBasedPoEndOptions();
        if (!cfg.Enabled)
        {
            _logger.LogInformation("FileBasedPoEnd is disabled; FileBasedPoChangeWorker will not process PO changes.");
            return;
        }

        _logger.LogInformation(
            "FileBasedPoChangeWorker started — PO end is driven by WIP bundle filename changes in the TM Bundle folder.");

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
            _logger.LogInformation(
                "Mill {Mill}: file-based PO change {OldPo} → {NewPo} from WIP file {File}; running PO end workflow.",
                request.MillNo,
                endedPo,
                newPo,
                request.WipFileName);

            await _poEndWorkflow.ExecuteAsync(endedPo, request.MillNo, advancePlan, cancellationToken)
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
