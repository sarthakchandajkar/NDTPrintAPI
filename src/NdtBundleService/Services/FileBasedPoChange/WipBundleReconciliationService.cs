using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using Serilog.Context;

namespace NdtBundleService.Services.FileBasedPoChange;

/// <inheritdoc />
public sealed class WipBundleReconciliationService : IWipBundleReconciliationService
{
    private readonly IOptions<NdtBundleOptions> _options;
    private readonly INdtBundleRepository _bundleRepository;
    private readonly IWipBundleRunningPoProvider _wipRunningPo;
    private readonly FileBasedPoChangeQueue _queue;
    private readonly ILogger<WipBundleReconciliationService> _logger;

    public WipBundleReconciliationService(
        IOptions<NdtBundleOptions> options,
        INdtBundleRepository bundleRepository,
        IWipBundleRunningPoProvider wipRunningPo,
        FileBasedPoChangeQueue queue,
        ILogger<WipBundleReconciliationService> logger)
    {
        _options = options;
        _bundleRepository = bundleRepository;
        _wipRunningPo = wipRunningPo;
        _queue = queue;
        _logger = logger;
    }

    public async Task<int> ReconcileAsync(CancellationToken cancellationToken)
    {
        var options = _options.Value;
        if (!MillPoEndSourceResolver.AnyMillUsesFilePoEnd(options))
            return 0;

        var sqlEnabled = SqlTraceabilityConnection.IsSqlEnabled(options);
        if (!sqlEnabled)
        {
            _logger.LogWarning(
                "WIP bundle reconciliation: SQL disabled; durable NDT_Bundle checks skipped — using in-memory WIP state only.");
        }

        var candidates = WipBundleFolderScanner.Scan(options);
        var enqueued = 0;

        for (var millNo = 1; millNo <= 4; millNo++)
        {
            if (MillPoEndSourceResolver.ForMill(millNo, options) != MillPoEndSource.File)
                continue;

            enqueued += await ReconcileMillAsync(
                millNo,
                candidates,
                sqlEnabled,
                cancellationToken).ConfigureAwait(false);
        }

        if (enqueued > 0)
        {
            _logger.LogInformation(
                "WIP bundle reconciliation pass complete — {Count} missed file-based PO change(s) enqueued.",
                enqueued);
        }

        return enqueued;
    }

    private async Task<int> ReconcileMillAsync(
        int millNo,
        IReadOnlyList<WipBundleFolderScanner.WipBundleFileCandidate> candidates,
        bool sqlEnabled,
        CancellationToken cancellationToken)
    {
        var millCandidates = candidates
            .Where(c => c.MillNo == millNo)
            .OrderByDescending(c => c.StampUtc)
            .ThenByDescending(c => c.SortKey, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (millCandidates.Count == 0)
            return 0;

        var latest = millCandidates[0];
        var newPo = InputSlitCsvParsing.NormalizePo(latest.PoNumber);
        if (string.IsNullOrWhiteSpace(newPo))
            return 0;

        var prior = millCandidates
            .Skip(1)
            .FirstOrDefault(c => !InputSlitCsvParsing.PoEquals(c.PoNumber, newPo));

        if (prior.MillNo == 0)
            return 0;

        var endedPo = InputSlitCsvParsing.NormalizePo(prior.PoNumber);
        if (string.IsNullOrWhiteSpace(endedPo) || InputSlitCsvParsing.PoEquals(endedPo, newPo))
            return 0;

        if (_queue.IsMillPending(millNo))
            return 0;

        var runningPo = await _wipRunningPo.TryGetRunningPoForMillAsync(millNo, cancellationToken).ConfigureAwait(false);
        if (!string.IsNullOrWhiteSpace(runningPo)
            && InputSlitCsvParsing.PoEquals(runningPo, newPo)
            && !_wipRunningPo.IsWaitingForNewWipAfterPoEnd(millNo))
        {
            return 0;
        }

        if (sqlEnabled)
        {
            if (await _bundleRepository.HasPrintedBundleForPoAsync(millNo, newPo, cancellationToken).ConfigureAwait(false))
                return 0;
        }

        var correlationId = Guid.NewGuid();
        var ageMinutes = Math.Max(0, (DateTime.UtcNow - latest.StampUtc).TotalMinutes);

        NdtBundleRecord? latestPrinted = null;
        if (sqlEnabled)
        {
            latestPrinted = await _bundleRepository
                .GetLatestPrintedBundleForMillAsync(millNo, cancellationToken)
                .ConfigureAwait(false);
        }

        using (LogContext.PushProperty("CorrelationId", correlationId))
        {
            var request = new FileBasedPoChangeRequest
            {
                MillNo = millNo,
                EndedPo = endedPo,
                NewPo = newPo,
                WipStampUtc = latest.StampUtc,
                WipFileName = latest.FileName,
                CorrelationId = correlationId,
                FromReconciliation = true
            };

            if (!_queue.TryEnqueue(request))
                return 0;

            _logger.LogWarning(
                "Mill {Mill}: reconciliation caught missed file-based PO change {OldPo} → {NewPo} from {File} (age {AgeMinutes:F1} min, latestPrintedPo {LatestPrintedPo}, latestPrintedAt {LatestPrintedAt}) — enqueued, CorrelationId {CorrelationId}.",
                millNo,
                endedPo,
                newPo,
                latest.FileName,
                ageMinutes,
                latestPrinted?.PoNumber ?? "(none)",
                latestPrinted?.PrintedAt,
                correlationId);
        }

        return 1;
    }
}
