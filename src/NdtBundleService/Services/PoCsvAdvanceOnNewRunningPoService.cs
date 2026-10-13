using Microsoft.Extensions.Logging;
using NdtBundleService.Configuration;
using NdtBundleService.Services.PlcHandshake;

namespace NdtBundleService.Services;

/// <summary>
/// Opens a stamp-only <c>CsvAdvance</c> fill parent when WIP confirms a new running PO after PO change,
/// so early Input Slit CSVs (including 1-slit month-end POs) do not wait for the first PLC threshold print.
/// Also flushes PLC slit-end NDT buffered during the wait-for-WIP gap into MW56 remainder.
/// </summary>
public interface IPoCsvAdvanceOnNewRunningPoService
{
    Task TryOpenStampForRunningPoAsync(int millNo, string poNumber, CancellationToken cancellationToken);
}

/// <inheritdoc />
public sealed class PoCsvAdvanceOnNewRunningPoService : IPoCsvAdvanceOnNewRunningPoService
{
    private readonly IMillSequenceService _millSequence;
    private readonly IFormationChartProvider _formationChartProvider;
    private readonly IPipeSizeProvider _pipeSizeProvider;
    private readonly IMillOwnership _millOwnership;
    private readonly IPlcSlitEndBundleCloser _slitEndCloser;
    private readonly PlcHandshakeCoordinator _handshakeCoordinator;
    private readonly ILogger<PoCsvAdvanceOnNewRunningPoService> _logger;

    public PoCsvAdvanceOnNewRunningPoService(
        IMillSequenceService millSequence,
        IFormationChartProvider formationChartProvider,
        IPipeSizeProvider pipeSizeProvider,
        IMillOwnership millOwnership,
        IPlcSlitEndBundleCloser slitEndCloser,
        PlcHandshakeCoordinator handshakeCoordinator,
        ILogger<PoCsvAdvanceOnNewRunningPoService> logger)
    {
        _millSequence = millSequence;
        _formationChartProvider = formationChartProvider;
        _pipeSizeProvider = pipeSizeProvider;
        _millOwnership = millOwnership;
        _slitEndCloser = slitEndCloser;
        _handshakeCoordinator = handshakeCoordinator;
        _logger = logger;
    }

    public async Task TryOpenStampForRunningPoAsync(
        int millNo,
        string poNumber,
        CancellationToken cancellationToken)
    {
        if (!_millSequence.IsEnabled || millNo is < 1 or > 4 || !_millOwnership.Owns(millNo))
            return;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po))
            return;

        string? pipeSize = null;
        try
        {
            pipeSize = await _pipeSizeProvider.TryGetPipeSizeForPoAsync(po, cancellationToken).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(pipeSize))
            {
                var byPo = await _pipeSizeProvider.GetPipeSizeByPoAsync(cancellationToken).ConfigureAwait(false);
                byPo.TryGetValue(po, out pipeSize);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "WIP stamp-open: pipe size lookup failed for PO {PO} Mill {Mill}.", po, millNo);
        }

        IReadOnlyDictionary<string, Models.FormationChartEntry> formation;
        try
        {
            formation = await _formationChartProvider.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "WIP stamp-open: formation chart load failed for PO {PO} Mill {Mill}.", po, millNo);
            formation = new Dictionary<string, Models.FormationChartEntry>(StringComparer.OrdinalIgnoreCase);
        }

        var provisional = Math.Max(1, FormationChartLookup.ResolveThreshold(formation, pipeSize));

        var opened = await _millSequence
            .TryOpenCsvAdvanceStampTargetAsync(po, millNo, provisional, slitNo: null, cancellationToken)
            .ConfigureAwait(false);

        if (opened is { } o)
        {
            _logger.LogInformation(
                "WIP-confirmed running PO {PO} Mill {Mill}: opened CSV-advance stamp {BundleNo} provisionalTarget={Target}.",
                po,
                millNo,
                o.Formatted,
                provisional);
        }

        // Apply slit-end NDT that finished while Active PO was cleared (waiting for this WIP).
        try
        {
            var flushed = await _slitEndCloser
                .FlushPendingPoChangeNdtAsync(millNo, po, cancellationToken)
                .ConfigureAwait(false);
            if (flushed > 0)
            {
                await _handshakeCoordinator
                    .TrySyncHooterFromMesAsync(millNo, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "WIP-confirmed PO {PO} Mill {Mill}: failed to flush buffered PO-change slit NDT into hooter remainder.",
                po,
                millNo);
        }
    }
}
