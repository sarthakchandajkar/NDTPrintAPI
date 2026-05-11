using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// On each poll: PO end for each configured PLC entry in <see cref="PlcPoEndOptions.Mills"/> (Modbus coil and/or PO_Id mode + optional MES ack).
/// </summary>
public sealed class PlcPoEndPollHandler
{
    private readonly NdtBundleOptions _options;
    private readonly IPlcClient _plcClient;
    private readonly IActivePoPerMillService _activePoPerMill;
    private readonly IPoEndWorkflowService _poEndWorkflow;
    private readonly MillPoEndTransitionDetector _poIdDetector;
    private readonly PoEndDetectionDiagnostics _diagnostics;
    private readonly ICurrentPoPlanService? _currentPoPlanService;
    private readonly ILogger<PlcPoEndPollHandler> _logger;

    /// <summary>Previous scan coil level per mill index 0..3 = mills 1..4.</summary>
    private readonly bool[] _lastCoilLevel = new bool[4];

    public PlcPoEndPollHandler(
        IOptions<NdtBundleOptions> options,
        IPlcClient plcClient,
        IActivePoPerMillService activePoPerMill,
        IPoEndWorkflowService poEndWorkflow,
        MillPoEndTransitionDetector poIdDetector,
        PoEndDetectionDiagnostics diagnostics,
        ILogger<PlcPoEndPollHandler> logger,
        ICurrentPoPlanService? currentPoPlanService = null)
    {
        _options = options.Value;
        _plcClient = plcClient;
        _activePoPerMill = activePoPerMill;
        _poEndWorkflow = poEndWorkflow;
        _poIdDetector = poIdDetector;
        _diagnostics = diagnostics;
        _currentPoPlanService = currentPoPlanService;
        _logger = logger;
    }

    public async Task PollAsync(CancellationToken cancellationToken)
    {
        var plcCfg = _options.PlcPoEnd ?? new PlcPoEndOptions();
        if (!plcCfg.Enabled)
            return;

        _diagnostics.SetDetectionMode(plcCfg.DetectionMode ?? "CoilRisingEdge");

        if (PlcPoEndOptions.IsModbusPoIdTransition(plcCfg))
        {
            await PollPoIdTransitionAsync(plcCfg, cancellationToken).ConfigureAwait(false);
            return;
        }

        await PollCoilRisingEdgeAsync(plcCfg, cancellationToken).ConfigureAwait(false);
    }

    private async Task PollCoilRisingEdgeAsync(PlcPoEndOptions plcCfg, CancellationToken cancellationToken)
    {
        IReadOnlyDictionary<int, bool> signals;
        try
        {
            signals = await _plcClient.GetPoEndSignalsByMillAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "PLC PO-end poll failed; skipping this cycle.");
            return;
        }

        IReadOnlyDictionary<int, string>? poByMill = null;

        foreach (var ep in plcCfg.Mills.OrderBy(e => e.MillNo))
        {
            var m = ep.MillNo;
            if (m is < 1 or > 4)
                continue;

            var now = signals.TryGetValue(m, out var v) && v;
            var was = _lastCoilLevel[m - 1];
            _lastCoilLevel[m - 1] = now;

            if (!now || was)
                continue;

            poByMill ??= await _activePoPerMill.GetLatestPoByMillAsync(cancellationToken).ConfigureAwait(false);
            if (!poByMill.TryGetValue(m, out var po) || string.IsNullOrWhiteSpace(po))
            {
                _logger.LogWarning(
                    "PLC PO-end rising edge for Mill {Mill} but no active PO found in slit CSVs; close bundles skipped for this mill.",
                    m);
                continue;
            }

            var ok = await RunWorkflowForMillAsync(m, po, plcCfg, cancellationToken).ConfigureAwait(false);
            await AckMesIfNeededAsync(m, plcCfg, ok, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task PollPoIdTransitionAsync(PlcPoEndOptions plcCfg, CancellationToken cancellationToken)
    {
        IReadOnlyDictionary<int, MillPoPlcSnapshot>? snapshots;
        try
        {
            snapshots = await _plcClient.ReadMillPoSnapshotsAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "PLC PO_Id snapshot read failed; skipping this cycle.");
            return;
        }

        if (snapshots is null || snapshots.Count == 0)
        {
            _logger.LogWarning("PO_Id transition mode active but no Modbus snapshots returned; check PlcPoEnd.Mills and ModbusTcp driver.");
            return;
        }

        IReadOnlyDictionary<int, string>? poByMill = null;

        foreach (var ep in plcCfg.Mills.OrderBy(e => e.MillNo))
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (ep.MillNo is < 1 or > 4)
                continue;
            if (!snapshots.TryGetValue(ep.MillNo, out var snap))
                continue;

            _diagnostics.UpdateMill(ep.MillNo, d =>
            {
                d.LastError = snap.ReadOk ? null : "Modbus read failed";
                if (snap.ReadOk)
                    d.CurrentPoIdFromPlc = snap.PoId;
            });

            var endedPo = _poIdDetector.Evaluate(ep.MillNo, snap, plcCfg, ep);

            _diagnostics.UpdateMill(ep.MillNo, d =>
            {
                d.TrackedPrevPoId = _poIdDetector.GetTrackedPrevPoId(ep.MillNo);
                if (snap.ReadOk && snap.SlitEntryCount.HasValue)
                    d.LastSlitEntryCount = snap.SlitEntryCount;
            });

            if (endedPo is null)
                continue;

            if (ep.RequireLatestCsvPoMatchesEndedPlcPo)
            {
                poByMill ??= await _activePoPerMill.GetLatestPoByMillAsync(cancellationToken).ConfigureAwait(false);
                if (!poByMill.TryGetValue(ep.MillNo, out var csvPo) ||
                    !InputSlitCsvParsing.PoEquals(csvPo, endedPo))
                {
                    _logger.LogWarning(
                        "Mill {Mill}: PO_Id transition ended PO {Ended} but latest slit CSV PO mismatch or missing (RequireLatestCsvPoMatchesEndedPlcPo); skipping workflow.",
                        ep.MillNo,
                        endedPo);
                    continue;
                }
            }

            _diagnostics.UpdateMill(ep.MillNo, d =>
            {
                d.LastTransitionUtc = DateTimeOffset.UtcNow;
                d.LastEndedPoNumber = endedPo;
            });

            var ok = await RunWorkflowForMillAsync(ep.MillNo, endedPo, plcCfg, cancellationToken).ConfigureAwait(false);
            await AckMesIfNeededAsync(ep.MillNo, plcCfg, ok, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Writes PLC RESETMESTOPLC (e.g. M20.2) after PO end processing to clear POChangeTOMES (e.g. M20.1).</summary>
    private async Task AckMesIfNeededAsync(int millNo, PlcPoEndOptions plcCfg, bool workflowSucceeded, CancellationToken cancellationToken)
    {
        if (plcCfg.WriteMesAckOnlyOnWorkflowSuccess && !workflowSucceeded)
            return;

        try
        {
            await _plcClient.AcknowledgeMesPoChangeAsync(millNo, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "MES acknowledgment Modbus write failed for Mill {Mill}.", millNo);
        }
    }

    private async Task<bool> RunWorkflowForMillAsync(int millNo, string poNumber, PlcPoEndOptions plcCfg, CancellationToken cancellationToken)
    {
        var advancePlan = plcCfg.AdvancePoPlanFileOnPoEnd && _currentPoPlanService != null;
        _logger.LogInformation(
            "PO end Mill {Mill} PO {PO}; running PO end workflow (advance plan file: {Advance}).",
            millNo,
            poNumber,
            advancePlan);

        try
        {
            await _poEndWorkflow.ExecuteAsync(poNumber, millNo, advancePlan, cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "PO end workflow failed for Mill {Mill} PO {PO}.", millNo, poNumber);
            return false;
        }
    }
}
