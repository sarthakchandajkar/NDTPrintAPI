using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services.PlcHandshake;

/// <summary>
/// Runs the existing PO-end workflow (close partial bundles, batch state, optional plan advance)
/// when the PLC handshake detects a PO change.
/// </summary>
public sealed class PoChangeHandler : IPoChangeHandler
{
    private readonly IPoEndWorkflowService _poEndWorkflow;
    private readonly IActivePoPerMillService _activePoPerMill;
    private readonly ICurrentPoPlanService? _currentPoPlanService;
    private readonly IOptions<NdtBundleOptions> _options;
    private readonly ILogger<PoChangeHandler> _logger;

    public PoChangeHandler(
        IPoEndWorkflowService poEndWorkflow,
        IActivePoPerMillService activePoPerMill,
        IOptions<NdtBundleOptions> options,
        ILogger<PoChangeHandler> logger,
        ICurrentPoPlanService? currentPoPlanService = null)
    {
        _poEndWorkflow = poEndWorkflow;
        _activePoPerMill = activePoPerMill;
        _options = options;
        _logger = logger;
        _currentPoPlanService = currentPoPlanService;
    }

    public async Task HandlePoChangeAsync(MillConfig mill, CancellationToken cancellationToken)
    {
        var millNo = mill.ResolveMillNo();
        if (millNo is < 1 or > 4)
        {
            _logger.LogWarning(
                "PO change for {MillName}: invalid MillNo; expected 1–4 (set MillNo or Name Mill-N).",
                mill.Name);
            return;
        }

        _logger.LogInformation(
            "PO change detected for {MillName} (Mill {MillNo}) from PLC trigger {Trigger}.",
            mill.Name,
            millNo,
            mill.TriggerAddress);

        var poByMill = await _activePoPerMill.GetLatestPoByMillAsync(cancellationToken).ConfigureAwait(false);
        if (!poByMill.TryGetValue(millNo, out var po) || string.IsNullOrWhiteSpace(po))
        {
            _logger.LogWarning(
                "{MillName}: PO change trigger active but no PO from latest Input Slit CSV for Mill {MillNo}; PO end workflow skipped.",
                mill.Name,
                millNo);
            return;
        }

        var handshake = _options.Value.PlcHandshake ?? new PlcHandshakeOptions();
        var advancePlan = handshake.AdvancePoPlanFileOnPoEnd && _currentPoPlanService != null;

        _logger.LogInformation(
            "{MillName}: running PO end workflow for PO {PO} (advance plan file: {Advance}).",
            mill.Name,
            po,
            advancePlan);

        try
        {
            await _poEndWorkflow.ExecuteAsync(po, millNo, advancePlan, cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("{MillName}: PO end workflow completed for PO {PO}.", mill.Name, po);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "{MillName}: PO end workflow failed for PO {PO}.", mill.Name, po);
        }
    }
}
