using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services.PoLifecycle;

/// <summary>
/// Transitions a <see cref="PoLifecyclePhase.Closed"/> PO back to <see cref="PoLifecyclePhase.Running"/>
/// and clears open accumulation when the mill re-adopts it via WIP confirmation.
/// </summary>
public sealed class PoReopenService
{
    private readonly IPoLifecycleService _lifecycle;
    private readonly Lazy<INdtBundleRuntimeStateStore> _runtimeState;
    private readonly INdtBundleRepository _bundleRepository;
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly ILogger<PoReopenService> _logger;

    public PoReopenService(
        IPoLifecycleService lifecycle,
        Lazy<INdtBundleRuntimeStateStore> runtimeState,
        INdtBundleRepository bundleRepository,
        IOptionsMonitor<NdtBundleOptions> options,
        ILogger<PoReopenService> logger)
    {
        _lifecycle = lifecycle;
        _runtimeState = runtimeState;
        _bundleRepository = bundleRepository;
        _options = options;
        _logger = logger;
    }

    /// <summary>
    /// When the PO is Closed on a Plc lifecycle mill, moves it to Running and zeroes open accumulation.
    /// Force-finalizes any <c>Awaiting_Csv_Recon</c> bundle for this PO/mill before clearing accumulation.
    /// </summary>
    /// <param name="wipConfirmedRunningPo">WIP-file PO that confirms resume; slit activity alone must not reopen.</param>
    /// <returns>True when a Closed → Running transition was applied.</returns>
    public bool TryReopenIfClosed(int millNo, string poNumber, string wipConfirmedRunningPo)
    {
        if (MillPoEndSourceResolver.ForMill(millNo, _options.CurrentValue) != MillPoEndSource.Plc)
            return false;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po) || millNo is < 1 or > 4)
            return false;

        if (_lifecycle.GetPhase(millNo, po) != PoLifecyclePhase.Closed)
            return false;

        if (!PoRunningAdoption.IsWipConfirmedRunning(po, wipConfirmedRunningPo))
            return false;

        if (!_lifecycle.TryReopen(millNo, po))
            return false;

        try
        {
            _bundleRepository
                .TryForceFinalizeAwaitingReconOnReopenAsync(po, millNo, CancellationToken.None)
                .GetAwaiter()
                .GetResult();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "PO reopen recon finalize failed for PO {PO} Mill {Mill}; lifecycle reopen continues.",
                po,
                millNo);
        }

        _runtimeState.Value.ClearOpenAccumulation(po, millNo);

        _logger.LogWarning(
            "PO reopened (was Closed): PO {PO} Mill {Mill}.",
            po,
            millNo);

        return true;
    }
}
