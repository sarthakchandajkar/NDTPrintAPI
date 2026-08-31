using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Quiet-drain cutover guard: refuse to start when pre-migration awaiting-recon rows remain,
/// fill targets are missing on printed bundles, or Bundle_Accumulation still has open rows.
/// Mill mode scopes SQL + runtime checks to the owned mill; Monolith checks all mills.
/// </summary>
public sealed class FillCutoverStartupCheck : IHostedService
{
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly ICsvFillService _csvFill;
    private readonly INdtBundleRuntimeStateStore _runtimeState;
    private readonly IMillOwnership _ownership;
    private readonly ILogger<FillCutoverStartupCheck> _logger;

    public FillCutoverStartupCheck(
        IOptionsMonitor<NdtBundleOptions> options,
        ICsvFillService csvFill,
        INdtBundleRuntimeStateStore runtimeState,
        IMillOwnership ownership,
        ILogger<FillCutoverStartupCheck> logger,
        IMillSequenceService? millSequence = null)
    {
        _options = options;
        _csvFill = csvFill;
        _runtimeState = runtimeState;
        _ownership = ownership;
        _logger = logger;
        _ = millSequence;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var opt = _options.CurrentValue;
        if (!opt.RequireCleanFillCutover)
        {
            _logger.LogWarning(
                "RequireCleanFillCutover=false — skipping fill-to-target cutover safety check.");
            return;
        }

        await _runtimeState.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var millNo = _ownership.SingleOwnedMill;
        var awaiting = await _csvFill.HasAwaitingCsvReconRowsAsync(cancellationToken, millNo).ConfigureAwait(false);
        var missingTarget = await _csvFill.HasBundlesMissingFillTargetAsync(cancellationToken, millNo).ConfigureAwait(false);
        var openRuntime = _runtimeState.HasUnsafeOpenStateForFillCutover(millNo);

        if (!awaiting && !missingTarget && !openRuntime)
        {
            _logger.LogInformation(
                "Fill-to-target cutover check passed{Scope}.",
                millNo.HasValue ? $" (mill {millNo.Value})" : "");
            return;
        }

        var reasons = new List<string>();
        if (awaiting)
            reasons.Add(millNo.HasValue
                ? $"Awaiting_Csv_Recon=1 rows exist for mill {millNo.Value}"
                : "Awaiting_Csv_Recon=1 rows exist");
        if (missingTarget)
            reasons.Add(millNo.HasValue
                ? $"printed bundles missing Target_Ndt_Pcs for mill {millNo.Value}"
                : "printed bundles missing Target_Ndt_Pcs");
        if (openRuntime)
            reasons.Add("Bundle_Accumulation has open size-count rows");

        var detail = string.Join("; ", reasons);
        _logger.LogError(
            "Fill-to-target cutover blocked: {Detail}. "
            + "Deploy only after quiet drain (docs/Fill_To_Target_Quiet_Drain_Checklist.sql). "
            + "Set NdtBundle:RequireCleanFillCutover=false only for local tests.",
            detail);

        throw new InvalidOperationException(
            "Fill-to-target cutover blocked: " + detail + ". Complete quiet drain first.");
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
