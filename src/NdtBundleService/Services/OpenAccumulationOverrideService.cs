using Microsoft.Extensions.Logging;
using NdtBundleService.Services.PlcHandshake;

namespace NdtBundleService.Services;

public sealed record OpenAccumulationOverrideResult(
    bool Success,
    string? Message,
    int MillNo,
    string? PoNumber,
    string SizeKey,
    int PreviousAccumulated,
    int NewAccumulated,
    int Threshold,
    bool HooterSyncedToPlc,
    MillHooterResolvedValues? ResolvedHooter);

/// <summary>
/// Operator correction for open-bundle PLC accumulation (<c>sizeCounts</c>) when restart or lost
/// slit-edge state desyncs MES/MW56 from actual production toward the next bundle close.
/// </summary>
public sealed class OpenAccumulationOverrideService
{
    private readonly INdtBundleRuntimeStateStore _runtimeState;
    private readonly IActivePoPerMillService _activePoPerMill;
    private readonly IPipeSizeProvider _pipeSizeProvider;
    private readonly IFormationChartProvider _formationChartProvider;
    private readonly IMillHooterPlcValuesService _hooterValues;
    private readonly PlcHandshakeCoordinator _handshakeCoordinator;
    private readonly ILogger<OpenAccumulationOverrideService> _logger;

    public OpenAccumulationOverrideService(
        INdtBundleRuntimeStateStore runtimeState,
        IActivePoPerMillService activePoPerMill,
        IPipeSizeProvider pipeSizeProvider,
        IFormationChartProvider formationChartProvider,
        IMillHooterPlcValuesService hooterValues,
        PlcHandshakeCoordinator handshakeCoordinator,
        ILogger<OpenAccumulationOverrideService> logger)
    {
        _runtimeState = runtimeState;
        _activePoPerMill = activePoPerMill;
        _pipeSizeProvider = pipeSizeProvider;
        _formationChartProvider = formationChartProvider;
        _hooterValues = hooterValues;
        _handshakeCoordinator = handshakeCoordinator;
        _logger = logger;
    }

    public async Task<OpenAccumulationOverrideResult> SetOpenAccumulationAsync(
        int millNo,
        int accumulated,
        string? poNumber,
        string? sizeKey,
        CancellationToken cancellationToken)
    {
        if (millNo is < 1 or > 4)
        {
            return Fail(millNo, "MillNo must be between 1 and 4.", sizeKey: sizeKey ?? "Default");
        }

        if (accumulated < 0)
        {
            return Fail(millNo, "Accumulated count must be non-negative.", sizeKey: sizeKey ?? "Default");
        }

        await _runtimeState.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var po = string.IsNullOrWhiteSpace(poNumber) ? null : InputSlitCsvParsing.NormalizePo(poNumber.Trim());
        if (string.IsNullOrWhiteSpace(po))
        {
            var poByMill = await _activePoPerMill.GetLatestPoByMillAsync(cancellationToken).ConfigureAwait(false);
            if (!poByMill.TryGetValue(millNo, out var activePo) || string.IsNullOrWhiteSpace(activePo))
            {
                return Fail(
                    millNo,
                    "No running PO for this mill. Provide poNumber explicitly or start production on the line.",
                    sizeKey: sizeKey ?? "Default");
            }

            po = InputSlitCsvParsing.NormalizePo(activePo.Trim());
        }

        var resolvedSizeKey = (sizeKey ?? string.Empty).Trim();
        if (string.IsNullOrEmpty(resolvedSizeKey))
        {
            string? pipeSize = null;
            try
            {
                var pipeSizeByPo = await _pipeSizeProvider.GetPipeSizeByPoAsync(cancellationToken).ConfigureAwait(false);
                pipeSizeByPo.TryGetValue(po, out pipeSize);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Pipe size lookup failed for PO {PO}; using Default size key.", po);
            }

            resolvedSizeKey = FormationChartLookup.NormalizePipeSizeKey(pipeSize);
            if (string.IsNullOrEmpty(resolvedSizeKey))
                resolvedSizeKey = "Default";
        }

        var before = _runtimeState.GetSizeCounts(po, millNo);
        var previous = before.GetValueOrDefault(resolvedSizeKey);

        var next = new Dictionary<string, int>(before, StringComparer.OrdinalIgnoreCase)
        {
            [resolvedSizeKey] = accumulated
        };
        _runtimeState.SetSizeCounts(po, millNo, next);
        await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);

        var hooterSynced = await _handshakeCoordinator
            .TrySyncHooterFromMesAsync(millNo, cancellationToken)
            .ConfigureAwait(false);

        var formation = await _formationChartProvider.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);
        string? pipeSizeForThreshold = null;
        try
        {
            var pipeSizeByPo = await _pipeSizeProvider.GetPipeSizeByPoAsync(cancellationToken).ConfigureAwait(false);
            pipeSizeByPo.TryGetValue(po, out pipeSizeForThreshold);
        }
        catch
        {
            // best-effort threshold for response
        }

        var threshold = FormationChartLookup.ResolveThreshold(formation, pipeSizeForThreshold);
        var resolved = await _hooterValues.ResolveAsync(millNo, cancellationToken).ConfigureAwait(false);

        _logger.LogWarning(
            "Operator open-accumulation override Mill {Mill} PO {PO} size {SizeKey}: {Previous} → {New} (threshold {Threshold}, hooterSynced={Synced}).",
            millNo,
            po,
            resolvedSizeKey,
            previous,
            accumulated,
            threshold,
            hooterSynced);

        return new OpenAccumulationOverrideResult(
            Success: true,
            Message: hooterSynced
                ? "Open accumulation updated, persisted, and MW56/MW58 rewritten on PLC."
                : "Open accumulation updated and persisted. PLC hooter sync skipped (handshake not connected or hooter disabled).",
            MillNo: millNo,
            PoNumber: po,
            SizeKey: resolvedSizeKey,
            PreviousAccumulated: previous,
            NewAccumulated: accumulated,
            Threshold: threshold,
            HooterSyncedToPlc: hooterSynced,
            ResolvedHooter: resolved);
    }

    private static OpenAccumulationOverrideResult Fail(int millNo, string message, string sizeKey) =>
        new(false, message, millNo, null, sizeKey, 0, 0, 0, false, null);
}
