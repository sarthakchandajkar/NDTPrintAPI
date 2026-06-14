namespace NdtBundleService.Services.PlcHandshake;

/// <summary>MW58 threshold and MW56 accumulated values derived from running PO + formation chart + bundle engine state.</summary>
public sealed record MillHooterResolvedValues(
    string? PoNumber,
    string? PipeSize,
    int Threshold,
    int Accumulated)
{
    public static MillHooterResolvedValues Empty { get; } = new(null, null, 0, 0);

    public bool HasPo => !string.IsNullOrWhiteSpace(PoNumber);
}

/// <summary>
/// Resolves hooter PLC memory values: MW58 from formation chart (running PO pipe size),
/// MW56 from bundle-engine size count (NDT pipes toward the next bundle for that size).
/// </summary>
public interface IMillHooterPlcValuesService
{
    Task<MillHooterResolvedValues> ResolveAsync(int millNo, CancellationToken cancellationToken);
}

public sealed class MillHooterPlcValuesService : IMillHooterPlcValuesService
{
    private readonly IActivePoPerMillService _activePoPerMill;
    private readonly IPipeSizeProvider _pipeSizeProvider;
    private readonly IFormationChartProvider _formationChartProvider;
    private readonly INdtBundleRuntimeStateStore _runtimeState;

    public MillHooterPlcValuesService(
        IActivePoPerMillService activePoPerMill,
        IPipeSizeProvider pipeSizeProvider,
        IFormationChartProvider formationChartProvider,
        INdtBundleRuntimeStateStore runtimeState)
    {
        _activePoPerMill = activePoPerMill;
        _pipeSizeProvider = pipeSizeProvider;
        _formationChartProvider = formationChartProvider;
        _runtimeState = runtimeState;
    }

    public async Task<MillHooterResolvedValues> ResolveAsync(int millNo, CancellationToken cancellationToken)
    {
        if (millNo is < 1 or > 4)
            return MillHooterResolvedValues.Empty;

        try
        {
            await _runtimeState.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception)
        {
            return MillHooterResolvedValues.Empty;
        }

        var poByMill = await _activePoPerMill.GetLatestPoByMillAsync(cancellationToken).ConfigureAwait(false);
        if (!poByMill.TryGetValue(millNo, out var po) || string.IsNullOrWhiteSpace(po))
            return MillHooterResolvedValues.Empty;

        var poNorm = InputSlitCsvParsing.NormalizePo(po.Trim());

        var pipeSizeByPo = await _pipeSizeProvider.GetPipeSizeByPoAsync(cancellationToken).ConfigureAwait(false);
        pipeSizeByPo.TryGetValue(poNorm, out var pipeSize);

        var formation = await _formationChartProvider.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);
        var threshold = FormationChartLookup.ResolveThreshold(formation, pipeSize);

        var sizeKey = FormationChartLookup.NormalizePipeSizeKey(pipeSize);
        if (string.IsNullOrEmpty(sizeKey))
            sizeKey = "Default";

        var sizeCounts = _runtimeState.GetSizeCounts(poNorm, millNo);
        sizeCounts.TryGetValue(sizeKey, out var accumulated);

        return new MillHooterResolvedValues(poNorm, pipeSize, threshold, Math.Max(0, accumulated));
    }
}
