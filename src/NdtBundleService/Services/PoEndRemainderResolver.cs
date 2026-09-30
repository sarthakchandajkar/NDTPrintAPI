using NdtBundleService.Services.PlcHandshake;

namespace NdtBundleService.Services;

/// <summary>
/// Resolves NDT pipes accumulated since the last printed bundle (toward-next-bundle / MW56 semantic)
/// for an immediate PLC PO-end flush.
/// </summary>
public static class PoEndRemainderResolver
{
    /// <summary>
    /// Order: runtime sizeCounts → hooter AccumulatedValue (MW56 mirror) → PLC DB251 NDT at edge.
    /// </summary>
    public static int Resolve(
        string poNumber,
        int millNo,
        string? pipeSize,
        INdtBundleRuntimeStateStore runtimeState,
        PlcHandshakeStatusRegistry? handshakeStatus,
        int? plcNdtCountFinal)
    {
        var fromSize = GetSizeCountTotal(poNumber, millNo, pipeSize, runtimeState);
        if (fromSize > 0)
            return fromSize;

        if (handshakeStatus is not null &&
            handshakeStatus.TryGetMill(millNo, out var st) &&
            st?.AccumulatedValue is int mw56 &&
            mw56 > 0)
        {
            return mw56;
        }

        if (plcNdtCountFinal is int plc && plc > 0)
            return plc;

        return 0;
    }

    /// <summary>
    /// Manual PO-end confirm: merge app remainder with live PLC so the current (last) slit
    /// is included even when it never got a slit-end accumulate into sizeCounts.
    /// </summary>
    public static int ResolveForManualConfirm(
        string poNumber,
        int millNo,
        string? pipeSize,
        INdtBundleRuntimeStateStore runtimeState,
        PlcHandshakeStatusRegistry? handshakeStatus,
        int? livePlcNdt)
    {
        var fromSize = GetSizeCountTotal(poNumber, millNo, pipeSize, runtimeState);
        var fromMw56 = 0;
        if (handshakeStatus is not null &&
            handshakeStatus.TryGetMill(millNo, out var st) &&
            st?.AccumulatedValue is int mw56 &&
            mw56 > 0)
        {
            fromMw56 = mw56;
        }

        var fromLive = livePlcNdt is int n && n > 0 ? n : 0;
        var best = Math.Max(fromSize, Math.Max(fromMw56, fromLive));

        // When MW56 still matches finished-slit sizeCounts, live DB251 is typically the open/last slit
        // not yet folded into the app remainder (mid-slit PO change → confirm after slit ends).
        // Require a positive MW56 so a missing hooter reading does not double-count sizeCounts+live.
        if (fromSize > 0 && fromLive > 0 && fromMw56 > 0 && fromMw56 <= fromSize)
        {
            var merged = fromSize + fromLive;
            if (merged > best)
                best = merged;
        }

        return best;
    }

    private static int GetSizeCountTotal(
        string poNumber,
        int millNo,
        string? pipeSize,
        INdtBundleRuntimeStateStore runtimeState)
    {
        var sizeKey = FormationChartLookup.NormalizePipeSizeKey(pipeSize);
        if (string.IsNullOrEmpty(sizeKey))
            sizeKey = "Default";

        var sizeCounts = runtimeState.GetSizeCounts(poNumber, millNo);
        if (sizeCounts.TryGetValue(sizeKey, out var fromSize) && fromSize > 0)
            return fromSize;

        foreach (var kv in sizeCounts)
        {
            if (kv.Value > 0)
                return kv.Value;
        }

        return 0;
    }
}
