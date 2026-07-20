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
        var sizeKey = FormationChartLookup.NormalizePipeSizeKey(pipeSize);
        if (string.IsNullOrEmpty(sizeKey))
            sizeKey = "Default";

        var sizeCounts = runtimeState.GetSizeCounts(poNumber, millNo);
        if (sizeCounts.TryGetValue(sizeKey, out var fromSize) && fromSize > 0)
            return fromSize;

        // Any positive size key (PO may have been tracked under a different normalized size).
        foreach (var kv in sizeCounts)
        {
            if (kv.Value > 0)
                return kv.Value;
        }

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
}
