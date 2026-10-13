using NdtBundleService.Configuration;

namespace NdtBundleService.Services.PlcHandshake;

/// <summary>
/// Pure SAP-PO resolution for Manual Confirm (arm snapshot + confirm-time live sources).
/// </summary>
public static class PlcPoEndManualConfirmPoResolution
{
    public const string SourcePlcPoId = "PlcPoId";
    public const string SourceRunningPoAtEdge = "RunningPoAtEdge";
    public const string SourceLiveRunningPo = "LiveRunningPo";
    public const string SourceEndedPoAtEdge = "EndedPoAtEdge";
    public const string SourceLiveEndedPo = "LiveEndedPo";
    public const string SourceActiveOrSlitPo = "ActiveOrSlitPo";

    /// <summary>
    /// Order: plausible PLC PO_Id → running at edge → live running → (while waiting) ended PO
    /// snapshot then live ended → active/slit PO.
    /// </summary>
    public static string? TryResolve(
        int poIdAtEdge,
        string? runningPoAtEdge,
        string? liveRunningPo,
        bool waitingForNewWip,
        string? endedPoAtEdge,
        string? liveEndedPo,
        string? activeOrSlitPo,
        PlcPoEndOptions? plcCfg,
        out string? source)
    {
        plcCfg ??= new PlcPoEndOptions();

        if (PlcPoNumberResolution.TryResolveFromPlcPoId(poIdAtEdge, plcCfg, out var fromPlc))
        {
            source = SourcePlcPoId;
            return fromPlc;
        }

        var atEdge = Normalize(runningPoAtEdge);
        if (atEdge is not null)
        {
            source = SourceRunningPoAtEdge;
            return atEdge;
        }

        var liveRunning = Normalize(liveRunningPo);
        if (liveRunning is not null)
        {
            source = SourceLiveRunningPo;
            return liveRunning;
        }

        if (waitingForNewWip)
        {
            var endedSnap = Normalize(endedPoAtEdge);
            if (endedSnap is not null)
            {
                source = SourceEndedPoAtEdge;
                return endedSnap;
            }

            var endedLive = Normalize(liveEndedPo);
            if (endedLive is not null)
            {
                source = SourceLiveEndedPo;
                return endedLive;
            }

            // Do not fall through to ActivePo/Input Slit while waiting — that PO may be the *next*
            // PO's early slits (e.g. 2550 while EndedPo is still 2596).
            source = null;
            return null;
        }

        var slit = Normalize(activeOrSlitPo);
        if (slit is not null)
        {
            source = SourceActiveOrSlitPo;
            return slit;
        }

        source = null;
        return null;
    }

    private static string? Normalize(string? po)
    {
        var n = InputSlitCsvParsing.NormalizePo(po);
        return string.IsNullOrWhiteSpace(n) ? null : n;
    }
}
