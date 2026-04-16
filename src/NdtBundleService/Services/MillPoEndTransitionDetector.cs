using System.Globalization;
using Microsoft.Extensions.Logging;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Per-mill PLC-style PO end: valid PO_ID changed vs tracked previous, with first-scan and slit-valid gating.
/// One instance per application (singleton); internal state is isolated per mill index 1–4.
/// </summary>
public sealed class MillPoEndTransitionDetector
{
    private readonly MillTransitionState[] _states;

    public MillPoEndTransitionDetector(ILogger<MillPoEndTransitionDetector> logger)
    {
        _logger = logger;
        _states = new MillTransitionState[4];
        for (var i = 0; i < 4; i++)
            _states[i] = new MillTransitionState();
    }

    private readonly ILogger<MillPoEndTransitionDetector> _logger;

    /// <summary>
    /// Evaluates one mill after a fresh Modbus snapshot. Updates tracked previous PO when appropriate.
    /// </summary>
    /// <returns>The PO number to close (previous PO) when a transition fires; otherwise null.</returns>
    public string? Evaluate(
        int millNo,
        MillPoPlcSnapshot snap,
        PlcPoEndOptions options,
        MillModbusPoEndEndpoint ep)
    {
        if (millNo is < 1 or > 4)
            return null;

        if (!snap.ReadOk)
            return null;

        var st = _states[millNo - 1];

        if (!st.FirstScanComplete)
        {
            st.PrevPoId = snap.PoId;
            st.PrevPoTypeId = snap.PoTypeId;
            st.LastSlitEntryCount = snap.SlitEntryCount;
            st.FirstScanComplete = true;
            _logger.LogDebug("Mill {Mill}: PO-end PO_Id first scan; tracking PrevPoId={Prev}.", millNo, st.PrevPoId);
            return null;
        }

        if (!snap.SlitEntryValid)
        {
            _logger.LogTrace("Mill {Mill}: slit entry not valid; skipping PO transition evaluation.", millNo);
            return null;
        }

        var minId = options.MinValidPoId;
        var maxId = options.MaxValidPoId;
        var curOk = IsValidPoId(snap.PoId, minId, maxId);
        var prevOk = IsValidPoId(st.PrevPoId, minId, maxId);

        if (!curOk || !prevOk)
        {
            if (options.ResyncPrevPoWhenCurrentInvalid && !curOk && prevOk)
            {
                _logger.LogInformation(
                    "Mill {Mill}: current PO_Id {Cur} invalid; resyncing tracked previous (was {Prev}).",
                    millNo,
                    snap.PoId,
                    st.PrevPoId);
                st.PrevPoId = snap.PoId;
                st.PrevPoTypeId = snap.PoTypeId;
                st.LastSlitEntryCount = snap.SlitEntryCount;
            }

            return null;
        }

        if (snap.PoId == st.PrevPoId)
        {
            if (snap.SlitEntryCount.HasValue)
                st.LastSlitEntryCount = snap.SlitEntryCount;
            return null;
        }

        if (ep.RequireSlitEntryCountChange && ep.SlitEntryCountStartAddress.HasValue && snap.SlitEntryCount is { } cnt)
        {
            if (st.LastSlitEntryCount is { } lastCount && lastCount == cnt)
            {
                _logger.LogTrace(
                    "Mill {Mill}: PO_Id changed but slit entry count unchanged ({Count}); suppressed by RequireSlitEntryCountChange.",
                    millNo,
                    cnt);
                return null;
            }
        }

        var endedPo = FormatPoNumber(st.PrevPoId, options.PoNumberFormatFromPlc);
        endedPo = InputSlitCsvParsing.NormalizePo(endedPo);

        _logger.LogInformation(
            "Mill {Mill}: PO_Id transition detected (ended={Ended}, new={New}); running PO end workflow.",
            millNo,
            endedPo,
            snap.PoId);

        st.PrevPoId = snap.PoId;
        st.PrevPoTypeId = snap.PoTypeId;
        if (snap.SlitEntryCount.HasValue)
            st.LastSlitEntryCount = snap.SlitEntryCount;

        return endedPo;
    }

    /// <summary>Reset internal state (e.g. after service restart this is implicit via new process).</summary>
    public void ResetMill(int millNo)
    {
        if (millNo is < 1 or > 4)
            return;
        _states[millNo - 1] = new MillTransitionState();
    }

    public int? GetTrackedPrevPoId(int millNo)
    {
        if (millNo is < 1 or > 4)
            return null;
        var st = _states[millNo - 1];
        return st.FirstScanComplete ? st.PrevPoId : null;
    }

    private static bool IsValidPoId(int id, int minId, int maxId) => id >= minId && id <= maxId;

    private static string FormatPoNumber(int poId, string format)
    {
        if (string.IsNullOrWhiteSpace(format))
            return poId.ToString(CultureInfo.InvariantCulture);
        try
        {
            return string.Format(CultureInfo.InvariantCulture, format, poId);
        }
        catch (FormatException)
        {
            return poId.ToString(CultureInfo.InvariantCulture);
        }
    }

    private sealed class MillTransitionState
    {
        public bool FirstScanComplete;

        public int PrevPoId;

        public int? PrevPoTypeId;

        public int? LastSlitEntryCount;
    }
}
