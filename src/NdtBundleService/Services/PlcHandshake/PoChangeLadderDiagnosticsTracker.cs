using System.Globalization;

namespace NdtBundleService.Services.PlcHandshake;

/// <summary>
/// Pure change detector for PO-change ladder observation. MD232 only contributes a change
/// when it crosses the PLC threshold either way, or resets to 0.
/// </summary>
public sealed class PoChangeLadderDiagnosticsTracker
{
    private readonly float _md232Threshold;
    private bool _hasPrev;
    private PoChangeLadderSnapshot _prev;

    public PoChangeLadderDiagnosticsTracker(float md232Threshold = 30_000f)
    {
        _md232Threshold = md232Threshold;
    }

    public bool HasPrevious => _hasPrev;

    public PoChangeLadderSnapshot Previous => _prev;

    /// <summary>
    /// Returns true when a log line should be emitted. Always true for the first sample.
    /// </summary>
    public bool Observe(in PoChangeLadderSnapshot current, out string reason)
    {
        if (!_hasPrev)
        {
            _prev = current;
            _hasPrev = true;
            reason = "initial";
            return true;
        }

        var md232Event = IsMd232LogEvent(_prev.Md232, current.Md232, _md232Threshold);
        var other =
            current.Slit1PoId != _prev.Slit1PoId
            || current.Slit2PoId != _prev.Slit2PoId
            || current.ReconOn != _prev.ReconOn
            || current.ManualButtEnd != _prev.ManualButtEnd
            || current.M406 != _prev.M406
            || current.M407 != _prev.M407
            || current.Mw314 != _prev.Mw314
            || current.SlitId != _prev.SlitId
            || current.Ndt != _prev.Ndt;

        if (!other && !md232Event)
        {
            // Keep latest MD232 for next cross/reset detection without logging chatter.
            _prev = current;
            reason = string.Empty;
            return false;
        }

        reason = BuildReason(_prev, current, md232Event);
        _prev = current;
        return true;
    }

    public static bool IsMd232LogEvent(float previous, float current, float threshold)
    {
        var wasAbove = previous > threshold;
        var nowAbove = current > threshold;
        if (wasAbove != nowAbove)
            return true;

        // Reset to 0 (exact) from a non-zero value.
        return current == 0f && previous != 0f;
    }

    public static string FormatLogLine(string millName, in PoChangeLadderSnapshot s, string reason) =>
        string.Create(
            CultureInfo.InvariantCulture,
            $"{millName}: PO-change ladder diag ({reason}): Slit1.PO_ID={s.Slit1PoId} Slit2.PO_ID={s.Slit2PoId} MD232={s.Md232:0.###} ReconOn={s.ReconOn} ManualButtEnd={s.ManualButtEnd} M40.6={s.M406} M40.7={s.M407} MW314={s.Mw314} SlitId={s.SlitId} NDT={s.Ndt}");

    private static string BuildReason(
        in PoChangeLadderSnapshot prev,
        in PoChangeLadderSnapshot curr,
        bool md232Event)
    {
        var parts = new List<string>(8);
        if (curr.Slit1PoId != prev.Slit1PoId)
            parts.Add("Slit1.PO_ID");
        if (curr.Slit2PoId != prev.Slit2PoId)
            parts.Add("Slit2.PO_ID");
        if (md232Event)
        {
            if (curr.Md232 == 0f && prev.Md232 != 0f)
                parts.Add("MD232->0");
            else if (curr.Md232 > prev.Md232)
                parts.Add("MD232-cross-up");
            else
                parts.Add("MD232-cross-down");
        }

        if (curr.ReconOn != prev.ReconOn)
            parts.Add("ReconOn");
        if (curr.ManualButtEnd != prev.ManualButtEnd)
            parts.Add("ManualButtEnd");
        if (curr.M406 != prev.M406)
            parts.Add("M40.6");
        if (curr.M407 != prev.M407)
            parts.Add("M40.7");
        if (curr.Mw314 != prev.Mw314)
            parts.Add("MW314");
        if (curr.SlitId != prev.SlitId)
            parts.Add("SlitId");
        if (curr.Ndt != prev.Ndt)
            parts.Add("NDT");

        return parts.Count == 0 ? "change" : string.Join(',', parts);
    }
}

/// <summary>One poll sample of PO-change ladder + live NDT/slit signals.</summary>
public readonly record struct PoChangeLadderSnapshot(
    int Slit1PoId,
    int Slit2PoId,
    float Md232,
    bool ReconOn,
    bool ManualButtEnd,
    bool M406,
    bool M407,
    int Mw314,
    int SlitId,
    int Ndt);
