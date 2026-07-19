using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>F-3 WIP-wait gate decision (extracted for unit tests).</summary>
public static class SlitWipBundlingGate
{
    /// <summary>
    /// Returns true when the slit row should skip bundling because the mill is waiting for a new WIP file.
    /// Plc mills with a valid file PO bypass the gate when <paramref name="bundleSlitRowsWithFilePoDuringWipWait"/> is true.
    /// File mills always keep the historical hard-stop.
    /// </summary>
    public static bool ShouldSkipBundling(
        bool waitingForWip,
        string? runningPoFromWip,
        string? filePo,
        MillPoEndSource poEndSource,
        bool bundleSlitRowsWithFilePoDuringWipWait)
    {
        if (!waitingForWip || !string.IsNullOrWhiteSpace(runningPoFromWip))
            return false;

        var allowBundleDuringWipWait = bundleSlitRowsWithFilePoDuringWipWait
            && poEndSource == MillPoEndSource.Plc
            && !string.IsNullOrWhiteSpace(filePo);

        return !allowBundleDuringWipWait;
    }
}
