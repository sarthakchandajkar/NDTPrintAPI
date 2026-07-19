using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>Gates file vs PLC bundle close based on <see cref="BundleCloseTrigger"/> and S7 health.</summary>
public static class BundleClosePolicy
{
    /// <summary>
    /// File threshold close runs for <c>File</c>, and for <c>PlcWithFileFallback</c> when the PLC path is unhealthy.
    /// Identical to historical behavior whenever this returns true.
    /// </summary>
    public static bool AllowFileThresholdClose(BundleCloseTrigger trigger, bool plcPathHealthy) =>
        trigger switch
        {
            BundleCloseTrigger.File => true,
            BundleCloseTrigger.Plc => false,
            BundleCloseTrigger.PlcWithFileFallback => !plcPathHealthy,
            _ => true
        };

    public static bool AllowPlcClose(BundleCloseTrigger trigger, bool plcPathHealthy) =>
        trigger switch
        {
            BundleCloseTrigger.File => false,
            BundleCloseTrigger.Plc => plcPathHealthy,
            BundleCloseTrigger.PlcWithFileFallback => plcPathHealthy,
            _ => false
        };
}
