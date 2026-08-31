using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// When the PLC path owns open-bundle accumulation and close, Input Slit CSV rows are traceability +
/// recon compare only — they must not add to <see cref="INdtBundleRuntimeStateStore"/> sizeCounts
/// or MW56.
/// </summary>
public static class PlcOpenCsvIngestPolicy
{
    /// <summary>
    /// True when CSV ingestion must not call <c>IncrementSizeCount</c> / <c>ProcessSlitRecord</c>.
    /// Closed bundles awaiting recon attach use a separate FIFO path and are not covered here.
    /// </summary>
    public static bool ShouldIngestTraceabilityOnly(BundleCloseTrigger trigger, bool plcPathHealthy) =>
        BundleClosePolicy.AllowPlcClose(trigger, plcPathHealthy);
}
