using Microsoft.Extensions.Logging;

namespace NdtBundleService.Services;

/// <summary>Compare open-bundle PLC accumulation vs cumulative CSV slit sum (log-only).</summary>
public static class PlcOpenCsvReconcile
{
    public static void LogDiscrepancyIfNeeded(
        ILogger logger,
        string poNumber,
        int millNo,
        int csvSlitSum,
        int plcAccumulated,
        string? pipeSizeKey = null)
    {
        if (csvSlitSum == plcAccumulated)
            return;

        logger.LogWarning(
            "Mill {Mill}: open PLC bundle PO {PO} size {Size}: CSV slit sum {CsvSum} ≠ PLC remainder {PlcRemainder} (CSV traceability-only; PLC leads close/MW56).",
            millNo,
            poNumber,
            pipeSizeKey ?? "Default",
            csvSlitSum,
            plcAccumulated);
    }
}
