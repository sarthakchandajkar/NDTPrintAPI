using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>PLC-closed bundle awaiting late CSV rows, with current slit sum for FIFO fill.</summary>
public readonly record struct PlcCsvReconAwaitingBundle(
    string BundleNo,
    int EngineSequence,
    int PlcTotal,
    int CurrentSlitSum,
    DateTime PrintedAtUtc);

/// <summary>
/// Order-preserving FIFO attribution: fill the oldest awaiting bundle until its PLC count is satisfied, then the next.
/// Surplus rows (all awaiting bundles full) return false so the engine may open a sweep partial.
/// </summary>
public static class PlcCsvReconFifo
{
    public static bool ShouldFinalize(
        int plcTotal,
        int slitSum,
        DateTime printedAtUtc,
        int reconWindowMinutes,
        DateTime utcNow) =>
        slitSum >= plcTotal
        || utcNow >= printedAtUtc.AddMinutes(Math.Max(1, reconWindowMinutes));

    /// <summary>
    /// Picks the oldest awaiting bundle that still has unfilled PLC capacity.
    /// Mutates <paramref name="bundles"/> slit sums when attach succeeds.
    /// </summary>
    public static bool TryAttachRow(
        IList<PlcCsvReconAwaitingBundle> bundles,
        InputSlitRecord record,
        out string ndtBatchNoFormatted)
    {
        ndtBatchNoFormatted = string.Empty;
        if (bundles.Count == 0)
            return false;

        var ndt = Math.Max(0, record.NdtPipes);
        if (ndt == 0)
            return false;

        for (var i = 0; i < bundles.Count; i++)
        {
            var b = bundles[i];
            if (b.CurrentSlitSum >= b.PlcTotal)
                continue;

            bundles[i] = b with { CurrentSlitSum = b.CurrentSlitSum + ndt };
            ndtBatchNoFormatted = b.BundleNo;
            return true;
        }

        return false;
    }

    public static bool HasUnfilledCapacity(IReadOnlyList<PlcCsvReconAwaitingBundle> bundles) =>
        bundles.Any(b => b.CurrentSlitSum < b.PlcTotal);
}
