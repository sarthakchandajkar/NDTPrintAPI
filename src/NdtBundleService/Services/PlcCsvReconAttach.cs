using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Late Input Slit rows for PLC-closed bundles awaiting CSV recon attach to that bundle
/// without advancing engine sizeCounts or RunningTotal (no new sequence).
/// </summary>
public static class PlcCsvReconAttach
{
    /// <summary>
    /// When <paramref name="awaiting"/> is present, stamps the closed bundle number and accumulates
    /// slit sum for recon. Caller must not call <c>GetBatchForRecord</c> / <c>ProcessSlitRecord</c>.
    /// Prefer <see cref="PlcCsvReconFifo.TryAttachRow"/> with a full awaiting list for multi-bundle FIFO fill.
    /// </summary>
    public static bool TryAttach(
        (string BundleNo, int EngineSequence, int PlcTotal)? awaiting,
        InputSlitRecord record,
        IDictionary<(string Po, int Mill), int> slitSumByPoMill,
        out string ndtBatchNoFormatted)
    {
        ndtBatchNoFormatted = string.Empty;
        if (awaiting is null)
            return false;

        var a = awaiting.Value;
        var bundles = new List<PlcCsvReconAwaitingBundle>
        {
            new(a.BundleNo, a.EngineSequence, a.PlcTotal, CurrentSlitSum: 0, DateTime.MinValue)
        };
        if (!PlcCsvReconFifo.TryAttachRow(bundles, record, out ndtBatchNoFormatted))
            return false;

        var key = (InputSlitCsvParsing.NormalizePo(record.PoNumber), record.MillNo);
        slitSumByPoMill.TryGetValue(key, out var prev);
        slitSumByPoMill[key] = prev + Math.Max(0, record.NdtPipes);
        return true;
    }
}
