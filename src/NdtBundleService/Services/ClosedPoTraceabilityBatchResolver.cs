using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>Result of resolving which existing bundle should receive a Closed-PO traceability slit row.</summary>
public sealed record ClosedPoTraceabilityRoute(string? BatchNoFormatted, bool RequiresManualReview);

/// <summary>
/// Attaches Closed-PO late slit rows to an existing bundle (FIFO recon, SQL lookup) without opening a new sequence.
/// </summary>
public static class ClosedPoTraceabilityBatchResolver
{
    public static async Task<ClosedPoTraceabilityRoute> ResolveAsync(
        INdtBundleRepository bundleRepository,
        IList<PlcCsvReconAwaitingBundle> awaitingList,
        InputSlitRecord record,
        int millNo,
        string? pipeSize,
        CancellationToken cancellationToken)
    {
        if (awaitingList.Count > 0
            && PlcCsvReconFifo.TryAttachRow(awaitingList, record, out var attachedBatchNo))
        {
            return new ClosedPoTraceabilityRoute(attachedBatchNo, RequiresManualReview: false);
        }

        var existing = await bundleRepository
            .TryFindTraceabilityBundleForPoMillAsync(record.PoNumber, millNo, pipeSize, cancellationToken)
            .ConfigureAwait(false);

        if (!string.IsNullOrWhiteSpace(existing))
            return new ClosedPoTraceabilityRoute(existing, RequiresManualReview: false);

        return new ClosedPoTraceabilityRoute(null, RequiresManualReview: true);
    }
}
