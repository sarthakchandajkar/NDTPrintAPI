using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>Result of resolving which existing bundle should receive a Closed-PO traceability slit row.</summary>
public sealed record ClosedPoTraceabilityRoute(string? BatchNoFormatted, bool RequiresManualReview);

/// <summary>
/// Attaches Closed-PO late slit rows to an incomplete fill-to-target bundle without opening a new sequence.
/// Soft "best existing printed bundle" attach is retired — if no incomplete target exists, Manual_Review.
/// </summary>
public static class ClosedPoTraceabilityBatchResolver
{
    public static async Task<ClosedPoTraceabilityRoute> ResolveAsync(
        ICsvFillService csvFill,
        InputSlitRecord record,
        int millNo,
        string? pipeSize,
        CancellationToken cancellationToken)
    {
        var incomplete = await csvFill
            .TryGetOldestIncompleteAsync(record.PoNumber, millNo, pipeSize, cancellationToken)
            .ConfigureAwait(false);

        if (incomplete is not null)
            return new ClosedPoTraceabilityRoute(incomplete.BundleNo, RequiresManualReview: false);

        return new ClosedPoTraceabilityRoute(null, RequiresManualReview: true);
    }
}
