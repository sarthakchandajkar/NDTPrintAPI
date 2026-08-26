namespace NdtBundleService.Services;

/// <summary>
/// Result of mill-wide close allocation. Final sequence is the sole number printed on the tag
/// and used for fill-to-target CSV stamping (no provisional stamp).
/// </summary>
public readonly record struct BundleCloseAllocation(int FinalSequence)
{
    /// <summary>Legacy 2-arg ctor (provisional ignored) for test fakes during cutover.</summary>
    public BundleCloseAllocation(int finalSequence, int provisionalSequenceIgnored)
        : this(finalSequence)
    {
        _ = provisionalSequenceIgnored;
    }
}
