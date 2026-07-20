namespace NdtBundleService.Services;

/// <summary>
/// Result of mill-wide close allocation. <see cref="ProvisionalSequence"/> is the open-row stamp
/// used before close; when it differs from <see cref="FinalSequence"/>, slit SQL/CSV must be corrected.
/// </summary>
public readonly record struct BundleCloseAllocation(int FinalSequence, int ProvisionalSequence)
{
    public bool NeedsStampCorrection =>
        ProvisionalSequence > 0 && ProvisionalSequence != FinalSequence;
}
