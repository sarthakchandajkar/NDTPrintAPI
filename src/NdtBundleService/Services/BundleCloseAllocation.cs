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

/// <summary>
/// An open bundle whose provisional stamp was taken by another PO's close allocation.
/// Its already-stamped slit rows/CSVs must be corrected from <see cref="OldProvisional"/> to
/// <see cref="NewProvisional"/> immediately, so reconcile never shows mixed-PO rows under one number.
/// </summary>
public readonly record struct ProvisionalStampReassignment(
    string PoNumber,
    int MillNo,
    int OldProvisional,
    int NewProvisional);
