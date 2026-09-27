namespace NdtBundleService.Services;

/// <summary>NDT_Bundle.Close_Source values used by fill-to-target and CSV stamp advance.</summary>
public static class BundleCloseSource
{
    public const string Plc = "Plc";
    public const string File = "File";

    /// <summary>
    /// Bundle row opened after CSV Complete/Overshoot for stamp-only continuation (no tag print yet).
    /// Next PLC close must claim this row instead of allocating a newer sequence.
    /// </summary>
    public const string CsvAdvance = "CsvAdvance";
}

/// <summary>Pure helpers for Option A: stamp advance + PLC claim of open CSV-advanced targets.</summary>
public static class CsvFillSequenceAdvance
{
    public static bool IsTerminalForAdvance(string? fillState) =>
        fillState is CsvFillState.CsvComplete or CsvFillState.CsvOvershoot;

    /// <summary>
    /// Open the next Mill_Sequence stamp target only when the just-stamped bundle is terminal
    /// and no other incomplete fill slot already exists for that PO/mill.
    /// </summary>
    public static bool ShouldOpenNextStampTarget(string? fillStateAfterStamp, bool anotherIncompleteExists) =>
        IsTerminalForAdvance(fillStateAfterStamp) && !anotherIncompleteExists;

    /// <summary>
    /// Provisional target for a CSV-advanced open bundle until PLC claims it with the real close count.
    /// Prefer the pre-reconcile original print total when Manual Reconcile shrank/grew the prior tag.
    /// </summary>
    public static int ResolveProvisionalTarget(int? manualReconOriginalTotal, int? targetNdtPcs, int totalNdtPcs)
    {
        if (manualReconOriginalTotal is > 0)
            return manualReconOriginalTotal.Value;
        if (targetNdtPcs is > 0)
            return targetNdtPcs.Value;
        return Math.Max(0, totalNdtPcs);
    }
}
