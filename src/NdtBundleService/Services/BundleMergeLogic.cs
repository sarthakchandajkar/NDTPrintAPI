namespace NdtBundleService.Services;

/// <summary>Pure merge eligibility, fill recompute, tombstone, and reference-rewrite order.</summary>
public static class BundleMergeLogic
{
    public const int LargeSequenceJumpWarn = 100;

    /// <summary>
    /// Ordered SQL mutations that must run before renaming <c>NDT_Bundle.Bundle_No</c> to the tombstone.
    /// <see cref="FkChildTables"/> first (especially Output_Slit_Row — FK_Output_Slit_Row_Bundle),
    /// then non-FK tables that would otherwise attach to a reused live number.
    /// </summary>
    public static readonly string[] ReferenceRewriteTables =
    [
        "Output_Slit_Row",
        "Manual_Station_Run",
        "NDT_Process_Consolidated",
        "Upload_Bundle_Row",
        "Pipeline_Event",
        "Ppc_Correction_Item"
    ];

    public static string Tombstone(string liveBundleNo) => NdtBundleSequence.ToTombstone(liveBundleNo);

    public static bool TryGetPreviousLive(
        IReadOnlyList<MergeCandidateBundle> liveSamePoMill,
        string sourceBundleNo,
        int millNo,
        out MergeCandidateBundle previous)
    {
        previous = default!;
        if (!NdtBundleSequence.TryParseSequenceForCurrentYear(sourceBundleNo, millNo, out var sourceSeq))
            return false;

        MergeCandidateBundle? best = null;
        var bestSeq = int.MinValue;
        foreach (var b in liveSamePoMill)
        {
            if (b.Voided)
                continue;
            if (string.Equals(b.BundleNo, sourceBundleNo, StringComparison.OrdinalIgnoreCase))
                continue;
            if (!NdtBundleSequence.TryParseSequenceForCurrentYear(b.BundleNo, millNo, out var seq))
                continue;
            if (seq >= sourceSeq || seq <= bestSeq)
                continue;
            bestSeq = seq;
            best = b;
        }

        if (best is null)
            return false;
        previous = best;
        return true;
    }

    public static (int Total, int Target, int Filled, string FillState) ComputeTargetFillAfterMerge(
        int targetTotal,
        int targetTargetPcs,
        int targetFilled,
        int sourceTotal,
        int sourceFilled,
        int discrepancyThresholdPercent)
    {
        var total = targetTotal + sourceTotal;
        var targetPcs = targetTargetPcs + sourceTotal;
        var (filled, state, _, _) = CsvFillLogic.ApplyFilledDelta(
            targetPcs,
            targetFilled,
            sourceFilled,
            discrepancyThresholdPercent);
        return (total, targetPcs, filled, state);
    }

    public static bool ShouldRollbackSequence(int sourceSequence, int millCurrentSequence, int liveMaxExcludingSource) =>
        millCurrentSequence == sourceSequence && liveMaxExcludingSource <= sourceSequence;

    public static string SequenceMessage(
        string sourceBundleNo,
        int millNo,
        bool rolledBack,
        int millCurrentAfter)
    {
        var next = NdtBundleSequence.Format(millCurrentAfter + 1, millNo);
        if (rolledBack)
            return $"Next bundle reuses {sourceBundleNo}.";
        return $"{sourceBundleNo} will remain a gap; next bundle is {next}.";
    }
}

public sealed record MergeCandidateBundle(
    string BundleNo,
    string PoNumber,
    int MillNo,
    int TotalNdtPcs,
    int TargetNdtPcs,
    int CsvFilled,
    string CsvFillState,
    bool Voided);
