using NdtBundleService.Services.PoLifecycle;

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

/// <summary>Pure helpers for Option A: stamp advance + PLC claim of open fill targets.</summary>
public static class CsvFillSequenceAdvance
{
    public static bool IsTerminalForAdvance(string? fillState) =>
        fillState is CsvFillState.CsvComplete or CsvFillState.CsvOvershoot or CsvFillState.CsvShort;

    /// <summary>
    /// After PLC PO-end Immediate print: release the remainder from open-fill so the next PO
    /// can form without waiting for late Input Slit CSVs. Under-filled remainders become
    /// <see cref="CsvFillState.CsvShort"/>; exact/over become Complete/Overshoot.
    /// </summary>
    public static string ResolveForceReleaseFillState(int targetNdtPcs, int csvFilled)
    {
        if (targetNdtPcs < 0)
            throw new ArgumentOutOfRangeException(nameof(targetNdtPcs));
        if (csvFilled < 0)
            throw new ArgumentOutOfRangeException(nameof(csvFilled));

        // Overshoot only after a formed total (> 0) is exceeded by slit fill.
        if (targetNdtPcs > 0 && csvFilled > targetNdtPcs)
            return CsvFillState.CsvOvershoot;
        if (csvFilled == targetNdtPcs && targetNdtPcs > 0)
            return CsvFillState.CsvComplete;
        if (targetNdtPcs == 0)
            return csvFilled == 0 ? CsvFillState.PlcClosed : CsvFillState.CsvFilling;
        // Printed remainder with missing/late CSVs (including 1-slit month-end remainders).
        return CsvFillState.CsvShort;
    }

    /// <summary>
    /// Manual reconcile: when operator total matches slit-grid sum, treat fill as complete even if
    /// <c>Csv_Filled</c> lagged (slit edits / late stamps).
    /// </summary>
    public static (int CsvFilled, string FillState) ResolveManualReconcileFill(
        int correctedTotal,
        int csvFilled,
        int slitSum)
    {
        if (correctedTotal < 0)
            throw new ArgumentOutOfRangeException(nameof(correctedTotal));
        if (csvFilled < 0)
            throw new ArgumentOutOfRangeException(nameof(csvFilled));
        if (slitSum < 0)
            throw new ArgumentOutOfRangeException(nameof(slitSum));

        // Overshoot only when a formed corrected total (> 0) is exceeded.
        if (correctedTotal > 0 && csvFilled > correctedTotal)
            return (csvFilled, CsvFillState.CsvOvershoot);

        if (slitSum >= correctedTotal && correctedTotal > 0)
            return (Math.Max(csvFilled, correctedTotal), CsvFillState.CsvComplete);

        if (csvFilled == correctedTotal && correctedTotal > 0)
            return (csvFilled, CsvFillState.CsvComplete);

        if (csvFilled > 0)
            return (csvFilled, CsvFillState.CsvFilling);

        return (csvFilled, CsvFillState.PlcClosed);
    }

    /// <summary>
    /// Open incomplete fill parents that PLC close may claim instead of allocating a new Bundle_No
    /// (CsvAdvance stamp targets, operator-seeded forming rows, placeholder parents).
    /// </summary>
    public static bool IsClaimableOpenFillState(string? fillState) =>
        fillState is CsvFillState.PlcClosed or CsvFillState.CsvFilling;

    /// <summary>
    /// Prefer genuine CSV-advance stamp targets when several open parents exist for the same PO/mill.
    /// Lower value = higher priority.
    /// </summary>
    public static int ClaimCloseSourcePriority(string? closeSource) =>
        string.Equals(closeSource, BundleCloseSource.CsvAdvance, StringComparison.OrdinalIgnoreCase)
            ? 0
            : 1;

    /// <summary>
    /// Open the next Mill_Sequence stamp target only when the just-stamped bundle is terminal
    /// and no other incomplete fill slot already exists for that PO/mill.
    /// </summary>
    public static bool ShouldOpenNextStampTarget(string? fillStateAfterStamp, bool anotherIncompleteExists) =>
        IsTerminalForAdvance(fillStateAfterStamp) && !anotherIncompleteExists;

    /// <summary>
    /// Open a CsvAdvance stamp only while this PO is still the mill's live run.
    /// Finished / previously-ran POs (lifecycle Closed/Draining, post-PO-end WIP wait, or mill already
    /// on a different WIP PO) must not seed a phantom forming batch.
    /// Used after Manual Reconcile and after CSV fill Complete/Overshoot/Short.
    /// </summary>
    public static bool IsEligiblePoForCsvAdvance(
        PoLifecyclePhase phase,
        string? bundlePo,
        string? runningPoForMill,
        bool millWaitingForNewWipAfterPoEnd)
    {
        if (phase is PoLifecyclePhase.Closed or PoLifecyclePhase.Draining)
            return false;
        if (millWaitingForNewWipAfterPoEnd)
            return false;

        var bundle = InputSlitCsvParsing.NormalizePo(bundlePo);
        if (string.IsNullOrWhiteSpace(bundle))
            return false;

        var running = InputSlitCsvParsing.NormalizePo(runningPoForMill);
        // Unknown running PO: allow (mid-PO when WIP probe briefly fails).
        if (string.IsNullOrWhiteSpace(running))
            return true;

        return string.Equals(bundle, running, StringComparison.OrdinalIgnoreCase);
    }

    /// <inheritdoc cref="IsEligiblePoForCsvAdvance"/>
    public static bool IsEligiblePoForCsvAdvanceAfterManualReconcile(
        PoLifecyclePhase phase,
        string? bundlePo,
        string? runningPoForMill,
        bool millWaitingForNewWipAfterPoEnd) =>
        IsEligiblePoForCsvAdvance(phase, bundlePo, runningPoForMill, millWaitingForNewWipAfterPoEnd);

    /// <summary>
    /// Empty incomplete <c>CsvAdvance</c> stamp for a different PO — safe to void when a new running PO opens.
    /// </summary>
    public static bool IsEmptyOrphanCsvAdvanceStamp(
        string? closeSource,
        string? fillState,
        int csvFilled,
        bool voided,
        string? stampPo,
        string? keepPo)
    {
        if (voided || csvFilled != 0)
            return false;
        if (!string.Equals(closeSource, BundleCloseSource.CsvAdvance, StringComparison.OrdinalIgnoreCase))
            return false;
        // Include CsvShort: PO-end drain can force-short an empty CsvAdvance stamp; that phantom
        // must still be voidable when a new running PO opens (do not leave dual-forming holes).
        if (fillState is not (CsvFillState.PlcClosed or CsvFillState.CsvFilling or CsvFillState.CsvShort))
            return false;

        var stamp = InputSlitCsvParsing.NormalizePo(stampPo);
        var keep = InputSlitCsvParsing.NormalizePo(keepPo);
        if (string.IsNullOrWhiteSpace(stamp) || string.IsNullOrWhiteSpace(keep))
            return false;

        return !string.Equals(stamp, keep, StringComparison.OrdinalIgnoreCase);
    }

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

    /// <summary>
    /// After claiming Bundle_No sequence N, Mill_Sequence must be at least N so the next allocate is N+1.
    /// </summary>
    public static bool ShouldRaiseMillSequenceToClaimed(int currentSequence, int claimedSequence) =>
        claimedSequence > 0 && currentSequence < claimedSequence;
}
