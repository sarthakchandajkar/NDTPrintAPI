namespace NdtBundleService.Services;

/// <summary>SQL <c>NDT_Bundle.Csv_Fill_State</c> values (CHECK constraint).</summary>
public static class CsvFillState
{
    public const string PlcClosed = "PlcClosed";
    public const string CsvFilling = "CsvFilling";
    public const string CsvComplete = "CsvComplete";
    public const string CsvShort = "CsvShort";
    public const string CsvOvershoot = "CsvOvershoot";
    public const string Voided = "Voided";

    public static bool IsIncomplete(string? state) =>
        state is PlcClosed or CsvFilling;

    public static bool IsTerminal(string? state) =>
        state is CsvComplete or CsvShort or CsvOvershoot or Voided;
}

/// <summary>Outcome of assigning one whole slit file to a fill-to-target bundle.</summary>
public sealed record CsvFillStampResult(
    string BundleNo,
    int TargetNdtPcs,
    int CsvFilledBefore,
    int CsvFilledAfter,
    string FillState,
    bool CountDiscrepancy,
    bool ManualReviewEscalated);

/// <summary>Pure fill-to-target arithmetic (no I/O).</summary>
public static class CsvFillLogic
{
    /// <summary>
    /// Whole-file assignment: never split. If <paramref name="fileNdtPipes"/> exceeds remaining
    /// capacity against a <em>formed</em> target (<paramref name="targetNdtPcs"/> &gt; 0), the file
    /// still stamps wholly onto the current bundle (overshoot).
    /// Target 0 means the bundle is not yet formed — slits may accumulate as <see cref="CsvFillState.CsvFilling"/>
    /// but never as overshoot (overshoot = slit sum &gt; formed total).
    /// </summary>
    public static CsvFillStampResult ComputeAfterStamp(
        string bundleNo,
        int targetNdtPcs,
        int csvFilledBefore,
        int fileNdtPipes,
        int discrepancyReviewThresholdPercent)
    {
        if (fileNdtPipes < 0)
            throw new ArgumentOutOfRangeException(nameof(fileNdtPipes));
        if (csvFilledBefore < 0)
            throw new ArgumentOutOfRangeException(nameof(csvFilledBefore));
        if (targetNdtPcs < 0)
            throw new ArgumentOutOfRangeException(nameof(targetNdtPcs));

        var after = csvFilledBefore + fileNdtPipes;
        var (state, discrepancy) = ResolveFillState(targetNdtPcs, after);

        var escalate = ShouldEscalateManualReview(targetNdtPcs, after, discrepancyReviewThresholdPercent);
        return new CsvFillStampResult(
            bundleNo,
            targetNdtPcs,
            csvFilledBefore,
            after,
            state,
            discrepancy,
            escalate);
    }

    /// <summary>
    /// Maps filled vs formed target. Overshoot only when <paramref name="targetNdtPcs"/> &gt; 0
    /// and <paramref name="csvFilled"/> &gt; target.
    /// </summary>
    public static (string State, bool CountDiscrepancy) ResolveFillState(int targetNdtPcs, int csvFilled)
    {
        if (targetNdtPcs < 0)
            throw new ArgumentOutOfRangeException(nameof(targetNdtPcs));
        if (csvFilled < 0)
            throw new ArgumentOutOfRangeException(nameof(csvFilled));

        // Not formed yet — do not treat any positive slit sum as overshoot against 0.
        if (targetNdtPcs == 0)
        {
            return csvFilled == 0
                ? (CsvFillState.PlcClosed, false)
                : (CsvFillState.CsvFilling, false);
        }

        if (csvFilled < targetNdtPcs)
            return (csvFilled == 0 ? CsvFillState.PlcClosed : CsvFillState.CsvFilling, false);

        if (csvFilled == targetNdtPcs)
            return (CsvFillState.CsvComplete, false);

        return (CsvFillState.CsvOvershoot, true);
    }

    public static bool ShouldEscalateManualReview(
        int targetNdtPcs,
        int csvFilled,
        int thresholdPercent)
    {
        if (targetNdtPcs <= 0 || thresholdPercent < 0)
            return false;

        var delta = Math.Abs(csvFilled - targetNdtPcs);
        var pct = delta * 100.0 / targetNdtPcs;
        return pct > thresholdPercent;
    }

    /// <summary>Quiet / PO-end escape when files never arrive: mark short and advance.</summary>
    public static (string State, bool CountDiscrepancy, bool ManualReview) ComputeQuietShort(
        int targetNdtPcs,
        int csvFilled,
        int discrepancyReviewThresholdPercent)
    {
        var discrepancy = csvFilled != targetNdtPcs;
        var escalate = ShouldEscalateManualReview(targetNdtPcs, csvFilled, discrepancyReviewThresholdPercent);
        return (CsvFillState.CsvShort, discrepancy, escalate);
    }

    /// <summary>
    /// Adjust filled by delta for same-basename count revision. Clamps at 0.
    /// Recalculates terminal/incomplete state against target.
    /// </summary>
    public static (int CsvFilled, string State, bool CountDiscrepancy, bool ManualReview) ApplyFilledDelta(
        int targetNdtPcs,
        int csvFilledBefore,
        int delta,
        int discrepancyReviewThresholdPercent)
    {
        var after = Math.Max(0, csvFilledBefore + delta);
        var (state, discrepancy) = ResolveFillState(targetNdtPcs, after);

        var escalate = ShouldEscalateManualReview(targetNdtPcs, after, discrepancyReviewThresholdPercent)
                       || state == CsvFillState.CsvOvershoot;
        return (after, state, discrepancy, escalate);
    }

    /// <summary>
    /// Operator reconcile revising <paramref name="newTarget"/> while <paramref name="csvFilled"/> is already stamped.
    /// Cases: (a) filled=0 → PlcClosed; (b) filled ≤ formed target → Filling/Complete;
    /// (c) filled &gt; formed target (&gt; 0) → Overshoot. Target 0 never overshoots.
    /// </summary>
    public static (string State, bool CountDiscrepancy, bool ManualReview) ComputeAfterTargetRevision(
        int newTarget,
        int csvFilled,
        int discrepancyReviewThresholdPercent)
    {
        if (newTarget < 0)
            throw new ArgumentOutOfRangeException(nameof(newTarget));
        if (csvFilled < 0)
            throw new ArgumentOutOfRangeException(nameof(csvFilled));

        var (state, discrepancy) = ResolveFillState(newTarget, csvFilled);
        var escalate = discrepancy
                       || ShouldEscalateManualReview(newTarget, csvFilled, discrepancyReviewThresholdPercent);
        return (state, discrepancy, escalate);
    }
}
