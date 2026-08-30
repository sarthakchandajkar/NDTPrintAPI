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
    /// capacity, the file still stamps wholly onto the current bundle (overshoot).
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
        string state;
        var discrepancy = false;

        if (after < targetNdtPcs)
        {
            state = CsvFillState.CsvFilling;
        }
        else if (after == targetNdtPcs)
        {
            state = CsvFillState.CsvComplete;
        }
        else
        {
            state = CsvFillState.CsvOvershoot;
            discrepancy = true;
        }

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
        string state;
        var discrepancy = false;

        if (after < targetNdtPcs)
            state = after == 0 ? CsvFillState.PlcClosed : CsvFillState.CsvFilling;
        else if (after == targetNdtPcs)
            state = CsvFillState.CsvComplete;
        else
        {
            state = CsvFillState.CsvOvershoot;
            discrepancy = true;
        }

        if (after != targetNdtPcs && state is CsvFillState.CsvComplete)
            discrepancy = true;

        var escalate = ShouldEscalateManualReview(targetNdtPcs, after, discrepancyReviewThresholdPercent)
                       || (discrepancy && after != targetNdtPcs && state == CsvFillState.CsvOvershoot);
        return (after, state, discrepancy || after != targetNdtPcs && state == CsvFillState.CsvOvershoot, escalate);
    }

    /// <summary>
    /// Operator reconcile revising <paramref name="newTarget"/> while <paramref name="csvFilled"/> is already stamped.
    /// Cases: (a) filled=0 → PlcClosed; (b) filled ≤ target → Filling/Complete; (c) filled &gt; target → Overshoot.
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

        string state;
        var discrepancy = false;

        if (csvFilled > newTarget)
        {
            state = CsvFillState.CsvOvershoot;
            discrepancy = true;
        }
        else if (csvFilled == newTarget)
        {
            state = csvFilled == 0 ? CsvFillState.PlcClosed : CsvFillState.CsvComplete;
        }
        else if (csvFilled > 0)
        {
            state = CsvFillState.CsvFilling;
        }
        else
        {
            state = CsvFillState.PlcClosed;
        }

        var escalate = discrepancy
                       || ShouldEscalateManualReview(newTarget, csvFilled, discrepancyReviewThresholdPercent);
        return (state, discrepancy, escalate);
    }
}
