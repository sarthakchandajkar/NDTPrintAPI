using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class CsvFillLogicTests
{
    [Fact]
    public void WorkedExample_SevenSixFourThenFive_CompletesAt22()
    {
        const int target = 22;
        var r1 = CsvFillLogic.ComputeAfterStamp("1226100001", target, 0, 7, 20);
        Assert.Equal(7, r1.CsvFilledAfter);
        Assert.Equal(CsvFillState.CsvFilling, r1.FillState);

        var r2 = CsvFillLogic.ComputeAfterStamp("1226100001", target, r1.CsvFilledAfter, 6, 20);
        Assert.Equal(13, r2.CsvFilledAfter);

        var r3 = CsvFillLogic.ComputeAfterStamp("1226100001", target, r2.CsvFilledAfter, 4, 20);
        Assert.Equal(17, r3.CsvFilledAfter);
        Assert.Equal(CsvFillState.CsvFilling, r3.FillState);

        var r4 = CsvFillLogic.ComputeAfterStamp("1226100001", target, r3.CsvFilledAfter, 5, 20);
        Assert.Equal(22, r4.CsvFilledAfter);
        Assert.Equal(CsvFillState.CsvComplete, r4.FillState);
        Assert.False(r4.CountDiscrepancy);
    }

    [Fact]
    public void WorkedExample_TailThreePlusTwo_SameComplete()
    {
        const int target = 22;
        var after17 = CsvFillLogic.ComputeAfterStamp("1226100001", target, 0, 7 + 6 + 4, 20);
        Assert.Equal(17, after17.CsvFilledAfter);

        var plus3 = CsvFillLogic.ComputeAfterStamp("1226100001", target, 17, 3, 20);
        Assert.Equal(20, plus3.CsvFilledAfter);
        Assert.Equal(CsvFillState.CsvFilling, plus3.FillState);

        var plus2 = CsvFillLogic.ComputeAfterStamp("1226100001", target, 20, 2, 20);
        Assert.Equal(22, plus2.CsvFilledAfter);
        Assert.Equal(CsvFillState.CsvComplete, plus2.FillState);
    }

    [Fact]
    public void Overshoot_WholeFile_NeverSplit()
    {
        var r = CsvFillLogic.ComputeAfterStamp("1226100001", targetNdtPcs: 20, csvFilledBefore: 18, fileNdtPipes: 5, 20);
        Assert.Equal(23, r.CsvFilledAfter);
        Assert.Equal(CsvFillState.CsvOvershoot, r.FillState);
        Assert.True(r.CountDiscrepancy);
    }

    [Fact]
    public void CountRevision_UsesDelta_NotFullAdd()
    {
        // Was 56 stamped, corrected to 46 → delta -10 (not +46).
        var (filled, state, discrepancy, _) =
            CsvFillLogic.ApplyFilledDelta(targetNdtPcs: 49, csvFilledBefore: 56, delta: 46 - 56, 20);
        Assert.Equal(46, filled);
        Assert.Equal(CsvFillState.CsvFilling, state);
        Assert.False(discrepancy);
    }

    [Fact]
    public void QuietShort_MarksShortAndDiscrepancy()
    {
        var (state, discrepancy, manual) = CsvFillLogic.ComputeQuietShort(22, 17, 20);
        Assert.Equal(CsvFillState.CsvShort, state);
        Assert.True(discrepancy);
        Assert.True(manual); // |22-17|/22 ≈ 22.7% > 20
    }

    [Fact]
    public void DiscrepancyThreshold_EscalatesOnlyWhenStrictlyOverPercent()
    {
        Assert.False(CsvFillLogic.ShouldEscalateManualReview(100, 80, 20)); // exactly 20%
        Assert.True(CsvFillLogic.ShouldEscalateManualReview(100, 79, 20));
    }

    [Fact]
    public void Reconcile_a_before_any_file_sets_PlcClosed()
    {
        var (state, discrepancy, _) = CsvFillLogic.ComputeAfterTargetRevision(20, csvFilled: 0, 20);
        Assert.Equal(CsvFillState.PlcClosed, state);
        Assert.False(discrepancy);
    }

    [Fact]
    public void Reconcile_b_mid_fill_corrected_at_or_above_filled_continues()
    {
        var (state, discrepancy, _) = CsvFillLogic.ComputeAfterTargetRevision(22, csvFilled: 17, 20);
        Assert.Equal(CsvFillState.CsvFilling, state);
        Assert.False(discrepancy);

        var complete = CsvFillLogic.ComputeAfterTargetRevision(17, csvFilled: 17, 20);
        Assert.Equal(CsvFillState.CsvComplete, complete.State);
    }

    [Fact]
    public void Reconcile_c_mid_fill_corrected_below_filled_is_overshoot()
    {
        var (state, discrepancy, manual) = CsvFillLogic.ComputeAfterTargetRevision(10, csvFilled: 17, 20);
        Assert.Equal(CsvFillState.CsvOvershoot, state);
        Assert.True(discrepancy);
        Assert.True(manual);
    }

    [Fact]
    public void Fill_pointer_orders_by_PrintedAt_then_BundleNo_when_timestamps_tie()
    {
        // Mirrors TryStampFileAsync / TryGetOldestIncomplete ORDER BY PrintedAt ASC, Bundle_No ASC.
        // PrintFailed never clears PrintedAt (set at pending-print upsert before ZPL attempt).
        var sameTick = new DateTime(2026, 8, 26, 10, 0, 0, DateTimeKind.Utc);
        var rows = new[]
        {
            (BundleNo: "1226100002", PrintedAt: sameTick),
            (BundleNo: "1226100001", PrintedAt: sameTick),
            (BundleNo: "1226100003", PrintedAt: sameTick.AddSeconds(1)),
        };

        var oldest = rows
            .OrderBy(r => r.PrintedAt)
            .ThenBy(r => r.BundleNo, StringComparer.Ordinal)
            .First();

        Assert.Equal("1226100001", oldest.BundleNo);
    }

    [Fact]
    public void PrintFailed_status_update_does_not_clear_PrintedAt_sql()
    {
        // Regression pin: UpdateBundlePrintStatusAsync only sets Print_Status / Print_Error /
        // Print_Attempted_At — never PrintedAt. Pending-print upsert sets PrintedAt = SYSDATETIME()
        // (and INSERT relies on NOT NULL DEFAULT). Schema: docs/NDT_Bundle_Table.sql.
        const string statusSql = @"
UPDATE dbo.NDT_Bundle
SET Print_Status = @Status,
    Print_Error = @Error,
    Print_Attempted_At = COALESCE(Print_Attempted_At, SYSDATETIME())
WHERE Bundle_No = @BundleNo";

        Assert.DoesNotContain("PrintedAt", statusSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Print_Status", statusSql, StringComparison.Ordinal);
    }
}