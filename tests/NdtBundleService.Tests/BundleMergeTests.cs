using NdtBundleService.Configuration;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class BundleMergeTests
{
    [Fact]
    public void TryGetPreviousLive_returns_immediately_previous_same_po_mill()
    {
        var a = Live(1);
        var b = Live(2);
        var c = Live(3);
        Assert.True(BundleMergeLogic.TryGetPreviousLive([a, b, c], c.BundleNo, 1, out var prev));
        Assert.Equal(b.BundleNo, prev.BundleNo);
    }

    [Fact]
    public void TryGetPreviousLive_rejects_when_no_earlier_live()
    {
        var only = Live(5);
        Assert.False(BundleMergeLogic.TryGetPreviousLive([only], only.BundleNo, 1, out _));
    }

    [Fact]
    public void TryGetPreviousLive_skips_voided_and_gaps_to_next_live()
    {
        var first = Live(1);
        var voided = Live(2) with { Voided = true };
        var source = Live(3);
        Assert.True(BundleMergeLogic.TryGetPreviousLive([first, voided, source], source.BundleNo, 1, out var prev));
        Assert.Equal(first.BundleNo, prev.BundleNo);
    }

    [Fact]
    public void ShouldRollbackSequence_when_source_is_mill_highest()
    {
        Assert.True(BundleMergeLogic.ShouldRollbackSequence(
            sourceSequence: 10, millCurrentSequence: 10, liveMaxExcludingSource: 9));
    }

    [Fact]
    public void ShouldRollbackSequence_false_when_higher_live_exists()
    {
        Assert.False(BundleMergeLogic.ShouldRollbackSequence(
            sourceSequence: 10, millCurrentSequence: 10, liveMaxExcludingSource: 11));
    }

    [Fact]
    public void SequenceMessage_gap_vs_reuse()
    {
        var source = NdtBundleSequence.Format(10, 1);
        var reuse = BundleMergeLogic.SequenceMessage(source, 1, rolledBack: true, millCurrentAfter: 9);
        Assert.Contains("reuses", reuse, StringComparison.OrdinalIgnoreCase);

        var gap = BundleMergeLogic.SequenceMessage(source, 1, rolledBack: false, millCurrentAfter: 12);
        Assert.Contains("gap", gap, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(NdtBundleSequence.Format(13, 1), gap);
    }

    [Fact]
    public void ComputeTargetFillAfterMerge_Complete_plus_Complete_is_105_of_105()
    {
        var (total, target, filled, state) = BundleMergeLogic.ComputeTargetFillAfterMerge(
            targetTotal: 50,
            targetTargetPcs: 50,
            targetFilled: 50,
            sourceTotal: 55,
            sourceFilled: 55,
            discrepancyThresholdPercent: 20);
        Assert.Equal(105, total);
        Assert.Equal(105, target);
        Assert.Equal(105, filled);
        Assert.Equal(CsvFillState.CsvComplete, state);
    }

    [Fact]
    public void ComputeTargetFillAfterMerge_incomplete_source_returns_target_CsvFilling()
    {
        var (total, target, filled, state) = BundleMergeLogic.ComputeTargetFillAfterMerge(
            targetTotal: 50,
            targetTargetPcs: 50,
            targetFilled: 50,
            sourceTotal: 20,
            sourceFilled: 10,
            discrepancyThresholdPercent: 20);
        Assert.Equal(70, total);
        Assert.Equal(70, target);
        Assert.Equal(60, filled);
        Assert.Equal(CsvFillState.CsvFilling, state);
        Assert.True(CsvFillState.IsIncomplete(state));
    }

    [Fact]
    public void ArchiveBundleArtifacts_renames_csv_and_zpl_to_tombstone()
    {
        var dir = Path.Combine(Path.GetTempPath(), "ndt-merge-archive-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var live = NdtBundleSequence.Format(2, 1);
            var tomb = NdtBundleSequence.ToTombstone(live);
            File.WriteAllText(Path.Combine(dir, NdtBundleOutputPaths.GetBundleCsvFileName(live)), "csv");
            File.WriteAllBytes(Path.Combine(dir, NdtBundleOutputPaths.GetBundleZplFileName(live)), [0x5E]);

            NdtBundleOutputPaths.ArchiveBundleArtifacts(
                new NdtBundleOptions { BundleSummaryOutputFolder = dir },
                live,
                tomb);

            Assert.False(File.Exists(Path.Combine(dir, NdtBundleOutputPaths.GetBundleCsvFileName(live))));
            Assert.False(File.Exists(Path.Combine(dir, NdtBundleOutputPaths.GetBundleZplFileName(live))));
            Assert.True(File.Exists(Path.Combine(dir, NdtBundleOutputPaths.GetBundleCsvFileName(tomb))));
            Assert.True(File.Exists(Path.Combine(dir, NdtBundleOutputPaths.GetBundleZplFileName(tomb))));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void Tombstone_sql_rewrites_child_tables_before_bundle_no_rename()
    {
        Assert.Equal("Output_Slit_Row", BundleMergeLogic.ReferenceRewriteTables[0]);
        Assert.Equal(
            new[]
            {
                "Output_Slit_Row",
                "Manual_Station_Run",
                "NDT_Process_Consolidated",
                "Upload_Bundle_Row",
                "Pipeline_Event",
                "Ppc_Correction_Item"
            },
            BundleMergeLogic.ReferenceRewriteTables);

        Assert.Contains("SET NDT_Batch_No = @Target", BundleMergeService.ReassignOutputSlitSql, StringComparison.Ordinal);
        Assert.Contains("SET NDT_Batch_No = @Target", BundleMergeService.ReassignManualStationSql, StringComparison.Ordinal);
        Assert.Contains("SET NDT_Batch_No = @Target", BundleMergeService.ReassignProcessSql, StringComparison.Ordinal);
        Assert.Contains("SET Bundle_Number = @Target", BundleMergeService.ReassignUploadSql, StringComparison.Ordinal);
        Assert.Contains("SET Bundle_No = @Tombstone", BundleMergeService.ReassignPipelineSql, StringComparison.Ordinal);
        Assert.Contains("SET NDT_Batch_No = @Tombstone", BundleMergeService.ReassignPpcSql, StringComparison.Ordinal);
        Assert.Contains("SET Bundle_No = @Tombstone", BundleMergeService.TombstoneBundleSql, StringComparison.Ordinal);
        Assert.Contains("Voided = 1", BundleMergeService.TombstoneBundleSql, StringComparison.Ordinal);
        Assert.Contains("Csv_Fill_State = N'Voided'", BundleMergeService.TombstoneBundleSql, StringComparison.Ordinal);
    }

    [Fact]
    public void ToTombstone_is_11_chars_and_not_parsed_as_live_sequence()
    {
        var live = NdtBundleSequence.Format(2, 1);
        var tomb = NdtBundleSequence.ToTombstone(live);
        Assert.Equal(live + "V", tomb);
        Assert.True(NdtBundleSequence.IsTombstone(tomb));
        Assert.False(NdtBundleSequence.TryParseSequenceForCurrentYear(tomb, 1, out _));
    }

    private static MergeCandidateBundle Live(int seq) =>
        new(
            NdtBundleSequence.Format(seq, 1),
            "PO-1",
            MillNo: 1,
            TotalNdtPcs: 10,
            TargetNdtPcs: 10,
            CsvFilled: 10,
            CsvFillState: CsvFillState.CsvComplete,
            Voided: false);
}
