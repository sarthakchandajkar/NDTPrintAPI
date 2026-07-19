using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.PoLifecycle;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class Phase2BackfillTests
{
    [Theory]
    [InlineData(null, true)] // legacy NULL = imported at any version
    [InlineData("2026-07-14T10:00:00Z", true)] // stored >= file
    [InlineData("2026-07-14T09:00:00Z", false)] // stored older than file → re-ingest
    public void VersionCheck_legacy_null_and_timestamp_rules(string? storedIso, bool expectedImported)
    {
        var fileLw = DateTime.Parse("2026-07-14T10:00:00Z", null, System.Globalization.DateTimeStyles.RoundtripKind);
        DateTime? stored = storedIso is null
            ? null
            : DateTime.Parse(storedIso, null, System.Globalization.DateTimeStyles.RoundtripKind);

        Assert.Equal(expectedImported, InputSlitBackfillPolicy.IsStoredVersionSufficient(stored, fileLw));
    }

    [Fact]
    public void Decide_exact_match_is_traceability_only_even_for_running()
    {
        var action = InputSlitBackfillPolicy.Decide(
            BackfillCoverageKind.ExactMatch,
            PoLifecyclePhase.Running,
            MillPoEndSource.Plc,
            autoCloseOrphanBundles: true);
        Assert.Equal(BackfillBundlingAction.TraceabilityOnly, action);
    }

    [Fact]
    public void Decide_ambiguous_is_manual_review_no_print()
    {
        var action = InputSlitBackfillPolicy.Decide(
            BackfillCoverageKind.Ambiguous,
            PoLifecyclePhase.Running,
            MillPoEndSource.Plc,
            autoCloseOrphanBundles: true);
        Assert.Equal(BackfillBundlingAction.ManualReview, action);
    }

    [Fact]
    public void Decide_closed_with_auto_close_is_orphan_auto_close()
    {
        var action = InputSlitBackfillPolicy.Decide(
            BackfillCoverageKind.None,
            PoLifecyclePhase.Closed,
            MillPoEndSource.Plc,
            autoCloseOrphanBundles: true);
        Assert.Equal(BackfillBundlingAction.OrphanAutoClose, action);
    }

    [Fact]
    public void Decide_closed_without_auto_close_is_manual_review()
    {
        var action = InputSlitBackfillPolicy.Decide(
            BackfillCoverageKind.None,
            PoLifecyclePhase.Closed,
            MillPoEndSource.Plc,
            autoCloseOrphanBundles: false);
        Assert.Equal(BackfillBundlingAction.ManualReview, action);
    }

    [Fact]
    public void Decide_running_none_is_normal_bundle()
    {
        var action = InputSlitBackfillPolicy.Decide(
            BackfillCoverageKind.None,
            PoLifecyclePhase.Running,
            MillPoEndSource.Plc,
            autoCloseOrphanBundles: true);
        Assert.Equal(BackfillBundlingAction.NormalBundle, action);
    }

    [Fact]
    public void Decide_draining_none_is_normal_bundle()
    {
        var action = InputSlitBackfillPolicy.Decide(
            BackfillCoverageKind.None,
            PoLifecyclePhase.Draining,
            MillPoEndSource.Plc,
            autoCloseOrphanBundles: true);
        Assert.Equal(BackfillBundlingAction.NormalBundle, action);
    }

    [Fact]
    public void Decide_file_mill_ambiguous_still_normal_bundle()
    {
        // Mill-4 PoEndSource=File: do not apply Plc Manual_Review path
        var action = InputSlitBackfillPolicy.Decide(
            BackfillCoverageKind.Ambiguous,
            PoLifecyclePhase.Running,
            MillPoEndSource.File,
            autoCloseOrphanBundles: true);
        Assert.Equal(BackfillBundlingAction.NormalBundle, action);
    }

    [Fact]
    public void Decide_file_mill_exact_match_is_traceability_only()
    {
        var action = InputSlitBackfillPolicy.Decide(
            BackfillCoverageKind.ExactMatch,
            PoLifecyclePhase.Running,
            MillPoEndSource.File,
            autoCloseOrphanBundles: true);
        Assert.Equal(BackfillBundlingAction.TraceabilityOnly, action);
    }

    [Fact]
    public void Coverage_exact_match_when_per_slit_output_has_batch_no()
    {
        var root = Path.Combine(Path.GetTempPath(), "ndt-backfill-" + Guid.NewGuid().ToString("N"));
        var inbox = Path.Combine(root, "inbox");
        var output = Path.Combine(root, "ndt-out");
        Directory.CreateDirectory(inbox);
        Directory.CreateDirectory(output);

        try
        {
            var name = "2510117_01_260714_1000060163.csv";
            var source = Path.Combine(inbox, name);
            File.WriteAllText(source, "PO Number,Slit No,NDT Pipes,Mill No\n1000060163,01,10,1\n");

            var existingOut = Path.Combine(output, name);
            File.WriteAllText(
                existingOut,
                "PO Number,Slit No,NDT Pipes,Mill No,NDT Batch No\n1000060163,01,10,1,1226100001\n");

            var opts = new NdtBundleOptions
            {
                OutputBundleFolder = output,
                BundleSummaryOutputFolder = Path.Combine(root, "bundles")
            };
            Directory.CreateDirectory(opts.BundleSummaryOutputFolder);

            var rows = new List<InputSlitRecord>
            {
                new() { PoNumber = "1000060163", MillNo = 1, SlitNo = "01", NdtPipes = 10 }
            };

            Assert.Equal(
                BackfillCoverageKind.ExactMatch,
                InputSlitBackfillCoverage.Evaluate(source, rows, opts));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { /* best-effort */ }
        }
    }

    [Fact]
    public void Coverage_ambiguous_when_summary_bundle_exists_for_po_mill()
    {
        var root = Path.Combine(Path.GetTempPath(), "ndt-backfill-" + Guid.NewGuid().ToString("N"));
        var inbox = Path.Combine(root, "inbox");
        var output = Path.Combine(root, "ndt-out");
        var bundles = Path.Combine(root, "bundles");
        Directory.CreateDirectory(inbox);
        Directory.CreateDirectory(output);
        Directory.CreateDirectory(bundles);

        try
        {
            var name = "old_prestart_1000060163.csv";
            var source = Path.Combine(inbox, name);
            File.WriteAllText(source, "PO Number,Slit No,NDT Pipes,Mill No\n1000060163,02,8,1\n");

            // No matching per-slit output — but a printed summary for the same PO+mill exists (E4 class).
            File.WriteAllText(
                Path.Combine(bundles, "NDT_Bundle_1226100001.csv"),
                "PO Number,Slit No,NDT Pipes,Mill No,NDT Batch No\n1000060163,01,11,1,1226100001\n");

            var opts = new NdtBundleOptions
            {
                OutputBundleFolder = output,
                BundleSummaryOutputFolder = bundles
            };
            var rows = new List<InputSlitRecord>
            {
                new() { PoNumber = "1000060163", MillNo = 1, SlitNo = "02", NdtPipes = 8 }
            };

            Assert.Equal(
                BackfillCoverageKind.Ambiguous,
                InputSlitBackfillCoverage.Evaluate(source, rows, opts));

            // E4 remediation path: Ambiguous → Manual_Review, never auto-print
            Assert.Equal(
                BackfillBundlingAction.ManualReview,
                InputSlitBackfillPolicy.Decide(
                    BackfillCoverageKind.Ambiguous,
                    PoLifecyclePhase.Running,
                    MillPoEndSource.Plc,
                    autoCloseOrphanBundles: true));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { /* best-effort */ }
        }
    }

    [Fact]
    public void Coverage_none_when_no_artifacts()
    {
        var root = Path.Combine(Path.GetTempPath(), "ndt-backfill-" + Guid.NewGuid().ToString("N"));
        var inbox = Path.Combine(root, "inbox");
        var output = Path.Combine(root, "ndt-out");
        var bundles = Path.Combine(root, "bundles");
        Directory.CreateDirectory(inbox);
        Directory.CreateDirectory(output);
        Directory.CreateDirectory(bundles);

        try
        {
            var source = Path.Combine(inbox, "new_file.csv");
            File.WriteAllText(source, "PO Number,Slit No,NDT Pipes,Mill No\n1000059986,01,5,1\n");

            var opts = new NdtBundleOptions
            {
                OutputBundleFolder = output,
                BundleSummaryOutputFolder = bundles
            };
            var rows = new List<InputSlitRecord>
            {
                new() { PoNumber = "1000059986", MillNo = 1, SlitNo = "01", NdtPipes = 5 }
            };

            Assert.Equal(
                BackfillCoverageKind.None,
                InputSlitBackfillCoverage.Evaluate(source, rows, opts));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { /* best-effort */ }
        }
    }

    /// <summary>
    /// Incident E4 replay: 16 pre-start files for an ended PO with existing tags on disk must go Manual_Review, not print.
    /// </summary>
    [Fact]
    public void IncidentReplay_E4_prestart_files_with_existing_tags_manual_review()
    {
        // Simulate: fresh DB (no Input_Slit_Row) but NDT_Bundle_*.csv already on disk for the PO.
        var coverage = BackfillCoverageKind.Ambiguous;
        var action = InputSlitBackfillPolicy.Decide(
            coverage,
            PoLifecyclePhase.Running, // lifecycle unknown after fresh deploy
            MillPoEndSource.Plc,
            autoCloseOrphanBundles: true);

        Assert.Equal(BackfillBundlingAction.ManualReview, action);
        Assert.NotEqual(BackfillBundlingAction.NormalBundle, action);
        Assert.NotEqual(BackfillBundlingAction.OrphanAutoClose, action);
    }
}
