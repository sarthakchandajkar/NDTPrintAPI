using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// Phase 2 SAP-status gating: strongest-status display rule, Accepted-folder backfill coverage,
/// and the watcher folder-distinctness (never-write) invariant.
/// See docs/NDT_Input_Slit_SAP_Status_Design.md.
/// </summary>
public sealed class OutputSlitSapStatusPhase2Tests
{
    [Fact]
    public void Strongest_accepted_wins_over_everything()
    {
        Assert.Equal(
            OutputSlitSapStatus.Accepted,
            OutputSlitSapStatusPolicy.Strongest(new[]
            {
                OutputSlitSapStatus.Pending,
                OutputSlitSapStatus.Rejected,
                OutputSlitSapStatus.Accepted
            }));
    }

    [Fact]
    public void Strongest_rejected_wins_over_pending()
    {
        Assert.Equal(
            OutputSlitSapStatus.Rejected,
            OutputSlitSapStatusPolicy.Strongest(new[]
            {
                OutputSlitSapStatus.Pending,
                OutputSlitSapStatus.Rejected
            }));
    }

    [Fact]
    public void Strongest_pending_only_is_pending()
    {
        Assert.Equal(
            OutputSlitSapStatus.Pending,
            OutputSlitSapStatusPolicy.Strongest(new[] { OutputSlitSapStatus.Pending }));
    }

    [Fact]
    public void Strongest_empty_is_null()
    {
        Assert.Null(OutputSlitSapStatusPolicy.Strongest(Array.Empty<OutputSlitSapStatus>()));
    }

    /// <summary>
    /// Ingest/backfill path: an output already pulled into the SAP Accepted folder is coverage
    /// (ExactMatch → TraceabilityOnly) so the worker never re-emits or double-posts it.
    /// </summary>
    [Fact]
    public void Coverage_exact_match_when_output_is_in_sap_accepted_folder()
    {
        var root = Path.Combine(Path.GetTempPath(), "ndt-sap-cov-" + Guid.NewGuid().ToString("N"));
        var inbox = Path.Combine(root, "inbox");
        var output = Path.Combine(root, "ndt-out");
        var accepted = Path.Combine(root, "sap-accepted");
        Directory.CreateDirectory(inbox);
        Directory.CreateDirectory(output);
        Directory.CreateDirectory(accepted);

        try
        {
            var name = "2510117_01_260714_1000060163.csv";
            var source = Path.Combine(inbox, name);
            File.WriteAllText(source, "PO Number,Slit No,NDT Pipes,Mill No\n1000060163,01,10,1\n");

            // Output no longer in pending — SAP moved it to Accepted (stamped with batch numbers).
            File.WriteAllText(
                Path.Combine(accepted, name),
                "PO Number,Slit No,NDT Pipes,Mill No,NDT Batch No\n1000060163,01,10,1,1226100001\n");

            var opts = new NdtBundleOptions
            {
                OutputBundleFolder = output,
                NdtInputSlitAcceptedFolder = accepted,
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

    /// <summary>
    /// A copy in the Rejected folder is NOT coverage: rejected data never posted to SAP, so the
    /// resubmit/backfill flow must remain open.
    /// </summary>
    [Fact]
    public void Coverage_not_exact_match_for_rejected_folder_copy()
    {
        var root = Path.Combine(Path.GetTempPath(), "ndt-sap-cov-" + Guid.NewGuid().ToString("N"));
        var inbox = Path.Combine(root, "inbox");
        var output = Path.Combine(root, "ndt-out");
        var rejected = Path.Combine(root, "sap-rejected");
        Directory.CreateDirectory(inbox);
        Directory.CreateDirectory(output);
        Directory.CreateDirectory(rejected);

        try
        {
            var name = "2510117_02_260714_1000060163.csv";
            var source = Path.Combine(inbox, name);
            File.WriteAllText(source, "PO Number,Slit No,NDT Pipes,Mill No\n1000060163,02,8,1\n");

            File.WriteAllText(
                Path.Combine(rejected, name),
                "PO Number,Slit No,NDT Pipes,Mill No,NDT Batch No\n1000060163,02,8,1,1226100001\n");

            var opts = new NdtBundleOptions
            {
                OutputBundleFolder = output,
                NdtInputSlitRejectedFolder = rejected,
                BundleSummaryOutputFolder = Path.Combine(root, "bundles")
            };
            Directory.CreateDirectory(opts.BundleSummaryOutputFolder);

            var rows = new List<InputSlitRecord>
            {
                new() { PoNumber = "1000060163", MillNo = 1, SlitNo = "02", NdtPipes = 8 }
            };

            Assert.NotEqual(
                BackfillCoverageKind.ExactMatch,
                InputSlitBackfillCoverage.Evaluate(source, rows, opts));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { /* best-effort */ }
        }
    }

    /// <summary>Accepted-folder copy without batch numbers is not treated as coverage.</summary>
    [Fact]
    public void Coverage_accepted_folder_file_without_batch_numbers_is_not_exact_match()
    {
        var root = Path.Combine(Path.GetTempPath(), "ndt-sap-cov-" + Guid.NewGuid().ToString("N"));
        var inbox = Path.Combine(root, "inbox");
        var output = Path.Combine(root, "ndt-out");
        var accepted = Path.Combine(root, "sap-accepted");
        Directory.CreateDirectory(inbox);
        Directory.CreateDirectory(output);
        Directory.CreateDirectory(accepted);

        try
        {
            var name = "2510117_03_260714_1000060163.csv";
            var source = Path.Combine(inbox, name);
            File.WriteAllText(source, "PO Number,Slit No,NDT Pipes,Mill No\n1000060163,03,6,1\n");

            // Same basename but no NDT Batch No column — not a stamped output.
            File.WriteAllText(
                Path.Combine(accepted, name),
                "PO Number,Slit No,NDT Pipes,Mill No\n1000060163,03,6,1\n");

            var opts = new NdtBundleOptions
            {
                OutputBundleFolder = output,
                NdtInputSlitAcceptedFolder = accepted,
                BundleSummaryOutputFolder = Path.Combine(root, "bundles")
            };
            Directory.CreateDirectory(opts.BundleSummaryOutputFolder);

            var rows = new List<InputSlitRecord>
            {
                new() { PoNumber = "1000060163", MillNo = 1, SlitNo = "03", NdtPipes = 6 }
            };

            Assert.NotEqual(
                BackfillCoverageKind.ExactMatch,
                InputSlitBackfillCoverage.Evaluate(source, rows, opts));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { /* best-effort */ }
        }
    }

    [Theory]
    [InlineData(@"Z:\To SAP\NDT Input Slit", @"Z:\To SAP\NDT Input Slit", true)]
    [InlineData(@"Z:\To SAP\NDT Input Slit\", @"z:\to sap\ndt input slit", true)]
    [InlineData(@"Z:\To SAP\NDT Input Slit", @"Z:\To SAP\NDT Input Slit Accepted", false)]
    [InlineData("", @"Z:\To SAP\NDT Input Slit", false)]
    [InlineData(null, null, false)]
    public void Watcher_folder_overlap_detection(string? a, string? b, bool expected)
    {
        Assert.Equal(expected, NdtInputSlitSapStatusWorker.FoldersOverlap(a, b));
    }
}
