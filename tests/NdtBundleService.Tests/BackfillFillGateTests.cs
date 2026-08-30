using Microsoft.Extensions.Logging.Abstractions;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.PoLifecycle;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// F-5 fill-to-target gate: fail-closed SQL coverage, and LastWrite vs incomplete PrintedAt
/// when a terminal fill row already exists. Live ingest is not covered here.
/// </summary>
public sealed class BackfillFillGateTests
{
    private const string Po = "1000060363";
    private const int Mill = 1;
    private const string Batch0001 = "1226100001";
    private const string Batch0002 = "1226100002";
    private const string LateFile = "late_straggler.csv";

    [Fact]
    public async Task Terminal_complete_0001_file_before_0002_printedAt_holds_no_stamp()
    {
        var printed2 = new DateTime(2026, 8, 30, 12, 0, 0, DateTimeKind.Utc);
        var fileLw = printed2.AddMinutes(-5);
        var fill = new InMemoryTransactionalCsvFillService();
        fill.Seed(Batch0001, target: 22, filled: 22, CsvFillState.CsvComplete, printedAt: printed2.AddMinutes(-20));
        fill.Seed(Batch0002, target: 22, filled: 0, CsvFillState.PlcClosed, printedAt: printed2);

        Assert.True(await fill.HasTerminalFillRowAsync(Po, Mill, CancellationToken.None));
        var incomplete = await fill.TryGetOldestIncompleteAsync(Po, Mill, pipeSize: null, CancellationToken.None);
        Assert.Equal(Batch0002, incomplete!.BundleNo);

        Assert.True(InputSlitBackfillFillGate.ShouldApply(
            isBackfill: true,
            BackfillBundlingAction.NormalBundle,
            MillPoEndSource.Plc,
            isFillToTarget: true));
        Assert.True(InputSlitBackfillFillGate.ShouldHoldRatherThanStamp(
            fileLw,
            hasTerminalFillRow: true,
            incomplete.PrintedAtUtc));

        await fill.UpsertHoldAsync(
            LateFile, Po, Mill, pipeSize: null, CsvFillHoldReason.BackfillAfterTerminal, CancellationToken.None);

        Assert.Equal(22, fill.GetFilled(Batch0001));
        Assert.Equal(CsvFillState.CsvComplete, fill.GetState(Batch0001));
        Assert.Equal(0, fill.GetFilled(Batch0002));
        Assert.Equal(CsvFillState.PlcClosed, fill.GetState(Batch0002));
        Assert.Contains(LateFile, fill.HeldFiles, StringComparer.OrdinalIgnoreCase);
        Assert.Contains(fill.HoldRecords, h =>
            h.File.Equals(LateFile, StringComparison.OrdinalIgnoreCase)
            && h.Reason == CsvFillHoldReason.BackfillAfterTerminal);
    }

    [Fact]
    public async Task Terminal_complete_0001_file_after_0002_printedAt_stamps_0002()
    {
        var printed2 = new DateTime(2026, 8, 30, 12, 0, 0, DateTimeKind.Utc);
        var fileLw = printed2.AddMinutes(5);
        var fill = new InMemoryTransactionalCsvFillService();
        fill.Seed(Batch0001, target: 22, filled: 22, CsvFillState.CsvComplete, printedAt: printed2.AddMinutes(-20));
        fill.Seed(Batch0002, target: 22, filled: 0, CsvFillState.PlcClosed, printedAt: printed2);

        var incomplete = await fill.TryGetOldestIncompleteAsync(Po, Mill, pipeSize: null, CancellationToken.None);
        Assert.False(InputSlitBackfillFillGate.ShouldHoldRatherThanStamp(
            fileLw,
            hasTerminalFillRow: true,
            incomplete!.PrintedAtUtc));

        var assigner = new SlitCsvFillAssigner(fill, NullLogger<SlitCsvFillAssigner>.Instance);
        var stamped = await assigner.AssignAsync(
            @"C:\inbox\next.csv", Po, Mill, pipeSize: null, fileNdtPipes: 7,
            holdWhenNoOpenBundle: true, CancellationToken.None);

        Assert.Equal(Batch0002, stamped.BatchNo);
        Assert.Equal(22, fill.GetFilled(Batch0001));
        Assert.Equal(CsvFillState.CsvComplete, fill.GetState(Batch0001));
        Assert.Equal(7, fill.GetFilled(Batch0002));
        Assert.Equal(CsvFillState.CsvFilling, fill.GetState(Batch0002));
        Assert.Empty(fill.HeldFiles);
    }

    [Fact]
    public async Task No_terminal_incomplete_0001_stamps_0001()
    {
        var fill = new InMemoryTransactionalCsvFillService();
        fill.Seed(Batch0001, target: 22, filled: 0, CsvFillState.PlcClosed, printedAt: DateTime.UtcNow);

        Assert.False(await fill.HasTerminalFillRowAsync(Po, Mill, CancellationToken.None));
        var incomplete = await fill.TryGetOldestIncompleteAsync(Po, Mill, pipeSize: null, CancellationToken.None);
        Assert.False(InputSlitBackfillFillGate.ShouldHoldRatherThanStamp(
            DateTime.UtcNow.AddMinutes(-1),
            hasTerminalFillRow: false,
            incomplete!.PrintedAtUtc));
        Assert.False(InputSlitBackfillFillGate.ShouldApply(
            isBackfill: false,
            BackfillBundlingAction.NormalBundle,
            MillPoEndSource.Plc,
            isFillToTarget: true));

        var assigner = new SlitCsvFillAssigner(fill, NullLogger<SlitCsvFillAssigner>.Instance);
        var stamped = await assigner.AssignAsync(
            @"C:\inbox\first.csv", Po, Mill, pipeSize: null, fileNdtPipes: 11,
            holdWhenNoOpenBundle: true, CancellationToken.None);

        Assert.Equal(Batch0001, stamped.BatchNo);
        Assert.Equal(11, fill.GetFilled(Batch0001));
        Assert.Empty(fill.HeldFiles);
    }

    [Fact]
    public async Task Live_ingest_stamps_0002_identically_when_backfill_would_hold()
    {
        var printed2 = new DateTime(2026, 8, 30, 12, 0, 0, DateTimeKind.Utc);
        var fileLw = printed2.AddMinutes(-5);
        const int pipes = 7;

        var liveAction = ClosedPoSlitIngestPolicy.DecideForRow(
            isBackfill: false,
            BackfillCoverageKind.None,
            PoLifecyclePhase.Running,
            MillPoEndSource.Plc,
            autoCloseOrphanBundles: true);
        Assert.Equal(BackfillBundlingAction.NormalBundle, liveAction);
        Assert.False(InputSlitBackfillFillGate.ShouldApply(
            isBackfill: false,
            liveAction,
            MillPoEndSource.Plc,
            isFillToTarget: true));

        var backfillAction = ClosedPoSlitIngestPolicy.DecideForRow(
            isBackfill: true,
            BackfillCoverageKind.None,
            PoLifecyclePhase.Running,
            MillPoEndSource.Plc,
            autoCloseOrphanBundles: true);
        Assert.True(InputSlitBackfillFillGate.ShouldApply(
            isBackfill: true,
            backfillAction,
            MillPoEndSource.Plc,
            isFillToTarget: true));
        Assert.True(InputSlitBackfillFillGate.ShouldHoldRatherThanStamp(
            fileLw,
            hasTerminalFillRow: true,
            printed2));

        var fill = new InMemoryTransactionalCsvFillService();
        fill.Seed(Batch0001, target: 22, filled: 22, CsvFillState.CsvComplete, printedAt: printed2.AddMinutes(-20));
        fill.Seed(Batch0002, target: 22, filled: 0, CsvFillState.PlcClosed, printedAt: printed2);

        var assigner = new SlitCsvFillAssigner(fill, NullLogger<SlitCsvFillAssigner>.Instance);
        var stamped = await assigner.AssignAsync(
            @"C:\inbox\live.csv", Po, Mill, pipeSize: null, pipes,
            holdWhenNoOpenBundle: true, CancellationToken.None);

        Assert.Equal(Batch0002, stamped.BatchNo);
        Assert.False(stamped.Held);
        Assert.Equal(22, fill.GetFilled(Batch0001));
        Assert.Equal(CsvFillState.CsvComplete, fill.GetState(Batch0001));
        Assert.Equal(pipes, fill.GetFilled(Batch0002));
        Assert.Equal(CsvFillState.CsvFilling, fill.GetState(Batch0002));
        Assert.Empty(fill.HeldFiles);
    }

    [Fact]
    public void Coverage_ambiguous_when_summary_bundle_exists_still_manual_review()
    {
        var root = Path.Combine(Path.GetTempPath(), "ndt-backfill-gate-" + Guid.NewGuid().ToString("N"));
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
    public void Sql_printed_lookup_failure_is_ambiguous_not_none()
    {
        Assert.False(InputSlitBackfillPolicy.FailClosedHasPrintedBundle(found: false, lookupFailed: false));
        Assert.True(InputSlitBackfillPolicy.FailClosedHasPrintedBundle(found: false, lookupFailed: true));
        Assert.True(InputSlitBackfillPolicy.FailClosedHasPrintedBundle(found: true, lookupFailed: false));

        Assert.Equal(
            BackfillCoverageKind.Ambiguous,
            InputSlitBackfillPolicy.ApplySqlPrintedLookup(
                BackfillCoverageKind.None,
                sqlLookupFailed: true,
                hasPrintedBundle: false));
        Assert.Equal(
            BackfillCoverageKind.None,
            InputSlitBackfillPolicy.ApplySqlPrintedLookup(
                BackfillCoverageKind.None,
                sqlLookupFailed: false,
                hasPrintedBundle: false));
        Assert.Equal(
            BackfillCoverageKind.Ambiguous,
            InputSlitBackfillPolicy.ApplySqlPrintedLookup(
                BackfillCoverageKind.None,
                sqlLookupFailed: false,
                hasPrintedBundle: true));
        Assert.Equal(
            BackfillCoverageKind.ExactMatch,
            InputSlitBackfillPolicy.ApplySqlPrintedLookup(
                BackfillCoverageKind.ExactMatch,
                sqlLookupFailed: true,
                hasPrintedBundle: false));
    }
}
