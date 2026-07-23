using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.PoLifecycle;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>FIFO CSV recon, null-safe resume selector, and processing-failure backoff pins.</summary>
public sealed class Phase3F2FifoReconAndNullSelectorTests
{
    [Fact]
    public void NormalizePo_and_PoEquals_handle_null_without_throw()
    {
        Assert.Equal(string.Empty, InputSlitCsvParsing.NormalizePo(null));
        Assert.False(InputSlitCsvParsing.PoEquals(null, "1000060364"));
        Assert.False(InputSlitCsvParsing.PoEquals("1000060364", null));
        Assert.False(InputSlitCsvParsing.PoEquals(null, null));
    }

    [Fact]
    public void Resume_selector_with_null_wipEndedPo_does_not_throw_and_excludes_nothing_by_wip()
    {
        var closedEntries = new List<PoLifecycleDrainEntry>
        {
            new(1, "1000060364", DateTime.UtcNow.AddMinutes(-30), PoLifecyclePhase.Closed)
        };

        Assert.True(PoResumeCandidateSelector.IsEligibleForResumeCandidate(
            1,
            "1000060364",
            wipEndedPo: null,
            closedEntries));
    }

    [Fact]
    public void Fifo_attach_fills_oldest_awaiting_bundle_before_next()
    {
        var printed = new DateTime(2026, 7, 22, 16, 0, 0, DateTimeKind.Utc);
        var bundles = new List<PlcCsvReconAwaitingBundle>
        {
            new("1226100001", 1, PlcTotal: 10, CurrentSlitSum: 8, printed),
            new("1226100002", 2, PlcTotal: 12, CurrentSlitSum: 0, printed.AddMinutes(30))
        };

        Assert.True(PlcCsvReconFifo.TryAttachRow(
            bundles,
            new InputSlitRecord { PoNumber = "1000060364", MillNo = 1, SlitNo = "1", NdtPipes = 2 },
            out var batchNo));

        Assert.Equal("1226100001", batchNo);
        Assert.Equal(10, bundles[0].CurrentSlitSum);
        Assert.Equal(0, bundles[1].CurrentSlitSum);

        Assert.True(PlcCsvReconFifo.TryAttachRow(
            bundles,
            new InputSlitRecord { PoNumber = "1000060364", MillNo = 1, SlitNo = "2", NdtPipes = 2 },
            out batchNo));

        Assert.Equal("1226100002", batchNo);
    }

    [Fact]
    public void Surplus_rows_skip_awaiting_when_all_bundles_satisfied()
    {
        var bundles = new List<PlcCsvReconAwaitingBundle>
        {
            new("1226100001", 1, 10, 10, DateTime.UtcNow.AddHours(-2)),
            new("1226100002", 2, 12, 12, DateTime.UtcNow.AddHours(-1))
        };

        Assert.False(PlcCsvReconFifo.TryAttachRow(
            bundles,
            new InputSlitRecord { PoNumber = "1000060364", MillNo = 1, SlitNo = "9", NdtPipes = 5 },
            out _));
        Assert.False(PlcCsvReconFifo.HasUnfilledCapacity(bundles));
    }

    [Fact]
    public void Finalize_only_when_slit_sum_meets_plc_or_window_expires()
    {
        var withinWindow = DateTime.UtcNow.AddMinutes(-30);
        var deficit = PlcCsvReconSemantics.EvaluateFinalize(
            "1226100001", plcTotal: 10, slitSum: 2, withinWindow, reconWindowMinutes: 180, DateTime.UtcNow, force: false);
        Assert.False(deficit.ClearsAwaitingCsvRecon);
        Assert.True(deficit.CountDiscrepancy);

        var met = PlcCsvReconSemantics.EvaluateFinalize(
            "1226100001", plcTotal: 10, slitSum: 10, withinWindow, reconWindowMinutes: 180, DateTime.UtcNow, force: false);
        Assert.True(met.ClearsAwaitingCsvRecon);
        Assert.False(met.CountDiscrepancy);

        var expired = PlcCsvReconSemantics.EvaluateFinalize(
            "1226100001", plcTotal: 10, slitSum: 2, DateTime.UtcNow.AddMinutes(-200), reconWindowMinutes: 180, DateTime.UtcNow, force: false);
        Assert.True(expired.ClearsAwaitingCsvRecon);
        Assert.True(expired.CountDiscrepancy);
    }

    [Fact]
    public void File_retry_tracker_records_failures_and_caps_at_max()
    {
        var tracker = new InputSlitFileRetryTracker();
        const string file = @"Z:\inbox\2604345_03.csv";

        for (var i = 1; i < 5; i++)
        {
            var (maxReached, _, count) = tracker.RecordFailure(file, maxFailures: 5);
            Assert.False(maxReached);
            Assert.Equal(i, count);
            tracker.Park(file, DateTime.UtcNow, [5, 30, 120]);
            Assert.True(tracker.ShouldSkip(file, DateTime.UtcNow));
        }

        var (capped, shouldLog, finalCount) = tracker.RecordFailure(file, maxFailures: 5);
        Assert.True(capped);
        Assert.True(shouldLog);
        Assert.Equal(5, finalCount);

        var (_, shouldLogAgain, _) = tracker.RecordFailure(file, maxFailures: 5);
        Assert.False(shouldLogAgain);
    }
}
