using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class ManualStationFlowHydratorTests
{
    [Fact]
    public void FromRuns_prefers_completed_chain_even_when_later_orphan_visual_exists()
    {
        var t0 = new DateTime(2026, 9, 29, 10, 49, 12, DateTimeKind.Utc);
        var runs = new List<ManualStationPrintedTag>
        {
            Run(4, "Visual Station 2", null, 3, 1, t0),
            Run(5, "Hydrotesting", "Big Hydrotesting", 3, 0, t0.AddSeconds(23)),
            Run(6, "Revisual Station 1", null, 2, 1, t0.AddSeconds(44)),
            Run(7, "Visual Station 1", null, 3, 1, t0.AddHours(2)),
        };

        var result = ManualStationFlowHydrator.TryFromManualStationRuns(runs);
        Assert.NotNull(result);
        Assert.Equal(3, result!.Visual!.OkPcs);
        Assert.Equal(3, result.Hydrotesting!.OkPcs);
        Assert.Equal(2, result.Revisual!.OkPcs);
        Assert.False(result.HydroInvalidatedByVisualReconcile);
        Assert.False(result.RevisualInvalidatedByUpstreamReconcile);
        Assert.Equal("Manual_Station_Run", result.Source);
    }

    [Fact]
    public void FromRuns_partial_visual_only_when_no_hydro()
    {
        var t0 = DateTime.UtcNow;
        var runs = new List<ManualStationPrintedTag>
        {
            Run(1, "Visual Station 1", null, 4, 0, t0),
        };

        var result = ManualStationFlowHydrator.TryFromManualStationRuns(runs);
        Assert.NotNull(result);
        Assert.Equal(4, result!.Visual!.OkPcs);
        Assert.Null(result.Hydrotesting);
        Assert.Null(result.Revisual);
    }

    [Fact]
    public void FromRuns_visual_after_hydro_without_revisual_invalidates_downstream()
    {
        var t0 = DateTime.UtcNow;
        var runs = new List<ManualStationPrintedTag>
        {
            Run(1, "Visual Station 1", null, 4, 0, t0),
            Run(2, "Hydrotesting", "Four Head Hydrotesting", 3, 1, t0.AddMinutes(1)),
            Run(3, "Visual Station 2", null, 4, 0, t0.AddMinutes(5)),
        };

        var result = ManualStationFlowHydrator.TryFromManualStationRuns(runs);
        Assert.NotNull(result);
        Assert.Equal(4, result!.Visual!.OkPcs);
        Assert.Null(result.Hydrotesting);
        Assert.Null(result.Revisual);
        Assert.True(result.HydroInvalidatedByVisualReconcile);
        Assert.True(result.RevisualInvalidatedByUpstreamReconcile);
    }

    [Fact]
    public void FromNdtProcessMetrics_rebuilds_station_counts()
    {
        var result = ManualStationFlowHydrator.TryFromNdtProcessMetrics(
            ndtPcs: 4,
            okPcs: 2,
            visualReject: 1,
            hydroReject: 0,
            revisualReject: 1,
            bundleStart: new DateTime(2026, 9, 29, 14, 49, 12),
            bundleEnd: new DateTime(2026, 9, 29, 14, 49, 54));

        Assert.NotNull(result);
        Assert.Equal(3, result!.Visual!.OkPcs);
        Assert.Equal(1, result.Visual.RejectedPcs);
        Assert.Equal(3, result.Hydrotesting!.OkPcs);
        Assert.Equal(0, result.Hydrotesting.RejectedPcs);
        Assert.Equal(2, result.Revisual!.OkPcs);
        Assert.Equal(1, result.Revisual.RejectedPcs);
        Assert.Equal("NDT_process_CSV", result.Source);
    }

    private static ManualStationPrintedTag Run(
        long id,
        string workStation,
        string? hydroType,
        int ok,
        int reject,
        DateTime importedAtUtc) =>
        new()
        {
            Id = id,
            NdtBatchNo = "1226100002",
            WorkStation = workStation,
            HydrotestingType = hydroType,
            OkPcs = ok,
            RejectPcs = reject,
            NdtPcs = ok + reject,
            BundleStart = importedAtUtc,
            BundleEnd = importedAtUtc,
            ImportedAtUtc = importedAtUtc
        };
}
