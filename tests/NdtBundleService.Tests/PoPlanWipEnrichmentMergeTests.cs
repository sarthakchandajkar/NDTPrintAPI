using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class PoPlanWipEnrichmentMergeTests
{
    [Fact]
    public void MergeSnapshots_fills_missing_po_from_csv()
    {
        var sql = new PoPlanWipSqlSnapshot
        {
            ByPo = new Dictionary<string, PoPlanWipRow>(StringComparer.OrdinalIgnoreCase)
            {
                ["10001111"] = new PoPlanWipRow { PoNumber = "10001111", PipeSize = "6" }
            },
            ByMill = new Dictionary<int, PoPlanWipRow>(),
            SourceDescription = "sql"
        };

        var csv = new PoPlanWipEnrichmentSnapshot(
            new Dictionary<int, PoPlanWipRow>(),
            new Dictionary<string, PoPlanWipRow>(StringComparer.OrdinalIgnoreCase)
            {
                ["10002222"] = new PoPlanWipRow
                {
                    PoNumber = "10002222",
                    PipeSize = "4",
                    PlannedMonth = "Jun-2026",
                    PiecesPerBundle = "12"
                }
            },
            "csv");

        var merged = PoPlanWipEnrichmentMerge.MergeSnapshots(sql, csv, "merged");

        Assert.Equal(2, merged.ByPo.Count);
        Assert.Equal("4", merged.ByPo["10002222"].PipeSize);
        Assert.Equal("Jun-2026", merged.ByPo["10002222"].PlannedMonth);
    }

    [Fact]
    public void MergeRows_keeps_sql_pipe_size_and_fills_csv_planned_month()
    {
        var sql = new PoPlanWipRow { PoNumber = "10001111", PipeSize = "6" };
        var csv = new PoPlanWipRow { PoNumber = "10001111", PlannedMonth = "Jun-2026", PipeSize = "4" };

        var merged = PoPlanWipEnrichmentMerge.MergeRows(sql, csv);

        Assert.Equal("6", merged.PipeSize);
        Assert.Equal("Jun-2026", merged.PlannedMonth);
    }

    [Fact]
    public void MergeRows_fills_pipe_thickness_from_csv_when_sql_missing()
    {
        var sql = new PoPlanWipRow { PoNumber = "10001111", PipeSize = "6" };
        var csv = new PoPlanWipRow { PoNumber = "10001111", PipeThickness = "2.6", PipeWeightPerMeter = "4.7" };

        var merged = PoPlanWipEnrichmentMerge.MergeRows(sql, csv);

        Assert.Equal("6", merged.PipeSize);
        Assert.Equal("2.6", merged.PipeThickness);
        Assert.Equal("4.7", merged.PipeWeightPerMeter);
    }
}
