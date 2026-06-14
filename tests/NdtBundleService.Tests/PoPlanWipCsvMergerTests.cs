using Microsoft.Extensions.Logging.Abstractions;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class PoPlanWipCsvMergerTests
{
    [Fact]
    public async Task MergeFileAsync_reads_pipe_thickness_from_po_accepted_headers()
    {
        var path = Path.Combine(Path.GetTempPath(), $"po-plan-{Guid.NewGuid():N}.csv");
        await File.WriteAllTextAsync(path,
            "PO Number,Mill No,Planned Month,Pipe Grade,Pipe Size,Pipe Thickness,Pipe Length,Pieces Per Bundle,NDTPcsPerBundle,Total Pieces\n" +
            "1000059234,03,Jun-2026,X52,2.5,2.6,6.000,70,20,1728\n");

        try
        {
            var merge = new PoPlanWipCsvMerger.MergeResult();
            var ok = await PoPlanWipCsvMerger.MergeFileAsync(
                path,
                merge,
                NullLogger.Instance,
                CancellationToken.None);

            Assert.True(ok);
            Assert.True(merge.ByPo.TryGetValue("1000059234", out var row));
            Assert.Equal("2.6", row.PipeThickness);
            Assert.Equal("2.5", row.PipeSize);
            Assert.Equal(3, row.MillNo);
            Assert.Equal("20", row.NdtPcsPerBundle);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task MergeFileAsync_reads_wip_bundle_columns()
    {
        var path = Path.Combine(Path.GetTempPath(), $"wip-{Guid.NewGuid():N}.csv");
        await File.WriteAllTextAsync(path,
            "PO_No,Mill Number,Planned Month,Pipe Grade,Pipe Size,Pipe Thickness,Pipe Length,Pipe Weight Per Meter,Pipe Type,Output Itemcode,Item Description,Total Pieces,Pieces Per Bundle,Product Type,PO Specification,Input WIP Itemcode\n" +
            "1000055673,01,02,,2.5,2.6,6.000,4.713,WIP,000004013082600600,BARE WIP,1728.000,70,,,000004910002602340\n");

        try
        {
            var merge = new PoPlanWipCsvMerger.MergeResult();
            var ok = await PoPlanWipCsvMerger.MergeFileAsync(
                path,
                merge,
                NullLogger.Instance,
                CancellationToken.None);

            Assert.True(ok);
            Assert.True(merge.ByPo.TryGetValue("1000055673", out var row));
            Assert.Equal("2.6", row.PipeThickness);
            Assert.Equal("4.713", row.PipeWeightPerMeter);
            Assert.Equal("WIP", row.PipeType);
            Assert.Equal("000004013082600600", row.OutputItemcode);
        }
        finally
        {
            File.Delete(path);
        }
    }
}
