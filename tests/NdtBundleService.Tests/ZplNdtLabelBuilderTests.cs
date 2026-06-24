using System.Text;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class ZplNdtLabelBuilderTests
{
    [Fact]
    public void BuildNdtTagZpl_PutsWeightOnSeparateLineFromSize()
    {
        var zpl = Encoding.UTF8.GetString(ZplNdtLabelBuilder.BuildNdtTagZpl(
            ndtBatchNo: "1226100078",
            millNo: 1,
            poNumber: "1000059504",
            pipeGrade: "--",
            pipeSize: "2 1/2\"",
            pipeThickness: "",
            pipeLength: "6.000",
            bundleWeight: "1184.5725",
            pipeType: "WIP",
            date: new DateTime(2026, 6, 24),
            pcsInBundle: 27,
            isReprint: true));

        Assert.Contains("Len: 6.000  Wt: 1184.5725^FS", zpl, StringComparison.Ordinal);
        Assert.Contains("Size:", zpl, StringComparison.Ordinal);
        Assert.DoesNotContain("Thk: -  Len:", zpl, StringComparison.Ordinal);
    }
}
