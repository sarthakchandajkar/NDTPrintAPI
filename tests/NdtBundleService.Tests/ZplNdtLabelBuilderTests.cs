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

    [Fact]
    public void BuildNdtTagZpl_Default_Uses100x100mmSquareCanvas()
    {
        var zpl = Encoding.UTF8.GetString(BuildSampleTag());

        Assert.Contains("^PW800^LL800^LH0,0", zpl, StringComparison.Ordinal);
        Assert.Equal(3, CountOccurrences(zpl, "^BCN,"));
    }

    [Fact]
    public void BuildNdtTagZpl_Square100x100_UsesTwoBottomBarcodes()
    {
        var size = new ZplNdtLabelBuilder.NdtTagLabelSize(100, 100);
        var zpl = Encoding.UTF8.GetString(BuildSampleTag(size));

        Assert.Contains("^PW800^LL800^LH0,0", zpl, StringComparison.Ordinal);
        Assert.Equal(3, CountOccurrences(zpl, "^BCN,"));
        Assert.Contains("^FO40,476^BY3^BCN,100,Y,N,N^FD", zpl, StringComparison.Ordinal);
        Assert.Contains("^FO40,626^BY3^BCN,100,Y,N,N^FD", zpl, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildDummyLabelZpl_Square100x100_SpreadsContentAcrossFullLabel()
    {
        var zpl = Encoding.UTF8.GetString(ZplDummyLabelBuilder.BuildDummyLabelZpl(labelWidthMm: 100, labelLengthMm: 100));

        Assert.Contains("^PW800^LL800^LH0,0", zpl, StringComparison.Ordinal);
        Assert.Equal(3, CountOccurrences(zpl, "^BCN,"));
        Assert.Contains("MADE IN OMAN - TEST PRINT^FS", zpl, StringComparison.Ordinal);
        Assert.Contains("^FO40,732^FB720,1,0,C,0^FDMADE IN OMAN - TEST PRINT^FS", zpl, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildNdtTagZpl_Compact100x50_UsesSingleBottomBarcode()
    {
        var size = new ZplNdtLabelBuilder.NdtTagLabelSize(100, 50);
        var zpl = Encoding.UTF8.GetString(BuildSampleTag(size));

        Assert.Contains("^PW800^LL400^LH0,0", zpl, StringComparison.Ordinal);
        Assert.Equal(2, CountOccurrences(zpl, "^BCN,"));
    }

    private static byte[] BuildSampleTag(ZplNdtLabelBuilder.NdtTagLabelSize? size = null) =>
        ZplNdtLabelBuilder.BuildNdtTagZpl(
            ndtBatchNo: "1226100078",
            millNo: 1,
            poNumber: "1000059504",
            pipeGrade: "X52",
            pipeSize: "2\"",
            pipeThickness: "0.250",
            pipeLength: "6.000",
            bundleWeight: "500",
            pipeType: "WIP",
            date: new DateTime(2026, 6, 24),
            pcsInBundle: 10,
            isReprint: false,
            labelSize: size);

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }
}
