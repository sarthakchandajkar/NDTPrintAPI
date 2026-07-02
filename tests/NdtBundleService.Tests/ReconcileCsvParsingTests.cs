using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class ReconcileCsvParsingTests
{
    [Theory]
    [InlineData("", "—")]
    [InlineData("  ", "—")]
    [InlineData("01", "01")]
    public void NormalizeSlitKey_maps_empty_to_dash(string raw, string expected) =>
        Assert.Equal(expected, ReconcileCsvParsing.NormalizeSlitKey(raw));

    [Theory]
    [InlineData("", "—", true)]
    [InlineData("", "01", false)]
    [InlineData("01", "01", true)]
    [InlineData("1", "01", false)]
    public void SlitKeysMatch_treats_empty_as_dash(string cell, string target, bool expected) =>
        Assert.Equal(expected, ReconcileCsvParsing.SlitKeysMatch(cell, target));

    [Theory]
    [InlineData("2603832_05_260615_1000059046.csv", "2603832_05", true)]
    [InlineData("quoted.csv", "05", false)]
    [InlineData("2603832_05_260615_1000059046.csv", "05", true)]
    [InlineData("2604061_01_260615_1000059046.csv", "2603832_05", false)]
    public void PerSlitOutputFileNameMatchesSlit_matches_input_slit_filename_pattern(
        string fileName,
        string slit,
        bool expected) =>
        Assert.Equal(expected, ReconcileCsvParsing.PerSlitOutputFileNameMatchesSlit(fileName, slit));

    [Fact]
    public void ResolveOutputCsvColumns_uses_header_positions()
    {
        var header = "PO Number,Slit No,NDT Pipes,Rejected P,Slit Start Time,Slit Finish Time,Mill No,NDT Short Length Pipe,Rejected Short Length Pipe,NDT Batch No";
        var cols = ReconcileCsvParsing.ResolveOutputCsvColumns(header);

        Assert.Equal(0, cols.PoNumber);
        Assert.Equal(1, cols.SlitNo);
        Assert.Equal(2, cols.NdtPipes);
        Assert.Equal(9, cols.NdtBatchNo);
    }

    [Fact]
    public void SplitCsvLine_handles_quoted_commas()
    {
        var cols = ReconcileCsvParsing.SplitCsvLine("\"PO,1\",01,15,0,2026-06-17,2026-06-17,1,,,012600001");
        Assert.True(cols.Count >= 10);
        Assert.Equal("PO,1", cols[0]);
        Assert.Equal("01", cols[1]);
        Assert.Equal("15", cols[2]);
    }
}
