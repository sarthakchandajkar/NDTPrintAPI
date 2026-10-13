using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class InputSlitRejectedPipesParsingTests
{
    [Theory]
    [InlineData("0", true, 0)]
    [InlineData("1", true, 1)]
    [InlineData("2.0", true, 2)]
    [InlineData("", false, 0)]
    [InlineData("   ", false, 0)]
    [InlineData(null, false, 0)]
    [InlineData("x", false, 0)]
    [InlineData("-1", false, 0)]
    public void TryParseRejectedPipes_requires_present_non_negative(string? raw, bool ok, int expected)
    {
        var parsed = InputSlitCsvParsing.TryParseRejectedPipes(raw, out var value);
        Assert.Equal(ok, parsed);
        Assert.Equal(expected, value);
    }

    [Fact]
    public void NormalizeRejectedPipesHeader_canonicalizes_to_Rejected_Pipes()
    {
        var header =
            "PO Number,Slit No,NDT Pipes,Rejected P,Slit Start Time,Slit Finish Time,Mill No,NDT Short Length Pipe,Rejected Short Length Pipe";
        var normalized = InputSlitCsvParsing.NormalizeRejectedPipesHeader(header);
        Assert.Contains("Rejected Pipes,", normalized);
        Assert.DoesNotContain("Rejected P,", normalized);
        Assert.Contains("Rejected Short Length Pipe", normalized);
    }

    [Fact]
    public void NormalizeRejectedPipesHeader_leaves_Rejected_Pipes_unchanged()
    {
        var header =
            "PO Number,Slit No,NDT Pipes,Rejected Pipes,Slit Start Time,Slit Finish Time,Mill No,NDT Short Length Pipe,Rejected Short Length Pipe";
        var normalized = InputSlitCsvParsing.NormalizeRejectedPipesHeader(header);
        Assert.Equal(header, normalized);
    }
}
