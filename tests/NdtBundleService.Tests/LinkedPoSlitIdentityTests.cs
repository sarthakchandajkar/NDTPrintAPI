using NdtBundleService.Services.LinkedPo;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class LinkedPoSlitIdentityTests
{
    [Theory]
    [InlineData("2607388_01_91", true, "2607388_01")]
    [InlineData("2607388_01", false, "2607388_01")]
    [InlineData("2607388_01_91_extra", false, "2607388_01_91_extra")]
    public void Linked_suffix_and_base(string slit, bool hasSuffix, string expectedBase)
    {
        Assert.Equal(hasSuffix, LinkedPoSlitIdentity.HasLinkedSuffix(slit));
        Assert.Equal(expectedBase, LinkedPoSlitIdentity.ToBaseSlitNo(slit));
    }

    [Fact]
    public void SameBaseSlit_matches_91_to_base()
    {
        Assert.True(LinkedPoSlitIdentity.SameBaseSlit("2607388_01", "2607388_01_91"));
        Assert.False(LinkedPoSlitIdentity.SameBaseSlit("2607388_01", "2607388_02"));
    }

    [Theory]
    [InlineData("2607388_01_261010_1000062750.csv", "2607388_01", "261010", "1000062750")]
    [InlineData("2607388_01_91_261010_1000062751.csv", "2607388_01_91", "261010", "1000062751")]
    [InlineData("2606700_04_261007_1000062671.csv", "2606700_04", "261007", "1000062671")]
    public void TryParseInputSlitFileName_ok(string name, string slit, string day, string po)
    {
        Assert.True(LinkedPoSlitIdentity.TryParseInputSlitFileName(name, out var s, out var d, out var p));
        Assert.Equal(slit, s);
        Assert.Equal(day, d);
        Assert.Equal(po, p);
    }

    [Fact]
    public void TryParseInputSlitFileName_rejects_garbage()
    {
        Assert.False(LinkedPoSlitIdentity.TryParseInputSlitFileName("not-a-slit.csv", out _, out _, out _));
    }

    [Theory]
    [InlineData("2607388_01_91_261010_1000062751.csv", "2607388_01_261010_1000062751.csv")]
    [InlineData("2607388_01_261010_1000062751.csv", "2607388_01_261010_1000062751.csv")]
    [InlineData(@"\\share\folder\2607388_01_91_261010_1000062751.csv", "2607388_01_261010_1000062751.csv")]
    public void ToSapOutputFileName_strips_linked_91(string input, string expected)
    {
        Assert.Equal(expected, LinkedPoSlitIdentity.ToSapOutputFileName(input));
    }
}
