using NdtBundleService.Configuration;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class PlcPoNumberResolutionTests
{
    private static PlcPoEndOptions DefaultCfg() => new()
    {
        MinValidPoId = 1_000_000_000,
        MaxValidPoId = int.MaxValue,
        MinSapPoNumberDigits = 10,
        PoNumberFormatFromPlc = "{0}",
    };

    [Theory]
    [InlineData(2, false)]
    [InlineData(1000059923, true)]
    [InlineData(1000059168, true)]
    [InlineData(999_999_999, false)]
    public void TryResolveFromPlcPoId_rejects_small_or_short_ids(int poId, bool expectedOk)
    {
        var ok = PlcPoNumberResolution.TryResolveFromPlcPoId(poId, DefaultCfg(), out var po);
        Assert.Equal(expectedOk, ok);
        if (expectedOk)
            Assert.Equal(poId.ToString(), po);
        else
            Assert.Equal(string.Empty, po);
    }

    [Fact]
    public void TryResolveFromPlcPoId_rejects_non_numeric_formatted_po()
    {
        var cfg = DefaultCfg();
        cfg.PoNumberFormatFromPlc = "PO{0}";
        var ok = PlcPoNumberResolution.TryResolveFromPlcPoId(1_000_599_923, cfg, out _);
        Assert.False(ok);
    }

    [Fact]
    public void IsPlausibleMesPoNumber_requires_minimum_digits()
    {
        Assert.False(PlcPoNumberResolution.IsPlausibleMesPoNumber("2", 10));
        Assert.True(PlcPoNumberResolution.IsPlausibleMesPoNumber("1000059923", 10));
    }
}
