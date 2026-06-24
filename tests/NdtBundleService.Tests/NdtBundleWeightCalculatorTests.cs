using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class NdtBundleWeightCalculatorTests
{
    [Fact]
    public void FormatBundleWeight_uses_per_meter_length_and_piece_count()
    {
        var wt = NdtBundleWeightCalculator.FormatBundleWeight("4.713", "6.000", 20);
        Assert.Equal("565.56", wt);
    }

    [Fact]
    public void FormatBundleWeight_falls_back_to_per_meter_times_pieces_when_length_missing()
    {
        var wt = NdtBundleWeightCalculator.FormatBundleWeight("4.713", "", 20);
        Assert.Equal("94.26", wt);
    }

    [Fact]
    public void FormatBundleWeight_returns_empty_when_weight_missing()
    {
        Assert.Equal(string.Empty, NdtBundleWeightCalculator.FormatBundleWeight("", "6.000", 20));
        Assert.Equal(string.Empty, NdtBundleWeightCalculator.FormatBundleWeight("4.713", "6.000", 0));
    }

    [Fact]
    public void TryParsePositiveDecimal_accepts_leading_numeric_prefix()
    {
        Assert.True(NdtBundleWeightCalculator.TryParsePositiveDecimal("6.000'", out var length));
        Assert.Equal(6.000m, length);
    }
}
