using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class NdtBundleRuntimeStateLogicTests
{
    [Theory]
    [InlineData("Default:5", true)]
    [InlineData("Default:0", false)]
    [InlineData(null, false)]
    public void HasOpenPartialBundle_uses_size_counts_only(string? sizeCountsSpec, bool expected)
    {
        IReadOnlyDictionary<string, int>? sizeCounts = null;
        if (sizeCountsSpec is not null)
        {
            var parts = sizeCountsSpec.Split(':');
            sizeCounts = new Dictionary<string, int> { [parts[0]] = int.Parse(parts[1]) };
        }

        Assert.Equal(expected, NdtBundleRuntimeStateLogic.HasOpenPartialBundle(sizeCounts));
    }
}
