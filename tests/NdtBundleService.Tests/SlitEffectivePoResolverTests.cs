using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class SlitEffectivePoResolverTests
{
    [Fact]
    public void Resolve_prefers_slit_po_when_both_slit_and_wip_present()
    {
        var po = SlitEffectivePoResolver.Resolve("1000059208", "1000059047");

        Assert.Equal("1000059208", po);
    }

    [Fact]
    public void Resolve_uses_wip_when_slit_po_missing()
    {
        var po = SlitEffectivePoResolver.Resolve("", "1000059047");

        Assert.Equal("1000059047", po);
    }

    [Fact]
    public void Resolve_returns_empty_when_both_missing()
    {
        Assert.Equal(string.Empty, SlitEffectivePoResolver.Resolve(null, null));
    }
}
