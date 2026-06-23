using NdtBundleService.Services.PlcHandshake;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class PlcHandshakeStatusRegistryTests
{
    [Fact]
    public void AllConnected_ignores_manually_disconnected_mills()
    {
        var registry = new PlcHandshakeStatusRegistry();
        registry.RegisterMill(1, new PlcHandshakeMillStatus { MillNo = 1, Connected = true, PlcConnectionEnabled = true });
        registry.RegisterMill(4, new PlcHandshakeMillStatus { MillNo = 4, Connected = false, PlcConnectionEnabled = false });

        Assert.True(registry.AllConnected());
    }

    [Fact]
    public void AllConnected_false_when_enabled_mill_is_offline()
    {
        var registry = new PlcHandshakeStatusRegistry();
        registry.RegisterMill(1, new PlcHandshakeMillStatus { MillNo = 1, Connected = true, PlcConnectionEnabled = true });
        registry.RegisterMill(4, new PlcHandshakeMillStatus { MillNo = 4, Connected = false, PlcConnectionEnabled = true });

        Assert.False(registry.AllConnected());
    }
}
