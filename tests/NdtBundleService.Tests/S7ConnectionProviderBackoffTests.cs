using Microsoft.Extensions.Logging.Abstractions;
using NdtBundleService.Configuration;
using NdtBundleService.Services.PlcHandshake.S7;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class S7ConnectionProviderBackoffTests
{
    [Fact]
    public void TakeReconnectDelayMs_uses_PlcHandshake_Initial_and_Max_not_a_parallel_key()
    {
        var mill = new MillConfig
        {
            Name = "Mill-1",
            MillNo = 1,
            IpAddress = "127.0.0.1",
            Slot = 2
        };
        var options = new PlcHandshakeOptions
        {
            InitialReconnectDelayMs = 1000,
            MaxReconnectDelayMs = 30_000
        };

        var provider = new S7ConnectionProvider(mill, options, NullLogger.Instance);
        Assert.Equal(1000, provider.TakeReconnectDelayMs());
        Assert.Equal(2000, provider.TakeReconnectDelayMs());
        Assert.Equal(4000, provider.TakeReconnectDelayMs());

        provider.ResetReconnectBackoff();
        Assert.Equal(1000, provider.TakeReconnectDelayMs());
    }
}
