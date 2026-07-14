using NdtBundleService.Configuration;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class InputSlitProcessMillsTests
{
    [Fact]
    public void Empty_list_allows_all_mills()
    {
        var o = new NdtBundleOptions { InputSlitProcessMills = null };
        Assert.True(SlitMonitoringWorker.IsMillAllowedForNdtInputSlit(o, 1));
        Assert.True(SlitMonitoringWorker.IsMillAllowedForNdtInputSlit(o, 4));
    }

    [Fact]
    public void Mill1_only_rejects_other_mills()
    {
        var o = new NdtBundleOptions { InputSlitProcessMills = [1] };
        Assert.True(SlitMonitoringWorker.IsMillAllowedForNdtInputSlit(o, 1));
        Assert.False(SlitMonitoringWorker.IsMillAllowedForNdtInputSlit(o, 2));
        Assert.False(SlitMonitoringWorker.IsMillAllowedForNdtInputSlit(o, 3));
        Assert.False(SlitMonitoringWorker.IsMillAllowedForNdtInputSlit(o, 4));
    }

    [Fact]
    public void Invalid_mill_always_rejected()
    {
        var o = new NdtBundleOptions { InputSlitProcessMills = null };
        Assert.False(SlitMonitoringWorker.IsMillAllowedForNdtInputSlit(o, 0));
        Assert.False(SlitMonitoringWorker.IsMillAllowedForNdtInputSlit(o, 5));
    }
}
