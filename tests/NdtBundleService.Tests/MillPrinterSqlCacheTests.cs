using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class MillPrinterSqlCacheTests
{
    [Fact]
    public async Task Cache_ttl_holds_stale_value_until_expiry_then_propagates_across_instances()
    {
        var now = new DateTime(2026, 8, 31, 12, 0, 0, DateTimeKind.Utc);
        DateTime UtcNow() => now;

        var backing = new InMemoryMillPrinterBackingStore();
        backing.Save([new MillPrinterEndpoint(1, "192.168.0.125", 9100)]);

        var options = new OptionsMonitorStub(new NdtBundleOptions());
        var mill = NewService(TestMillOwnership.Mill(1), backing, UtcNow);
        var shared = NewService(TestMillOwnership.Shared(), backing, UtcNow);

        var first = mill.ResolveForMill(1);
        Assert.Equal("192.168.0.125", first.Address);
        Assert.Equal(1, backing.LoadCalls);

        await shared.SaveAllAsync([new MillPrinterEndpoint(1, "10.0.0.9", 9100)], CancellationToken.None);
        Assert.Equal("10.0.0.9", shared.ResolveForMill(1).Address);

        Assert.Equal("192.168.0.125", mill.ResolveForMill(1).Address);
        Assert.Equal(1, backing.LoadCalls);

        now = now.Add(MillPrinterSettingsService.CacheTtl).AddMilliseconds(1);
        Assert.Equal("10.0.0.9", mill.ResolveForMill(1).Address);
        Assert.True(backing.LoadCalls >= 2);
    }

    [Fact]
    public void Mill_2_missing_row_is_not_configured_and_does_not_use_mill_1_printer()
    {
        var backing = new InMemoryMillPrinterBackingStore();
        backing.Save([new MillPrinterEndpoint(1, "192.168.0.125", 9100)]);
        var mill2 = NewService(
            TestMillOwnership.Mill(2),
            backing,
            () => DateTime.UtcNow,
            new NdtBundleOptions { NdtTagPrinterAddress = "192.168.0.125" });

        var resolved = mill2.ResolveForMill(2);
        Assert.False(resolved.Configured);
        Assert.Equal(string.Empty, resolved.Address);

        var mill1OnMill2Process = mill2.ResolveForMill(1);
        Assert.False(mill1OnMill2Process.Configured);
    }

    [Fact]
    public void Mill_1_falls_back_to_NdtTagPrinterAddress_when_row_missing()
    {
        var backing = new InMemoryMillPrinterBackingStore();
        var mill1 = NewService(
            TestMillOwnership.Mill(1),
            backing,
            () => DateTime.UtcNow,
            new NdtBundleOptions { NdtTagPrinterAddress = "192.168.0.125", NdtTagPrinterPort = 9100 });

        var resolved = mill1.ResolveForMill(1);
        Assert.True(resolved.Configured);
        Assert.Equal("192.168.0.125", resolved.Address);
    }

    [Fact]
    public void Mill_1_cannot_write_mill_2_printer()
    {
        var mill1 = NewService(TestMillOwnership.Mill(1), new InMemoryMillPrinterBackingStore(), () => DateTime.UtcNow);
        var ex = Assert.Throws<InvalidOperationException>(() =>
            mill1.SaveAllAsync([new MillPrinterEndpoint(2, "10.0.0.2", 9100)], CancellationToken.None)
                .GetAwaiter().GetResult());
        Assert.Contains("Mill 2", ex.Message, StringComparison.Ordinal);
    }

    private static MillPrinterSettingsService NewService(
        IMillOwnership ownership,
        IMillPrinterBackingStore store,
        Func<DateTime> utcNow,
        NdtBundleOptions? options = null) =>
        new(
            new OptionsMonitorStub(options ?? new NdtBundleOptions()),
            ownership,
            NullLogger<MillPrinterSettingsService>.Instance,
            utcNow,
            store);

    private sealed class OptionsMonitorStub(NdtBundleOptions value) : IOptionsMonitor<NdtBundleOptions>
    {
        public NdtBundleOptions CurrentValue { get; } = value;
        public NdtBundleOptions Get(string? name) => CurrentValue;
        public IDisposable? OnChange(Action<NdtBundleOptions, string?> listener) => null;
    }
}
