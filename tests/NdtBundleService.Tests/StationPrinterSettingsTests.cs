using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class StationPrinterSettingsTests
{
    [Fact]
    public void Visual_and_Revisual_map_to_the_same_VISUAL_REVISUAL_row()
    {
        Assert.Equal(StationPrinterTarget.VisualRevisual, StationPrinterTarget.For(ManualTagStation.Visual));
        Assert.Equal(StationPrinterTarget.VisualRevisual, StationPrinterTarget.For(ManualTagStation.Revisual));
        Assert.Equal(
            StationPrinterTarget.For(ManualTagStation.Visual),
            StationPrinterTarget.For(ManualTagStation.Revisual));
        Assert.Equal("Visual/Revisual", StationPrinterTarget.DisplayName(StationPrinterTarget.VisualRevisual));
        Assert.Contains("Visual/Revisual", StationPrinterTarget.UnconfiguredMessage(StationPrinterTarget.VisualRevisual), StringComparison.Ordinal);
    }

    [Fact]
    public void Hydro_lines_map_to_distinct_codes()
    {
        Assert.Equal(StationPrinterTarget.BigHydro, StationPrinterTarget.For(ManualTagStation.BigHydrotesting));
        Assert.Equal(StationPrinterTarget.FourHeadHydro, StationPrinterTarget.For(ManualTagStation.FourHeadHydrotesting));
        Assert.NotEqual(
            StationPrinterTarget.For(ManualTagStation.BigHydrotesting),
            StationPrinterTarget.For(ManualTagStation.FourHeadHydrotesting));
    }

    [Fact]
    public void Legacy_Hydrotesting_maps_to_BIG_HYDRO_with_flag()
    {
        var code = StationPrinterTarget.For(ManualTagStation.Hydrotesting, out var legacy);
        Assert.True(legacy);
        Assert.Equal(StationPrinterTarget.BigHydro, code);
    }

    [Fact]
    public async Task Cache_ttl_holds_stale_value_until_expiry()
    {
        var now = new DateTime(2026, 8, 31, 18, 0, 0, DateTimeKind.Utc);
        DateTime UtcNow() => now;

        var backing = new InMemoryStationPrinterBackingStore();
        backing.Save([new StationPrinterEndpoint(StationPrinterTarget.VisualRevisual, "192.168.0.125", 9100)]);

        var first = NewService(SharedRole(), backing, UtcNow);
        var second = NewService(SharedRole(), backing, UtcNow);

        Assert.Equal("192.168.0.125", first.Resolve(StationPrinterTarget.VisualRevisual).Address);
        Assert.Equal(1, backing.LoadCalls);

        await second.SaveAllAsync(
            [new StationPrinterEndpoint(StationPrinterTarget.VisualRevisual, "10.0.0.9", 9100)],
            CancellationToken.None);
        Assert.Equal("10.0.0.9", second.Resolve(StationPrinterTarget.VisualRevisual).Address);

        Assert.Equal("192.168.0.125", first.Resolve(StationPrinterTarget.VisualRevisual).Address);
        Assert.Equal(1, backing.LoadCalls);

        now = now.Add(StationPrinterSettingsService.CacheTtl).AddMilliseconds(1);
        Assert.Equal("10.0.0.9", first.Resolve(StationPrinterTarget.VisualRevisual).Address);
        Assert.True(backing.LoadCalls >= 2);
    }

    [Fact]
    public void Missing_or_blank_station_is_not_configured_and_does_not_use_another_station_or_mill()
    {
        var backing = new InMemoryStationPrinterBackingStore();
        backing.Save([
            new StationPrinterEndpoint(StationPrinterTarget.BigHydro, "10.1.1.1", 9100),
            new StationPrinterEndpoint(StationPrinterTarget.VisualRevisual, "", 9100)
        ]);
        var sut = NewService(
            SharedRole(),
            backing,
            () => DateTime.UtcNow,
            new NdtBundleOptions { NdtTagPrinterAddress = "192.168.0.125", NdtTagPrinterPort = 9100 });

        var visual = sut.Resolve(StationPrinterTarget.VisualRevisual);
        Assert.False(visual.Configured);
        Assert.Equal(string.Empty, visual.Address);

        var fourHead = sut.Resolve(StationPrinterTarget.FourHeadHydro);
        Assert.False(fourHead.Configured);
        Assert.Equal(string.Empty, fourHead.Address);

        var big = sut.Resolve(StationPrinterTarget.BigHydro);
        Assert.True(big.Configured);
        Assert.Equal("10.1.1.1", big.Address);
    }

    [Fact]
    public void Mill_instance_cannot_write_Station_Printer()
    {
        var mill = NewService(MillRole(), new InMemoryStationPrinterBackingStore(), () => DateTime.UtcNow);
        var ex = Assert.Throws<InvalidOperationException>(() =>
            mill.SaveAllAsync(
                    [new StationPrinterEndpoint(StationPrinterTarget.VisualRevisual, "10.0.0.2", 9100)],
                    CancellationToken.None)
                .GetAwaiter().GetResult());
        Assert.Contains("Shared-only", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Unknown_station_code_save_is_refused()
    {
        var sut = NewService(SharedRole(), new InMemoryStationPrinterBackingStore(), () => DateTime.UtcNow);
        var ex = Assert.Throws<InvalidOperationException>(() =>
            sut.SaveAllAsync(
                    [new StationPrinterEndpoint("VISUAL", "10.0.0.2", 9100)],
                    CancellationToken.None)
                .GetAwaiter().GetResult());
        Assert.Contains("Unknown station printer code", ex.Message, StringComparison.Ordinal);
    }

    private static StationPrinterSettingsService NewService(
        InstanceRoleOptions role,
        IStationPrinterBackingStore store,
        Func<DateTime> utcNow,
        NdtBundleOptions? options = null) =>
        new(
            new OptionsMonitorStub(options ?? new NdtBundleOptions()),
            Options.Create(role),
            NullLogger<StationPrinterSettingsService>.Instance,
            utcNow,
            store);

    private static InstanceRoleOptions SharedRole() => new()
    {
        Mode = InstanceRoleModes.Shared,
        OwnedMillNos = [],
        EnableDashboardApi = true,
        EnableMillWorkers = false,
        EnablePoPlanWipImport = true
    };

    private static InstanceRoleOptions MillRole() => new()
    {
        Mode = InstanceRoleModes.Mill,
        OwnedMillNos = [1],
        EnableDashboardApi = false,
        EnableMillWorkers = true,
        EnablePoPlanWipImport = false
    };

    private sealed class OptionsMonitorStub(NdtBundleOptions value) : IOptionsMonitor<NdtBundleOptions>
    {
        public NdtBundleOptions CurrentValue { get; } = value;
        public NdtBundleOptions Get(string? name) => CurrentValue;
        public IDisposable? OnChange(Action<NdtBundleOptions, string?> listener) => null;
    }
}
