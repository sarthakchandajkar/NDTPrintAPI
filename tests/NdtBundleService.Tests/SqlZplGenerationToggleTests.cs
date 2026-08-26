using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class SqlZplGenerationToggleTests
{
    [Fact]
    public void Cache_ttl_holds_stale_value_until_expiry_then_propagates_across_instances()
    {
        var now = new DateTime(2026, 8, 26, 12, 0, 0, DateTimeKind.Utc);
        DateTime UtcNow() => now;

        var store = new InMemoryAppSettings();
        store.Values[AppSettingRepository.ZplPhysicalPrintEnabledKey] = "true";

        var options = new OptionsMonitorStub(new NdtBundleOptions { EnableNdtTagZplAndPrint = true });

        // Two process instances sharing SQL (same store).
        var millToggle = new SqlZplGenerationToggle(
            store,
            options,
            NullLogger<SqlZplGenerationToggle>.Instance,
            UtcNow);
        var sharedToggle = new SqlZplGenerationToggle(
            store,
            options,
            NullLogger<SqlZplGenerationToggle>.Instance,
            UtcNow);

        Assert.True(millToggle.IsEnabled);
        Assert.Equal(1, store.GetCalls);

        // Shared dashboard flips print off — persists immediately on that instance.
        Assert.False(sharedToggle.SetEnabled(false));
        Assert.False(sharedToggle.IsEnabled);

        // Mill instance still sees cached true within the 2s TTL window.
        Assert.True(millToggle.IsEnabled);
        Assert.Equal(1, store.GetCalls); // no re-read yet

        // After TTL, mill observes the cross-instance write (~2s propagation).
        now = now.Add(SqlZplGenerationToggle.CacheTtl).AddMilliseconds(1);
        Assert.False(millToggle.IsEnabled);
        Assert.True(store.GetCalls >= 2);
    }

    private sealed class InMemoryAppSettings : IAppSettingRepository
    {
        public Dictionary<string, string> Values { get; } = new(StringComparer.OrdinalIgnoreCase);
        public int GetCalls { get; private set; }

        public Task<string?> GetValueAsync(string key, CancellationToken cancellationToken)
        {
            GetCalls++;
            Values.TryGetValue(key, out var v);
            return Task.FromResult<string?>(v);
        }

        public Task SetValueAsync(string key, string value, string? updatedBy, CancellationToken cancellationToken)
        {
            Values[key] = value;
            return Task.CompletedTask;
        }
    }

    private sealed class OptionsMonitorStub(NdtBundleOptions value) : IOptionsMonitor<NdtBundleOptions>
    {
        public NdtBundleOptions CurrentValue { get; } = value;
        public NdtBundleOptions Get(string? name) => CurrentValue;
        public IDisposable? OnChange(Action<NdtBundleOptions, string?> listener) => null;
    }
}
