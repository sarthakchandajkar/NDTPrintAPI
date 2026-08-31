using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class NdtBundleRuntimeStateStoreTests
{
    [Fact]
    public void IncrementSizeCount_is_additive_and_never_keeps_zero()
    {
        var store = CreateStore(TestMillOwnership.Monolith());
        store.IncrementSizeCount("PO-100", 1, "Default", 8);
        store.IncrementSizeCount("PO-100", 1, "Default", 4);
        Assert.Equal(12, store.GetSizeCounts("PO-100", 1)["Default"]);
        Assert.Equal(12, store.GetRunningTotal("PO-100", 1));

        store.IncrementSizeCount("PO-100", 1, "Default", -12);
        Assert.Empty(store.GetSizeCounts("PO-100", 1));
        Assert.False(store.HasUnsafeOpenStateForFillCutover(1));
    }

    [Fact]
    public void SetSizeCounts_zero_removes_key()
    {
        var store = CreateStore(TestMillOwnership.Monolith());
        store.SetSizeCounts("PO-100", 1, new Dictionary<string, int> { ["Default"] = 5 });
        store.SetSizeCounts("PO-100", 1, new Dictionary<string, int> { ["Default"] = 0 });
        Assert.Empty(store.GetSizeCounts("PO-100", 1));
    }

    [Fact]
    public void LastRecord_and_size_counts_are_independent_slots()
    {
        var store = CreateStore(TestMillOwnership.Monolith());
        store.SetLastRecord("PO-A", 1, new InputSlitRecord { PoNumber = "PO-A", MillNo = 1, SlitNo = "S1" });
        store.IncrementSizeCount("PO-A", 1, "6", 3);
        Assert.Equal("S1", store.GetLastRecord("PO-A", 1)?.SlitNo);
        Assert.Equal(3, store.GetSizeCounts("PO-A", 1)["6"]);
        Assert.Empty(store.GetSizeCounts("PO-A", 2));
    }

    private static NdtBundleRuntimeStateStore CreateStore(IMillOwnership ownership) =>
        new(
            new OptionsMonitorStub(new NdtBundleOptions { UseSqlServerForBundles = false }),
            ownership,
            NullLogger<NdtBundleRuntimeStateStore>.Instance);

    private sealed class OptionsMonitorStub(NdtBundleOptions value) : IOptionsMonitor<NdtBundleOptions>
    {
        public NdtBundleOptions CurrentValue { get; } = value;
        public NdtBundleOptions Get(string? name) => CurrentValue;
        public IDisposable? OnChange(Action<NdtBundleOptions, string?> listener) => null;
    }
}
