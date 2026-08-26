using Microsoft.Extensions.Logging.Abstractions;
using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.PlcHandshake;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class OpenAccumulationOverrideTests
{
    [Fact]
    public async Task SetOpenAccumulation_updates_sizeCounts_and_clears_running_total()
    {
        var runtime = new FakeRuntimeStateStore();
        runtime.SetSizeCounts("1000060591", 1, new Dictionary<string, int> { ["Default"] = 5 });

        var service = new OpenAccumulationOverrideService(
            runtime,
            new FixedActivePoPerMill(new Dictionary<int, string> { [1] = "1000060591" }),
            new FixedPipeSizeProvider(new Dictionary<string, string>()),
            new FixedFormationChartProvider(new Dictionary<string, FormationChartEntry>
            {
                ["Default"] = new FormationChartEntry { PipeSize = "Default", RequiredNdtPcs = 80 }
            }),
            new FixedHooterValues(new MillHooterResolvedValues("1000060591", null, 80, 42)),
            new PlcHandshakeCoordinator(),
            NullLogger<OpenAccumulationOverrideService>.Instance);

        var result = await service.SetOpenAccumulationAsync(1, 42, null, null, CancellationToken.None);

        Assert.True(result.Success);
        Assert.Equal(5, result.PreviousAccumulated);
        Assert.Equal(42, result.NewAccumulated);
        Assert.Equal(42, runtime.GetSizeCounts("1000060591", 1)["Default"]);
        Assert.Equal(0, runtime.GetRunningTotal("1000060591", 1));
        Assert.True(runtime.SaveCalled);
    }

    [Fact]
    public async Task SetOpenAccumulation_requires_running_po_when_not_specified()
    {
        var service = new OpenAccumulationOverrideService(
            new FakeRuntimeStateStore(),
            new FixedActivePoPerMill(new Dictionary<int, string>()),
            new FixedPipeSizeProvider(new Dictionary<string, string>()),
            new FixedFormationChartProvider(new Dictionary<string, FormationChartEntry>()),
            new FixedHooterValues(MillHooterResolvedValues.Empty),
            new PlcHandshakeCoordinator(),
            NullLogger<OpenAccumulationOverrideService>.Instance);

        var result = await service.SetOpenAccumulationAsync(1, 10, null, null, CancellationToken.None);

        Assert.False(result.Success);
        Assert.Contains("No running PO", result.Message ?? string.Empty, StringComparison.Ordinal);
    }

    private sealed class FakeRuntimeStateStore : INdtBundleRuntimeStateStore
    {
        private readonly Dictionary<(string Po, int Mill), MillSlot> _slots = new();

        public bool SaveCalled { get; private set; }

        public Task EnsureInitializedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public int GetBatchOffset(string poNumber, int millNo) => 0;

        public int GetRunningTotal(string poNumber, int millNo) => Slot(poNumber, millNo).RunningTotal;

        public void ClearRunningTotal(string poNumber, int millNo) => Slot(poNumber, millNo).RunningTotal = 0;

        public void ClearOpenAccumulation(string poNumber, int millNo)
        {
            var slot = Slot(poNumber, millNo);
            slot.RunningTotal = 0;
            slot.SizeCounts.Clear();
        }

        public DateTime GetLastActivityUtc(string poNumber, int millNo) => DateTime.UtcNow;

        public void ApplySlitContribution(
            string poNumber,
            int millNo,
            int ndtPipes,
            int threshold,
            out int totalSoFar) =>
            throw new NotSupportedException();

        public BundleCloseAllocation CloseBundle(string poNumber, int millNo, int closedTotalPcs, int threshold) =>
            throw new NotSupportedException();

        public void AdvanceOnPoEnd(string poNumber, int millNo, int threshold) { }

        public Task SyncBatchSequencesFromBundlesAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public int GetEngineBatchNo(string poNumber, int millNo) => 0;

        public void SetEngineBatchNo(string poNumber, int millNo, int batchNo) { }

        public Dictionary<string, int> GetSizeCounts(string poNumber, int millNo) =>
            new(Slot(poNumber, millNo).SizeCounts, StringComparer.OrdinalIgnoreCase);

        public void SetSizeCounts(string poNumber, int millNo, IReadOnlyDictionary<string, int> counts) =>
            Slot(poNumber, millNo).SizeCounts = new Dictionary<string, int>(counts, StringComparer.OrdinalIgnoreCase);

        public InputSlitRecord? GetLastRecord(string poNumber, int millNo) => null;

        public void SetLastRecord(string poNumber, int millNo, InputSlitRecord? record) { }

        public Task SaveAsync(CancellationToken cancellationToken)
        {
            SaveCalled = true;
            return Task.CompletedTask;
        }

        private MillSlot Slot(string poNumber, int millNo)
        {
            var key = (InputSlitCsvParsing.NormalizePo(poNumber), millNo);
            if (!_slots.TryGetValue(key, out var slot))
                _slots[key] = slot = new MillSlot();
            return slot;
        }

        private sealed class MillSlot
        {
            public int RunningTotal { get; set; }
            public Dictionary<string, int> SizeCounts { get; set; } = new(StringComparer.OrdinalIgnoreCase);
        }
    }

    private sealed class FixedActivePoPerMill(IReadOnlyDictionary<int, string> map) : IActivePoPerMillService
    {
        public Task<IReadOnlyDictionary<int, string>> GetLatestPoByMillAsync(CancellationToken cancellationToken) =>
            Task.FromResult(map);

        public IReadOnlyList<string> GetInputSlitReadFolderPaths() => Array.Empty<string>();
    }

    private sealed class FixedPipeSizeProvider(IReadOnlyDictionary<string, string> map) : IPipeSizeProvider
    {
        public Task<IReadOnlyDictionary<string, string>> GetPipeSizeByPoAsync(CancellationToken cancellationToken) =>
            Task.FromResult(map);

        public IReadOnlyDictionary<string, string>? TryGetCachedPipeSizes() => map;

        public Task<string?> TryGetPipeSizeForPoAsync(string poNumber, CancellationToken cancellationToken) =>
            Task.FromResult(map.TryGetValue(poNumber, out var size) ? size : null);
    }

    private sealed class FixedFormationChartProvider(IReadOnlyDictionary<string, FormationChartEntry> chart)
        : IFormationChartProvider
    {
        public Task<IReadOnlyDictionary<string, FormationChartEntry>> GetFormationChartAsync(
            CancellationToken cancellationToken) => Task.FromResult(chart);

        public void InvalidateCache() { }
    }

    private sealed class FixedHooterValues(MillHooterResolvedValues values) : IMillHooterPlcValuesService
    {
        public Task<MillHooterResolvedValues> ResolveAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult(values);
    }
}
