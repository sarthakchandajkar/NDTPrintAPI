using Microsoft.Extensions.Logging.Abstractions;
using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class NdtBundleEnginePoEndTests
{
    [Fact]
    public async Task HandlePoEnd_ClosesRunningTotalWhenSizeCountsEmpty()
    {
        var formation = new FormationChartProviderStub(new Dictionary<string, int> { ["Default"] = 20 });
        var pipeSize = new PipeSizeProviderStub(new Dictionary<string, string>());
        var runtime = new InMemoryRuntimeStateStore();
        var engine = TestEngineFactory.Create(formation, pipeSize, runtime);

        await runtime.EnsureInitializedAsync(CancellationToken.None);
        runtime.ApplySlitContribution("PO-100", 1, ndtPipes: 12, threshold: 20, out _, out _);

        var closed = new List<(int BatchNo, int Pcs)>();
        await engine.HandlePoEndAsync(
            "PO-100",
            1,
            (record, batchNo, total) =>
            {
                closed.Add((batchNo, total));
                return Task.CompletedTask;
            },
            CancellationToken.None);

        Assert.Single(closed);
        Assert.Equal(12, closed[0].Pcs);
        Assert.Equal(0, runtime.GetRunningTotal("PO-100", 1));
    }

    [Fact]
    public async Task PoEndWorkflow_AdvanceOnPoEnd_DoesNotBurnSequenceAfterHandlePoEnd()
    {
        var formation = new FormationChartProviderStub(new Dictionary<string, int> { ["Default"] = 20 });
        var pipeSize = new PipeSizeProviderStub(new Dictionary<string, string>());
        var runtime = new InMemoryRuntimeStateStore();
        var engine = TestEngineFactory.Create(formation, pipeSize, runtime);

        await runtime.EnsureInitializedAsync(CancellationToken.None);
        runtime.ApplySlitContribution("PO-100", 1, ndtPipes: 12, threshold: 20, out _, out _);

        await engine.HandlePoEndAsync(
            "PO-100",
            1,
            (_, _, _) => Task.CompletedTask,
            CancellationToken.None);

        runtime.AdvanceOnPoEnd("PO-100", 1, threshold: 20);

        Assert.Equal(1, runtime.GetEngineBatchNo("PO-100", 1));
        Assert.Equal(1, runtime.GetBatchOffset("PO-100", 1));
    }

    [Fact]
    public async Task HandlePoEnd_ClosesSizeCountPartials()
    {
        var formation = new FormationChartProviderStub(new Dictionary<string, int> { ["6"] = 20 });
        var pipeSize = new PipeSizeProviderStub(new Dictionary<string, string> { ["PO-200"] = "6" });
        var runtime = new InMemoryRuntimeStateStore();
        var engine = TestEngineFactory.Create(formation, pipeSize, runtime);

        await runtime.EnsureInitializedAsync(CancellationToken.None);
        runtime.SetSizeCounts("PO-200", 2, new Dictionary<string, int> { ["6"] = 15 });
        runtime.SetLastRecord("PO-200", 2, new InputSlitRecord
        {
            PoNumber = "PO-200",
            MillNo = 2,
            SlitNo = "01",
            NdtPipes = 15
        });

        var closed = new List<int>();
        await engine.HandlePoEndAsync(
            "PO-200",
            2,
            (_, _, total) =>
            {
                closed.Add(total);
                return Task.CompletedTask;
            },
            CancellationToken.None);

        Assert.Single(closed);
        Assert.Equal(15, closed[0]);
        Assert.Equal(0, runtime.GetSizeCounts("PO-200", 2)["6"]);
    }

    private sealed class FormationChartProviderStub : IFormationChartProvider
    {
        private readonly IReadOnlyDictionary<string, FormationChartEntry> _chart;

        public FormationChartProviderStub(IReadOnlyDictionary<string, int> thresholds)
        {
            _chart = thresholds.ToDictionary(
                kv => kv.Key,
                kv => new FormationChartEntry { PipeSize = kv.Key, RequiredNdtPcs = kv.Value },
                StringComparer.OrdinalIgnoreCase);
        }

        public Task<IReadOnlyDictionary<string, FormationChartEntry>> GetFormationChartAsync(CancellationToken cancellationToken) =>
            Task.FromResult(_chart);

        public void InvalidateCache() { }
    }

    private sealed class PipeSizeProviderStub : IPipeSizeProvider
    {
        private readonly IReadOnlyDictionary<string, string> _byPo;

        public PipeSizeProviderStub(IReadOnlyDictionary<string, string> byPo) => _byPo = byPo;

        public IReadOnlyDictionary<string, string>? TryGetCachedPipeSizes() => _byPo;

        public Task<string?> TryGetPipeSizeForPoAsync(string poNumber, CancellationToken cancellationToken)
        {
            _byPo.TryGetValue(poNumber, out var size);
            return Task.FromResult(size);
        }

        public Task<IReadOnlyDictionary<string, string>> GetPipeSizeByPoAsync(CancellationToken cancellationToken) =>
            Task.FromResult(_byPo);
    }

    private sealed class InMemoryRuntimeStateStore : INdtBundleRuntimeStateStore
    {
        private readonly Dictionary<string, PersistedSlot> _slots = new(StringComparer.OrdinalIgnoreCase);

        private sealed class PersistedSlot
        {
            public int BatchOffset;
            public int RunningTotal;
            public int EngineBatchNo;
            public Dictionary<string, int> SizeCounts = new(StringComparer.OrdinalIgnoreCase);
            public InputSlitRecord? LastRecord;
        }

        public Task EnsureInitializedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public int GetBatchOffset(string poNumber, int millNo) => Slot(poNumber, millNo).BatchOffset;

        public int GetRunningTotal(string poNumber, int millNo) => Slot(poNumber, millNo).RunningTotal;

        public void ClearRunningTotal(string poNumber, int millNo) => Slot(poNumber, millNo).RunningTotal = 0;
        public void ClearOpenAccumulation(string poNumber, int millNo) => ClearRunningTotal(poNumber, millNo);
        public DateTime GetLastActivityUtc(string poNumber, int millNo) => DateTime.UtcNow;

        public Task SyncBatchSequencesFromBundlesAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public void ApplySlitContribution(string poNumber, int millNo, int ndtPipes, int threshold, out int batchNumberForRow, out int totalSoFar)
        {
            var slot = Slot(poNumber, millNo);
            if (ndtPipes > 0)
                slot.RunningTotal += ndtPipes;
            totalSoFar = slot.RunningTotal;
            batchNumberForRow = slot.BatchOffset + 1;
            if (slot.RunningTotal >= threshold)
            {
                slot.BatchOffset += 1;
                slot.RunningTotal = 0;
            }
        }

        public BundleCloseAllocation CloseBundle(string poNumber, int millNo, int closedTotalPcs, int threshold)
        {
            var slot = Slot(poNumber, millNo);
            if (closedTotalPcs <= 0)
                return new BundleCloseAllocation(slot.EngineBatchNo, slot.EngineBatchNo + 1);
            var provisional = slot.EngineBatchNo + 1;
            slot.EngineBatchNo += 1;
            if (slot.BatchOffset < slot.EngineBatchNo)
                slot.BatchOffset = slot.EngineBatchNo;
            return new BundleCloseAllocation(slot.EngineBatchNo, provisional);
        }

        public void AdvanceOnPoEnd(string poNumber, int millNo, int threshold)
        {
            var slot = Slot(poNumber, millNo);
            slot.RunningTotal = 0;
            if (slot.BatchOffset < slot.EngineBatchNo)
                slot.BatchOffset = slot.EngineBatchNo;
        }

        public int GetEngineBatchNo(string poNumber, int millNo) => Slot(poNumber, millNo).EngineBatchNo;

        public void SetEngineBatchNo(string poNumber, int millNo, int batchNo) =>
            Slot(poNumber, millNo).EngineBatchNo = batchNo;

        public Dictionary<string, int> GetSizeCounts(string poNumber, int millNo) =>
            new(Slot(poNumber, millNo).SizeCounts, StringComparer.OrdinalIgnoreCase);

        public void SetSizeCounts(string poNumber, int millNo, IReadOnlyDictionary<string, int> counts) =>
            Slot(poNumber, millNo).SizeCounts = new Dictionary<string, int>(counts, StringComparer.OrdinalIgnoreCase);

        public InputSlitRecord? GetLastRecord(string poNumber, int millNo) => Slot(poNumber, millNo).LastRecord;

        public void SetLastRecord(string poNumber, int millNo, InputSlitRecord? record) =>
            Slot(poNumber, millNo).LastRecord = record;

        public Task SaveAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        private PersistedSlot Slot(string poNumber, int millNo)
        {
            var key = $"{InputSlitCsvParsing.NormalizePo(poNumber)}|{millNo}";
            if (!_slots.TryGetValue(key, out var slot))
            {
                slot = new PersistedSlot();
                _slots[key] = slot;
            }

            return slot;
        }
    }
}
