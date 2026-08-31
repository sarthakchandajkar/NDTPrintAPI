using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class FillToTargetCutoverAndConfigTests
{
    [Fact]
    public void MillCsvBatchMode_defaults_constant_for_mills_2_to_4()
    {
        var opt = new NdtBundleOptions();
        Assert.True(MillCsvBatchModeResolver.Resolve(opt, 1).IsFillToTarget);
        Assert.True(MillCsvBatchModeResolver.Resolve(opt, 2).IsConstant);
        Assert.Equal("10001", MillCsvBatchModeResolver.Resolve(opt, 2).Value);
        Assert.True(MillCsvBatchModeResolver.Resolve(opt, 3).IsConstant);
        Assert.True(MillCsvBatchModeResolver.Resolve(opt, 4).IsConstant);
    }

    [Fact]
    public void MillCsvBatchMode_config_can_enable_fill_for_mill_2()
    {
        var opt = new NdtBundleOptions
        {
            MillCsvBatchMode =
            {
                ["2"] = new MillCsvBatchModeEntry { Mode = "FillToTarget" }
            }
        };
        Assert.True(MillCsvBatchModeResolver.Resolve(opt, 2).IsFillToTarget);
    }

    [Fact]
    public async Task FillCutoverStartupCheck_refuses_when_awaiting_recon()
    {
        var options = Options.Create(new NdtBundleOptions { RequireCleanFillCutover = true });
        var fill = new StubFill { Awaiting = true };
        var runtime = new StubRuntime { Unsafe = false };
        var sut = new FillCutoverStartupCheck(
            new OptionsMonitorStub(options.Value),
            fill,
            runtime,
            TestMillOwnership.Monolith(),
            NullLogger<FillCutoverStartupCheck>.Instance);

        await Assert.ThrowsAsync<InvalidOperationException>(() => sut.StartAsync(CancellationToken.None));
    }

    [Fact]
    public async Task FillCutoverStartupCheck_refuses_when_runtime_open()
    {
        var options = Options.Create(new NdtBundleOptions { RequireCleanFillCutover = true });
        var fill = new StubFill();
        var runtime = new StubRuntime { Unsafe = true };
        var sut = new FillCutoverStartupCheck(
            new OptionsMonitorStub(options.Value),
            fill,
            runtime,
            TestMillOwnership.Monolith(),
            NullLogger<FillCutoverStartupCheck>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => sut.StartAsync(CancellationToken.None));
        Assert.Contains("Bundle_Accumulation", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task FillCutoverStartupCheck_passes_when_clean()
    {
        var options = Options.Create(new NdtBundleOptions { RequireCleanFillCutover = true });
        var sut = new FillCutoverStartupCheck(
            new OptionsMonitorStub(options.Value),
            new StubFill(),
            new StubRuntime(),
            TestMillOwnership.Monolith(),
            NullLogger<FillCutoverStartupCheck>.Instance);

        await sut.StartAsync(CancellationToken.None);
    }

    [Fact]
    public async Task FillCutoverStartupCheck_skips_when_disabled()
    {
        var options = Options.Create(new NdtBundleOptions { RequireCleanFillCutover = false });
        var sut = new FillCutoverStartupCheck(
            new OptionsMonitorStub(options.Value),
            new StubFill { Awaiting = true },
            new StubRuntime { Unsafe = true },
            TestMillOwnership.Monolith(),
            NullLogger<FillCutoverStartupCheck>.Instance);

        await sut.StartAsync(CancellationToken.None);
    }

    private sealed class StubFill : ICsvFillService
    {
        public bool Awaiting { get; set; }
        public bool MissingTarget { get; set; }

        public Task TryInitializeFillTargetAsync(string bundleNo, int targetNdtPcs, string? closeSource, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task<CsvFillIncompleteBundle?> TryGetOldestIncompleteAsync(string poNumber, int millNo, string? pipeSize, CancellationToken cancellationToken) =>
            Task.FromResult<CsvFillIncompleteBundle?>(null);

        public Task<CsvFillStampResult?> TryStampFileAsync(string poNumber, int millNo, string? pipeSize, int fileNdtPipes, CancellationToken cancellationToken) =>
            Task.FromResult<CsvFillStampResult?>(null);

        public Task<int> AdvanceQuietShortAsync(string? poNumber, int? millNo, int quietMinutes, DateTime utcNow, bool forcePoEnd, CancellationToken cancellationToken) =>
            Task.FromResult(0);

        public Task UpsertHoldAsync(string sourceFileName, string poNumber, int millNo, string? pipeSize, string reasonCode, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task<int> EscalateExpiredHoldsAsync(
            int quietMinutes,
            DateTime utcNow,
            CancellationToken cancellationToken,
            int? millNo = null) =>
            Task.FromResult(0);

        public Task ApplyCountRevisionAsync(string sourceFileName, string batchNo, int oldNdtPipes, int newNdtPipes, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task<Guid> ApplyBatchMoveAsync(string sourceFileName, string oldBatchNo, string newBatchNo, int ndtPipes, CancellationToken cancellationToken) =>
            Task.FromResult(Guid.Empty);

        public Task<bool> HasAwaitingCsvReconRowsAsync(CancellationToken cancellationToken, int? millNo = null) =>
            Task.FromResult(Awaiting);

        public Task<bool> HasBundlesMissingFillTargetAsync(CancellationToken cancellationToken, int? millNo = null) =>
            Task.FromResult(MissingTarget);
    }

    private sealed class StubRuntime : INdtBundleRuntimeStateStore
    {
        public bool Unsafe { get; set; }

        public Task EnsureInitializedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public int GetBatchOffset(string poNumber, int millNo) => 0;
        public int GetRunningTotal(string poNumber, int millNo) => 0;
        public void ClearRunningTotal(string poNumber, int millNo) { }
        public void ClearOpenAccumulation(string poNumber, int millNo) { }
        public DateTime GetLastActivityUtc(string poNumber, int millNo) => DateTime.UtcNow;
        public void ApplySlitContribution(string poNumber, int millNo, int ndtPipes, int threshold, out int totalSoFar) =>
            totalSoFar = 0;
        public BundleCloseAllocation CloseBundle(string poNumber, int millNo, int closedTotalPcs, int threshold) =>
            new(1);
        public void AdvanceOnPoEnd(string poNumber, int millNo, int threshold) { }
        public Task SyncBatchSequencesFromBundlesAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public int GetEngineBatchNo(string poNumber, int millNo) => 0;
        public void SetEngineBatchNo(string poNumber, int millNo, int batchNo) { }
        public Dictionary<string, int> GetSizeCounts(string poNumber, int millNo) => new();
        public void SetSizeCounts(string poNumber, int millNo, IReadOnlyDictionary<string, int> counts) { }
        public Models.InputSlitRecord? GetLastRecord(string poNumber, int millNo) => null;
        public void SetLastRecord(string poNumber, int millNo, Models.InputSlitRecord? record) { }
        public Task SaveAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public bool HasUnsafeOpenStateForFillCutover(int? millNo = null) => Unsafe;
    }

    private sealed class OptionsMonitorStub : IOptionsMonitor<NdtBundleOptions>
    {
        private readonly NdtBundleOptions _value;
        public OptionsMonitorStub(NdtBundleOptions value) => _value = value;
        public NdtBundleOptions CurrentValue => _value;
        public NdtBundleOptions Get(string? name) => _value;
        public IDisposable? OnChange(Action<NdtBundleOptions, string?> listener) => null;
    }
}
