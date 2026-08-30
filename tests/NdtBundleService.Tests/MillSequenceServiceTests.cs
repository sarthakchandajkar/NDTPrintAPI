using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class MillSequenceServiceTests
{
    [Fact]
    public void ShouldRefuseBelowLiveMax_unless_forced()
    {
        Assert.True(MillSequenceService.ShouldRefuseBelowLiveMax(5, liveMax: 10, forceBelowLiveMax: false));
        Assert.False(MillSequenceService.ShouldRefuseBelowLiveMax(5, liveMax: 10, forceBelowLiveMax: true));
        Assert.False(MillSequenceService.ShouldRefuseBelowLiveMax(10, liveMax: 10, forceBelowLiveMax: false));
        Assert.False(MillSequenceService.ShouldRefuseBelowLiveMax(11, liveMax: 10, forceBelowLiveMax: false));
    }

    [Fact]
    public void IsLargeJump_at_100()
    {
        Assert.False(MillSequenceService.IsLargeJump(10, 109));
        Assert.True(MillSequenceService.IsLargeJump(10, 110));
        Assert.True(MillSequenceService.IsLargeJump(200, 50));
    }

    [Fact]
    public async Task StartupGuard_refuses_when_scan_exceeds_table()
    {
        var seq = new StubMillSequence
        {
            Enabled = true,
            ScanExceeds = true
        };
        var sut = new MillSequenceStartupGuard(
            new OptionsMonitorStub(new NdtBundleOptions { RequireMillSequenceMatchesBundles = true }),
            seq,
            TestMillOwnership.Monolith(),
            NullLogger<MillSequenceStartupGuard>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => sut.StartAsync(CancellationToken.None));
        Assert.Contains("live bundles", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.True(seq.Seeded);
    }

    [Fact]
    public async Task StartupGuard_allows_scan_below_table()
    {
        var seq = new StubMillSequence { Enabled = true };
        var sut = new MillSequenceStartupGuard(
            new OptionsMonitorStub(new NdtBundleOptions { RequireMillSequenceMatchesBundles = true }),
            seq,
            TestMillOwnership.Monolith(),
            NullLogger<MillSequenceStartupGuard>.Instance);

        await sut.StartAsync(CancellationToken.None);
        Assert.True(seq.Seeded);
        Assert.Equal(new[] { 1, 2, 3, 4 }, seq.GuardedMills);
    }

    [Fact]
    public async Task StartupGuard_skips_scan_when_config_false()
    {
        var seq = new StubMillSequence { Enabled = true, ScanExceeds = true };
        var sut = new MillSequenceStartupGuard(
            new OptionsMonitorStub(new NdtBundleOptions { RequireMillSequenceMatchesBundles = false }),
            seq,
            TestMillOwnership.Monolith(),
            NullLogger<MillSequenceStartupGuard>.Instance);

        await sut.StartAsync(CancellationToken.None);
        Assert.True(seq.Seeded);
        Assert.Empty(seq.GuardedMills);
    }

    [Fact]
    public async Task StartupGuard_skips_when_sql_disabled()
    {
        var seq = new StubMillSequence { Enabled = false };
        var sut = new MillSequenceStartupGuard(
            new OptionsMonitorStub(new NdtBundleOptions { RequireMillSequenceMatchesBundles = true }),
            seq,
            TestMillOwnership.Monolith(),
            NullLogger<MillSequenceStartupGuard>.Instance);

        await sut.StartAsync(CancellationToken.None);
        Assert.False(seq.Seeded);
    }

    [Fact]
    public async Task StartupGuard_refuses_sql_off_when_printer_configured()
    {
        var seq = new StubMillSequence { Enabled = false };
        var sut = new MillSequenceStartupGuard(
            new OptionsMonitorStub(new NdtBundleOptions
            {
                EnableNdtTagZplAndPrint = true,
                NdtTagPrinterAddress = "192.168.0.125"
            }),
            seq,
            TestMillOwnership.Monolith(),
            NullLogger<MillSequenceStartupGuard>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => sut.StartAsync(CancellationToken.None));
        Assert.Contains("12YYM00000", ex.Message, StringComparison.Ordinal);
        Assert.Contains("EnableNdtTagZplAndPrint", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task StartupGuard_allows_sql_off_when_zpl_print_disabled()
    {
        var seq = new StubMillSequence { Enabled = false };
        var sut = new MillSequenceStartupGuard(
            new OptionsMonitorStub(new NdtBundleOptions
            {
                EnableNdtTagZplAndPrint = false,
                NdtTagPrinterAddress = "192.168.0.125"
            }),
            seq,
            TestMillOwnership.Monolith(),
            NullLogger<MillSequenceStartupGuard>.Instance);

        await sut.StartAsync(CancellationToken.None);
        Assert.False(seq.Seeded);
    }

    [Fact]
    public void SqlOffPrintGuard_printer_placeholder_is_not_configured()
    {
        Assert.False(SqlOffPrintGuard.PrinterWouldPrint(new NdtBundleOptions
        {
            EnableNdtTagZplAndPrint = true,
            NdtTagPrinterAddress = "0.0.0.0"
        }));
        Assert.True(SqlOffPrintGuard.MustRefuseStart(new NdtBundleOptions
        {
            UseSqlServerForBundles = false,
            EnableNdtTagZplAndPrint = true,
            NdtTagPrinterAddress = "192.168.0.125"
        }));
    }

    private sealed class StubMillSequence : IMillSequenceService
    {
        public bool Enabled { get; set; }
        public bool ScanExceeds { get; set; }
        public bool Seeded { get; private set; }
        public List<int> GuardedMills { get; } = [];

        public bool IsEnabled => Enabled;

        public Task SeedMissingRowsAsync(CancellationToken cancellationToken)
        {
            Seeded = true;
            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<MillSequenceSnapshot>> GetSnapshotsAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<MillSequenceSnapshot>>(Array.Empty<MillSequenceSnapshot>());

        public Task<MillSequenceSnapshot?> GetSnapshotAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<MillSequenceSnapshot?>(null);

        public Task<int> GetLiveMaxSequenceAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult(0);

        public Task<int> AllocateNextInTxAsync(
            Microsoft.Data.SqlClient.SqlConnection conn,
            Microsoft.Data.SqlClient.SqlTransaction tx,
            int millNo,
            string updatedBy,
            string reason,
            CancellationToken cancellationToken) =>
            Task.FromResult(0);

        public Task<MillSequenceSetResult> SetCurrentSequenceAsync(
            int millNo,
            int currentSequence,
            string reason,
            string updatedBy,
            bool forceBelowLiveMax,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task<bool> TryRollbackIfHighestInTxAsync(
            Microsoft.Data.SqlClient.SqlConnection conn,
            Microsoft.Data.SqlClient.SqlTransaction tx,
            int millNo,
            int sourceSequence,
            string updatedBy,
            string reason,
            CancellationToken cancellationToken) =>
            Task.FromResult(false);

        public Task<(int Sequence, string Formatted)> AllocateAndInsertBundleAsync(
            NdtBundleRecord pending,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        public Task EnsureScanDoesNotExceedTableAsync(int millNo, CancellationToken cancellationToken)
        {
            GuardedMills.Add(millNo);
            if (ScanExceeds)
                throw new InvalidOperationException($"Mill_Sequence for mill {millNo} is 1; live bundles go to 2.");
            return Task.CompletedTask;
        }
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
