using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.InstanceLease;
using NdtBundleService.Services.MillInstanceStatus;
using NdtBundleService.Services.PlcHandshake;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class MillInstanceStatusTests
{
    [Fact]
    public void Stale_row_is_reported_disconnected_without_clearing_counts()
    {
        var now = new DateTimeOffset(2026, 9, 22, 18, 0, 0, TimeSpan.Zero);
        var row = new PlcHandshakeMillStatus
        {
            MillNo = 1,
            MillName = "Mill-1",
            Connected = true,
            PlcConnectionEnabled = true,
            NdtCount = 80,
            LastUpdateUtc = now.AddSeconds(-6)
        };

        var applied = MillInstanceStatusFreshness.Apply(row, now);
        Assert.False(applied.Connected);
        Assert.Equal(80, applied.NdtCount);
        Assert.Contains("stale", applied.LastError, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PadAndApplyStale_fills_missing_mills()
    {
        var now = DateTimeOffset.UtcNow;
        var padded = MillInstanceStatusFreshness.PadAndApplyStale(
            [new PlcHandshakeMillStatus { MillNo = 1, MillName = "Mill-1", Connected = true, LastUpdateUtc = now }],
            now);

        Assert.Equal(4, padded.Count);
        Assert.Equal(new[] { 1, 2, 3, 4 }, padded.Select(m => m.MillNo).ToArray());
        Assert.True(padded[0].Connected);
        Assert.False(padded[1].Connected);
    }

    [Fact]
    public void Snapshot_prefers_local_registry_on_mill_with_handshake()
    {
        var registry = new PlcHandshakeStatusRegistry();
        registry.RegisterMill(1, new PlcHandshakeMillStatus
        {
            MillNo = 1,
            Connected = true,
            NdtCount = 42,
            PlcConnectionEnabled = true
        });
        var store = new InMemoryMillInstanceStatusStore();
        store.Upsert(
            new PlcHandshakeMillStatus { MillNo = 1, Connected = true, NdtCount = 1 },
            Guid.NewGuid(),
            "m",
            "Mill-1");

        var sut = new PlcLiveSnapshotService(
            registry,
            store,
            new BundleMonitor(new NdtBundleOptions { PlcHandshake = new PlcHandshakeOptions { Enabled = true } }),
            new RoleMonitor(MillRole()),
            () => DateTimeOffset.UtcNow);

        var snap = sut.GetSnapshot();
        Assert.Equal(PlcLiveSnapshotService.SourceLocal, snap.Source);
        Assert.True(snap.PlcHandshakeEnabled);
        Assert.Equal(42, snap.Mills.Single().NdtCount);
    }

    [Fact]
    public void Snapshot_on_Shared_reads_sql_store_not_empty_local_registry()
    {
        var registry = new PlcHandshakeStatusRegistry();
        var store = new InMemoryMillInstanceStatusStore();
        store.Upsert(
            new PlcHandshakeMillStatus
            {
                MillNo = 1,
                MillName = "Mill-1",
                Connected = true,
                PlcConnectionEnabled = true,
                NdtCount = 80,
                LastUpdateUtc = DateTimeOffset.UtcNow
            },
            Guid.NewGuid(),
            "vm",
            "Mill-1");

        var sut = new PlcLiveSnapshotService(
            registry,
            store,
            new BundleMonitor(new NdtBundleOptions { PlcHandshake = new PlcHandshakeOptions { Enabled = false } }),
            new RoleMonitor(SharedRole()),
            () => DateTimeOffset.UtcNow);

        var snap = sut.GetSnapshot();
        Assert.Equal(PlcLiveSnapshotService.SourceSql, snap.Source);
        Assert.True(snap.PlcHandshakeEnabled);
        Assert.Equal(80, snap.Mills.Single(m => m.MillNo == 1).NdtCount);
        Assert.False(snap.Mills.Single(m => m.MillNo == 2).Connected);
    }

    [Fact]
    public void Snapshot_mill_with_handshake_but_empty_registry_uses_sql()
    {
        var store = new InMemoryMillInstanceStatusStore();
        store.Upsert(
            new PlcHandshakeMillStatus
            {
                MillNo = 2,
                MillName = "Mill-2",
                Connected = true,
                PlcConnectionEnabled = true,
                NdtCount = 11,
                LastUpdateUtc = DateTimeOffset.UtcNow
            },
            Guid.NewGuid(),
            "vm",
            "Mill-2");

        var sut = new PlcLiveSnapshotService(
            new PlcHandshakeStatusRegistry(),
            store,
            new BundleMonitor(new NdtBundleOptions { PlcHandshake = new PlcHandshakeOptions { Enabled = true } }),
            new RoleMonitor(new InstanceRoleOptions
            {
                Mode = InstanceRoleModes.Mill,
                OwnedMillNos = [2],
                EnableMillWorkers = true
            }),
            () => DateTimeOffset.UtcNow);

        var snap = sut.GetSnapshot();
        Assert.Equal(PlcLiveSnapshotService.SourceSql, snap.Source);
        Assert.Equal(11, snap.Mills.Single(m => m.MillNo == 2).NdtCount);
    }

    [Fact]
    public void Snapshot_monolith_without_handshake_stays_socket_fallback()
    {
        var sut = new PlcLiveSnapshotService(
            new PlcHandshakeStatusRegistry(),
            new InMemoryMillInstanceStatusStore(),
            new BundleMonitor(new NdtBundleOptions { PlcHandshake = new PlcHandshakeOptions { Enabled = false } }),
            new RoleMonitor(new InstanceRoleOptions { Mode = InstanceRoleModes.Monolith }),
            () => DateTimeOffset.UtcNow);

        var snap = sut.GetSnapshot();
        Assert.Equal(PlcLiveSnapshotService.SourceNone, snap.Source);
        Assert.False(snap.PlcHandshakeEnabled);
    }

    [Fact]
    public void Publisher_writes_owned_mill_only_and_survives_store_errors()
    {
        var registry = new PlcHandshakeStatusRegistry();
        registry.RegisterMill(1, new PlcHandshakeMillStatus
        {
            MillNo = 1,
            Connected = true,
            NdtCount = 7,
            PlcConnectionEnabled = true
        });
        var store = new InMemoryMillInstanceStatusStore();
        var publisher = NewPublisher(store, registry, TestMillOwnership.Mill(1), MillRole());
        publisher.PublishOwned(connectedOverride: null);

        var rows = store.LoadAll();
        Assert.Single(rows);
        Assert.Equal(1, rows[0].MillNo);
        Assert.Equal(7, rows[0].NdtCount);

        var throwing = new ThrowingStore();
        var resilient = NewPublisher(throwing, registry, TestMillOwnership.Mill(1), MillRole());
        resilient.PublishOwned(connectedOverride: null);
        Assert.True(throwing.Called);
    }

    private static MillInstanceStatusPublisher NewPublisher(
        IMillInstanceStatusStore store,
        PlcHandshakeStatusRegistry registry,
        IMillOwnership ownership,
        InstanceRoleOptions role) =>
        new(
            store,
            registry,
            ownership,
            new StubLease(),
            new RoleMonitor(role),
            new BundleMonitor(new NdtBundleOptions()),
            NullLogger<MillInstanceStatusPublisher>.Instance);

    private static InstanceRoleOptions MillRole() => new()
    {
        Mode = InstanceRoleModes.Mill,
        OwnedMillNos = [1],
        EnableMillWorkers = true,
        EnableDashboardApi = false,
        EnablePoPlanWipImport = false,
        InstanceDisplayName = "Mill-1"
    };

    private static InstanceRoleOptions SharedRole() => new()
    {
        Mode = InstanceRoleModes.Shared,
        OwnedMillNos = [],
        EnableMillWorkers = false,
        EnableDashboardApi = true,
        EnablePoPlanWipImport = true
    };

    private sealed class StubLease : IMillInstanceLeaseService
    {
        public Guid InstanceId { get; } = Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");

        public Task<MillLeaseClaimResult> TryClaimAsync(
            int millNo, string? serviceName, int ttlSeconds, CancellationToken cancellationToken) =>
            Task.FromResult(new MillLeaseClaimResult { Claimed = true });

        public Task<MillLeaseRenewOutcome> TryRenewAsync(int millNo, int ttlSeconds, CancellationToken cancellationToken) =>
            Task.FromResult(MillLeaseRenewOutcome.Renewed);

        public Task ReleaseAsync(int millNo, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class ThrowingStore : IMillInstanceStatusStore
    {
        public bool Called { get; private set; }

        public IReadOnlyList<PlcHandshakeMillStatus> LoadAll() => [];

        public void Upsert(PlcHandshakeMillStatus status, Guid instanceId, string machineName, string? serviceName)
        {
            Called = true;
            throw new InvalidOperationException("sql down");
        }
    }

    private sealed class BundleMonitor(NdtBundleOptions value) : IOptionsMonitor<NdtBundleOptions>
    {
        public NdtBundleOptions CurrentValue { get; } = value;
        public NdtBundleOptions Get(string? name) => CurrentValue;
        public IDisposable? OnChange(Action<NdtBundleOptions, string?> listener) => null;
    }

    private sealed class RoleMonitor(InstanceRoleOptions value) : IOptionsMonitor<InstanceRoleOptions>
    {
        public InstanceRoleOptions CurrentValue { get; } = value;
        public InstanceRoleOptions Get(string? name) => CurrentValue;
        public IDisposable? OnChange(Action<InstanceRoleOptions, string?> listener) => null;
    }
}
