using System.Collections.Concurrent;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.InstanceLease;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class MillInstanceLeaseTests
{
    [Fact]
    public async Task TryClaim_when_sql_disabled_succeeds_for_dev()
    {
        var options = Options.Create(new NdtBundleOptions
        {
            UseSqlServerForBundles = false,
            ConnectionString = ""
        });
        var sut = new MillInstanceLeaseService(
            new OptionsMonitorStub(options.Value),
            NullLogger<MillInstanceLeaseService>.Instance);

        var result = await sut.TryClaimAsync(1, "test", 45, CancellationToken.None);
        Assert.True(result.Claimed);
    }

    [Fact]
    public async Task LeaseHostedService_fails_startup_when_claim_denied()
    {
        var lease = new DenyingLeaseService();
        var role = Options.Create(new InstanceRoleOptions
        {
            Mode = InstanceRoleModes.Mill,
            OwnedMillNos = [1],
            EnableMillWorkers = true,
            EnableDashboardApi = false,
            EnablePoPlanWipImport = false,
            EnableUploadScheduler = false,
            InstanceDisplayName = "Mill-1"
        });

        var sut = new MillInstanceLeaseHostedService(
            lease,
            TestMillOwnership.Mill(1),
            new OptionsMonitorStubRole(role.Value),
            new RecordingHostLifetime(),
            NullLogger<MillInstanceLeaseHostedService>.Instance);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => sut.StartAsync(CancellationToken.None));
        Assert.Contains("already owned", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("mill 1", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task RenewMillWithRetries_lost_lease_stops_host_immediately()
    {
        var lease = new ScriptedLeaseService(renew: _ => MillLeaseRenewOutcome.LostLease);
        var lifetime = new RecordingHostLifetime();
        var sut = CreateHosted(lease, lifetime, transientAttempts: 3, transientDelaySeconds: 0);

        var ok = await sut.RenewMillWithRetriesAsync(
            mill: 1,
            ttl: 45,
            maxTransientAttempts: 3,
            transientDelay: TimeSpan.Zero,
            CancellationToken.None);

        Assert.False(ok);
        Assert.Equal(1, lifetime.StopCount);
        Assert.Equal(1, lease.RenewCalls);
    }

    [Fact]
    public async Task RenewMillWithRetries_transient_errors_retry_then_stop_host()
    {
        var lease = new ScriptedLeaseService(renew: _ => throw new InvalidOperationException("SQL blip"));
        var lifetime = new RecordingHostLifetime();
        var sut = CreateHosted(lease, lifetime, transientAttempts: 3, transientDelaySeconds: 0);

        var ok = await sut.RenewMillWithRetriesAsync(
            mill: 1,
            ttl: 45,
            maxTransientAttempts: 3,
            transientDelay: TimeSpan.Zero,
            CancellationToken.None);

        Assert.False(ok);
        Assert.Equal(1, lifetime.StopCount);
        Assert.Equal(3, lease.RenewCalls);
    }

    [Fact]
    public async Task RenewMillWithRetries_transient_then_success_does_not_stop_host()
    {
        var calls = 0;
        var lease = new ScriptedLeaseService(renew: _ =>
        {
            calls++;
            if (calls < 2)
                throw new InvalidOperationException("SQL blip");
            return MillLeaseRenewOutcome.Renewed;
        });
        var lifetime = new RecordingHostLifetime();
        var sut = CreateHosted(lease, lifetime, transientAttempts: 3, transientDelaySeconds: 0);

        var ok = await sut.RenewMillWithRetriesAsync(
            mill: 1,
            ttl: 45,
            maxTransientAttempts: 3,
            transientDelay: TimeSpan.Zero,
            CancellationToken.None);

        Assert.True(ok);
        Assert.Equal(0, lifetime.StopCount);
        Assert.Equal(2, lease.RenewCalls);
    }

    [Fact]
    public async Task TryClaim_two_parallel_claims_exactly_one_succeeds()
    {
        var connectionString = Environment.GetEnvironmentVariable("NDT_LEASE_TEST_CONNECTION");
        if (!string.IsNullOrWhiteSpace(connectionString))
        {
            await AssertSqlParallelClaimExactlyOneAsync(connectionString).ConfigureAwait(false);
            return;
        }

        var store = new ConcurrentDictionary<int, Guid>();
        var a = new InMemoryLease(store);
        var b = new InMemoryLease(store);
        const int millNo = 2;

        var barrier = new Barrier(2);
        MillLeaseClaimResult? resultA = null;
        MillLeaseClaimResult? resultB = null;

        var taskA = Task.Run(() =>
        {
            barrier.SignalAndWait();
            resultA = a.TryClaim(millNo);
        });
        var taskB = Task.Run(() =>
        {
            barrier.SignalAndWait();
            resultB = b.TryClaim(millNo);
        });

        await Task.WhenAll(taskA, taskB);

        Assert.NotNull(resultA);
        Assert.NotNull(resultB);
        Assert.Equal(1, (resultA!.Claimed ? 1 : 0) + (resultB!.Claimed ? 1 : 0));
    }

    private static MillInstanceLeaseHostedService CreateHosted(
        IMillInstanceLeaseService lease,
        IHostApplicationLifetime lifetime,
        int transientAttempts,
        int transientDelaySeconds)
    {
        var role = new InstanceRoleOptions
        {
            Mode = InstanceRoleModes.Mill,
            OwnedMillNos = [1],
            EnableMillWorkers = true,
            EnableDashboardApi = false,
            EnablePoPlanWipImport = false,
            EnableUploadScheduler = false,
            InstanceDisplayName = "Mill-1",
            LeaseRenewMaxTransientAttempts = transientAttempts,
            LeaseRenewTransientRetryDelaySeconds = transientDelaySeconds
        };

        return new MillInstanceLeaseHostedService(
            lease,
            TestMillOwnership.Mill(1),
            new OptionsMonitorStubRole(role),
            lifetime,
            NullLogger<MillInstanceLeaseHostedService>.Instance);
    }

    private static async Task AssertSqlParallelClaimExactlyOneAsync(string connectionString)
    {
        var options = new NdtBundleOptions
        {
            UseSqlServerForBundles = true,
            ConnectionString = connectionString
        };

        var a = new MillInstanceLeaseService(
            new OptionsMonitorStub(options),
            NullLogger<MillInstanceLeaseService>.Instance);
        var b = new MillInstanceLeaseService(
            new OptionsMonitorStub(options),
            NullLogger<MillInstanceLeaseService>.Instance);

        const int millNo = 4;
        await using var conn = new Microsoft.Data.SqlClient.SqlConnection(connectionString);
        await conn.OpenAsync();
        await using (var clear = new Microsoft.Data.SqlClient.SqlCommand(
                         "DELETE FROM dbo.Mill_Instance_Lease WHERE Mill_No = @Mill;", conn))
        {
            clear.Parameters.AddWithValue("@Mill", millNo);
            await clear.ExecuteNonQueryAsync();
        }

        var barrier = new Barrier(2);
        MillLeaseClaimResult? resultA = null;
        MillLeaseClaimResult? resultB = null;

        var taskA = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            resultA = await a.TryClaimAsync(millNo, "A", 45, CancellationToken.None);
        });
        var taskB = Task.Run(async () =>
        {
            barrier.SignalAndWait();
            resultB = await b.TryClaimAsync(millNo, "B", 45, CancellationToken.None);
        });

        await Task.WhenAll(taskA, taskB);

        Assert.Equal(1, (resultA!.Claimed ? 1 : 0) + (resultB!.Claimed ? 1 : 0));

        if (resultA.Claimed)
            await a.ReleaseAsync(millNo, CancellationToken.None);
        if (resultB.Claimed)
            await b.ReleaseAsync(millNo, CancellationToken.None);
    }

    private sealed class RecordingHostLifetime : IHostApplicationLifetime
    {
        public int StopCount { get; private set; }
        public CancellationToken ApplicationStarted => CancellationToken.None;
        public CancellationToken ApplicationStopping => CancellationToken.None;
        public CancellationToken ApplicationStopped => CancellationToken.None;
        public void StopApplication() => StopCount++;
    }

    private sealed class ScriptedLeaseService : IMillInstanceLeaseService
    {
        private readonly Func<int, MillLeaseRenewOutcome> _renew;
        public int RenewCalls { get; private set; }
        public Guid InstanceId { get; } = Guid.NewGuid();

        public ScriptedLeaseService(Func<int, MillLeaseRenewOutcome> renew) => _renew = renew;

        public Task<MillLeaseClaimResult> TryClaimAsync(
            int millNo,
            string? serviceName,
            int ttlSeconds,
            CancellationToken cancellationToken) =>
            Task.FromResult(new MillLeaseClaimResult { Claimed = true });

        public Task<MillLeaseRenewOutcome> TryRenewAsync(int millNo, int ttlSeconds, CancellationToken cancellationToken)
        {
            RenewCalls++;
            return Task.FromResult(_renew(millNo));
        }

        public Task ReleaseAsync(int millNo, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class InMemoryLease
    {
        private readonly ConcurrentDictionary<int, Guid> _store;
        private readonly Guid _id = Guid.NewGuid();

        public InMemoryLease(ConcurrentDictionary<int, Guid> store) => _store = store;

        public MillLeaseClaimResult TryClaim(int millNo)
        {
            while (true)
            {
                if (_store.TryAdd(millNo, _id))
                    return new MillLeaseClaimResult { Claimed = true };

                if (_store.TryGetValue(millNo, out var holder))
                {
                    if (holder == _id)
                        return new MillLeaseClaimResult { Claimed = true };

                    return new MillLeaseClaimResult
                    {
                        Claimed = false,
                        HolderInstanceId = holder,
                        HolderMachineName = "in-memory"
                    };
                }
            }
        }
    }

    private sealed class DenyingLeaseService : IMillInstanceLeaseService
    {
        public Guid InstanceId { get; } = Guid.NewGuid();

        public Task<MillLeaseClaimResult> TryClaimAsync(
            int millNo,
            string? serviceName,
            int ttlSeconds,
            CancellationToken cancellationToken) =>
            Task.FromResult(new MillLeaseClaimResult
            {
                Claimed = false,
                HolderMachineName = "OTHER-HOST",
                HolderServiceName = "NdtBundleService-M1",
                HolderInstanceId = Guid.NewGuid()
            });

        public Task<MillLeaseRenewOutcome> TryRenewAsync(int millNo, int ttlSeconds, CancellationToken cancellationToken) =>
            Task.FromResult(MillLeaseRenewOutcome.Renewed);

        public Task ReleaseAsync(int millNo, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class OptionsMonitorStub : IOptionsMonitor<NdtBundleOptions>
    {
        private readonly NdtBundleOptions _value;
        public OptionsMonitorStub(NdtBundleOptions value) => _value = value;
        public NdtBundleOptions CurrentValue => _value;
        public NdtBundleOptions Get(string? name) => _value;
        public IDisposable? OnChange(Action<NdtBundleOptions, string?> listener) => null;
    }

    private sealed class OptionsMonitorStubRole : IOptionsMonitor<InstanceRoleOptions>
    {
        private readonly InstanceRoleOptions _value;
        public OptionsMonitorStubRole(InstanceRoleOptions value) => _value = value;
        public InstanceRoleOptions CurrentValue => _value;
        public InstanceRoleOptions Get(string? name) => _value;
        public IDisposable? OnChange(Action<InstanceRoleOptions, string?> listener) => null;
    }
}
