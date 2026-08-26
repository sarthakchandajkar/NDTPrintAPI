using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services.InstanceLease;

/// <summary>
/// Claims mill lease(s) at startup (fail-fast if another live holder exists), renews while running, releases on stop.
/// Lost lease or exhausted transient renew retries call <see cref="IHostApplicationLifetime.StopApplication"/>.
/// </summary>
public sealed class MillInstanceLeaseHostedService : IHostedService, IDisposable
{
    private readonly IMillInstanceLeaseService _lease;
    private readonly IMillOwnership _ownership;
    private readonly IOptionsMonitor<InstanceRoleOptions> _role;
    private readonly IHostApplicationLifetime _lifetime;
    private readonly ILogger<MillInstanceLeaseHostedService> _logger;
    private readonly List<int> _claimedMills = new();
    private CancellationTokenSource? _renewCts;
    private Task? _renewTask;

    public MillInstanceLeaseHostedService(
        IMillInstanceLeaseService lease,
        IMillOwnership ownership,
        IOptionsMonitor<InstanceRoleOptions> role,
        IHostApplicationLifetime lifetime,
        ILogger<MillInstanceLeaseHostedService> logger)
    {
        _lease = lease;
        _ownership = ownership;
        _role = role;
        _lifetime = lifetime;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var role = _role.CurrentValue;
        var mills = role.IsMonolith
            ? new[] { 1, 2, 3, 4 }
            : _ownership.OwnedMills.OrderBy(m => m).ToArray();

        if (mills.Length == 0)
            return;

        var ttl = role.LeaseTtlSeconds > 0 ? role.LeaseTtlSeconds : 45;
        var display = role.ResolveDisplayName();

        foreach (var mill in mills)
        {
            var result = await _lease.TryClaimAsync(mill, display, ttl, cancellationToken).ConfigureAwait(false);
            if (!result.Claimed)
            {
                var holder = string.IsNullOrWhiteSpace(result.HolderMachineName)
                    ? "another instance"
                    : $"{result.HolderMachineName}/{result.HolderServiceName ?? "?"} ({result.HolderInstanceId})";

                throw new InvalidOperationException(
                    $"Mill_Instance_Lease claim failed for mill {mill}: already owned by {holder}. "
                    + "A second process must not run mill workers for the same mill. "
                    + "Stop the other instance or wait for its lease to expire.");
            }

            _claimedMills.Add(mill);
            _logger.LogInformation(
                "Claimed Mill_Instance_Lease for mill {Mill} (InstanceId={InstanceId}, TTL={Ttl}s).",
                mill,
                _lease.InstanceId,
                ttl);
        }

        _renewCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _renewTask = RenewLoopAsync(_renewCts.Token);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_renewCts is not null)
        {
            _renewCts.Cancel();
            if (_renewTask is not null)
            {
                try
                {
                    await _renewTask.ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // expected
                }
            }
        }

        foreach (var mill in _claimedMills)
            await _lease.ReleaseAsync(mill, cancellationToken).ConfigureAwait(false);

        _claimedMills.Clear();
    }

    private async Task RenewLoopAsync(CancellationToken cancellationToken)
    {
        var role = _role.CurrentValue;
        var interval = TimeSpan.FromSeconds(
            Math.Max(1, role.LeaseRenewIntervalSeconds > 0 ? role.LeaseRenewIntervalSeconds : 15));
        var ttl = role.LeaseTtlSeconds > 0 ? role.LeaseTtlSeconds : 45;
        var maxTransientAttempts = Math.Max(1, role.LeaseRenewMaxTransientAttempts);
        var transientDelay = TimeSpan.FromSeconds(Math.Max(0, role.LeaseRenewTransientRetryDelaySeconds));

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }

            foreach (var mill in _claimedMills.ToArray())
            {
                if (!await RenewMillWithRetriesAsync(mill, ttl, maxTransientAttempts, transientDelay, cancellationToken)
                        .ConfigureAwait(false))
                {
                    return;
                }
            }
        }
    }

    /// <summary>
    /// Returns false when the host was asked to stop (lost lease or transient retries exhausted).
    /// </summary>
    internal async Task<bool> RenewMillWithRetriesAsync(
        int mill,
        int ttl,
        int maxTransientAttempts,
        TimeSpan transientDelay,
        CancellationToken cancellationToken)
    {
        for (var attempt = 1; attempt <= maxTransientAttempts; attempt++)
        {
            try
            {
                var outcome = await _lease.TryRenewAsync(mill, ttl, cancellationToken).ConfigureAwait(false);
                if (outcome == MillLeaseRenewOutcome.Renewed)
                    return true;

                _logger.LogError(
                    "Lost Mill_Instance_Lease for mill {Mill} (renew rows-affected=0). Stopping host to prevent dual writers.",
                    mill);
                _lifetime.StopApplication();
                return false;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Transient Mill_Instance_Lease renew failure for mill {Mill} (attempt {Attempt}/{Max}).",
                    mill,
                    attempt,
                    maxTransientAttempts);

                if (attempt >= maxTransientAttempts)
                {
                    _logger.LogError(
                        "Exhausted {Max} transient renew attempts for mill {Mill}. Stopping host.",
                        maxTransientAttempts,
                        mill);
                    _lifetime.StopApplication();
                    return false;
                }

                if (transientDelay > TimeSpan.Zero)
                {
                    try
                    {
                        await Task.Delay(transientDelay, cancellationToken).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }
                }
            }
        }

        return true;
    }

    public void Dispose()
    {
        _renewCts?.Cancel();
        _renewCts?.Dispose();
    }
}
