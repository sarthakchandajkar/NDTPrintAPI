using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.InstanceLease;
using NdtBundleService.Services.PlcHandshake;

namespace NdtBundleService.Services.MillInstanceStatus;

/// <summary>
/// Copies in-memory handshake status to <c>dbo.Mill_Instance_Status</c> on a background timer.
/// Must not run on the S7 / PO-end / tag-print path — handshake continues if SQL publish fails.
/// Also publishes mill WIP running PO so Shared ActivePo / wip-by-mills can prefer it over slit CSV.
/// </summary>
public sealed class MillInstanceStatusPublisher : BackgroundService
{
    public static readonly TimeSpan DefaultInterval = TimeSpan.FromMilliseconds(500);

    private readonly IMillInstanceStatusStore _store;
    private readonly PlcHandshakeStatusRegistry _registry;
    private readonly IMillOwnership _ownership;
    private readonly IMillInstanceLeaseService _lease;
    private readonly IWipBundleRunningPoProvider _wipRunningPo;
    private readonly IOptionsMonitor<InstanceRoleOptions> _role;
    private readonly IOptionsMonitor<NdtBundleOptions> _bundle;
    private readonly ILogger<MillInstanceStatusPublisher> _logger;

    public MillInstanceStatusPublisher(
        IMillInstanceStatusStore store,
        PlcHandshakeStatusRegistry registry,
        IMillOwnership ownership,
        IMillInstanceLeaseService lease,
        IWipBundleRunningPoProvider wipRunningPo,
        IOptionsMonitor<InstanceRoleOptions> role,
        IOptionsMonitor<NdtBundleOptions> bundle,
        ILogger<MillInstanceStatusPublisher> logger)
    {
        _store = store;
        _registry = registry;
        _ownership = ownership;
        _lease = lease;
        _wipRunningPo = wipRunningPo;
        _role = role;
        _bundle = bundle;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!SqlTraceabilityConnection.IsSqlEnabled(_bundle.CurrentValue))
        {
            _logger.LogInformation("Mill_Instance_Status publisher idle — SQL traceability is disabled.");
            return;
        }

        _logger.LogInformation(
            "Mill_Instance_Status publisher started (interval {Interval}ms). Dashboard only — not on PO-end/print path.",
            (int)ResolveInterval().TotalMilliseconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            PublishOwned(connectedOverride: null);
            try
            {
                await Task.Delay(ResolveInterval(), stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        PublishOwned(connectedOverride: false);
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }

    internal void PublishOwned(bool? connectedOverride)
    {
        var mills = _role.CurrentValue.IsMonolith
            ? new[] { 1, 2, 3, 4 }
            : _ownership.OwnedMills.OrderBy(m => m).ToArray();
        if (mills.Length == 0)
            return;

        var machine = Environment.MachineName ?? string.Empty;
        var service = _role.CurrentValue.ResolveDisplayName();
        var instanceId = _lease.InstanceId;

        foreach (var millNo in mills)
        {
            try
            {
                PlcHandshakeMillStatus row;
                if (_registry.TryGetMill(millNo, out var live) && live is not null)
                {
                    row = MillInstanceStatusMapper.Clone(live);
                }
                else
                {
                    row = MillInstanceStatusMapper.Heartbeat(
                        millNo,
                        $"Mill-{millNo}",
                        "no handshake loop on this instance");
                }

                if (connectedOverride == false)
                    row.Connected = false;

                ApplyRunningPo(row, millNo);
                _store.Upsert(row, instanceId, machine, service);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Mill_Instance_Status publish failed for mill {Mill} (dashboard only; mill workers unaffected).",
                    millNo);
            }
        }
    }

    private void ApplyRunningPo(PlcHandshakeMillStatus row, int millNo)
    {
        var now = DateTimeOffset.UtcNow;
        if (_wipRunningPo.IsWaitingForNewWipAfterPoEnd(millNo))
        {
            row.WaitingForNewWip = true;
            row.RunningPoNumber = null;
            row.RunningPoSource = "Waiting";
            row.RunningPoUpdatedAtUtc = now;
            return;
        }

        // Sync read — TryGetRunningPoForMillAsync is sync for in-memory WIP on mill.
        var wipPo = _wipRunningPo.TryGetRunningPoForMillAsync(millNo, CancellationToken.None)
            .GetAwaiter()
            .GetResult();
        if (!string.IsNullOrWhiteSpace(wipPo))
        {
            row.WaitingForNewWip = false;
            row.RunningPoNumber = InputSlitCsvParsing.NormalizePo(wipPo);
            row.RunningPoSource = "Wip";
            row.RunningPoUpdatedAtUtc = now;
            return;
        }

        row.WaitingForNewWip = false;
        row.RunningPoNumber = null;
        row.RunningPoSource = null;
        row.RunningPoUpdatedAtUtc = now;
    }

    private TimeSpan ResolveInterval()
    {
        var ms = _bundle.CurrentValue.PlcHandshake?.PollIntervalMs ?? 500;
        if (ms < 250)
            ms = 250;
        if (ms > 2000)
            ms = 2000;
        return TimeSpan.FromMilliseconds(ms);
    }
}
