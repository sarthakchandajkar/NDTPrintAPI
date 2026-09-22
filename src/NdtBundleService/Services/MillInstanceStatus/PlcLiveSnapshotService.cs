using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.PlcHandshake;

namespace NdtBundleService.Services.MillInstanceStatus;

public sealed record PlcLiveSnapshot(
    bool PlcHandshakeEnabled,
    string Source,
    string? Message,
    IReadOnlyList<PlcHandshakeMillStatus> Mills);

public interface IPlcLiveSnapshotService
{
    PlcLiveSnapshot GetSnapshot();
}

/// <summary>
/// Local RAM snapshot on mill/monolith (no delay vs S7 loop). SQL snapshot on Shared for the dashboard.
/// </summary>
public sealed class PlcLiveSnapshotService : IPlcLiveSnapshotService
{
    public const string SourceLocal = "local";
    public const string SourceSql = "sql";
    public const string SourceNone = "none";

    private readonly PlcHandshakeStatusRegistry _registry;
    private readonly IMillInstanceStatusStore _store;
    private readonly IOptionsMonitor<NdtBundleOptions> _bundle;
    private readonly IOptionsMonitor<InstanceRoleOptions> _role;
    private readonly Func<DateTimeOffset> _utcNow;

    public PlcLiveSnapshotService(
        PlcHandshakeStatusRegistry registry,
        IMillInstanceStatusStore store,
        IOptionsMonitor<NdtBundleOptions> bundle,
        IOptionsMonitor<InstanceRoleOptions> role)
        : this(registry, store, bundle, role, () => DateTimeOffset.UtcNow)
    {
    }

    internal PlcLiveSnapshotService(
        PlcHandshakeStatusRegistry registry,
        IMillInstanceStatusStore store,
        IOptionsMonitor<NdtBundleOptions> bundle,
        IOptionsMonitor<InstanceRoleOptions> role,
        Func<DateTimeOffset> utcNow)
    {
        _registry = registry;
        _store = store;
        _bundle = bundle;
        _role = role;
        _utcNow = utcNow;
    }

    public PlcLiveSnapshot GetSnapshot()
    {
        var handshake = _bundle.CurrentValue.PlcHandshake ?? new PlcHandshakeOptions();
        if (handshake.Enabled)
        {
            var local = _registry.GetSnapshot();
            if (local.Count > 0)
            {
                return new PlcLiveSnapshot(
                    true,
                    SourceLocal,
                    null,
                    local);
            }
        }

        var role = _role.CurrentValue;
        if (role.IsShared || role.IsMill)
        {
            var sql = MillInstanceStatusFreshness.PadAndApplyStale(_store.LoadAll(), _utcNow());
            return new PlcLiveSnapshot(
                true,
                SourceSql,
                "Live counts from mill instances via Mill_Instance_Status (S7 stays on each mill process).",
                sql);
        }

        return new PlcLiveSnapshot(
            false,
            SourceNone,
            "PlcHandshake disabled; use plc-server Socket.IO for live counts.",
            Array.Empty<PlcHandshakeMillStatus>());
    }
}
