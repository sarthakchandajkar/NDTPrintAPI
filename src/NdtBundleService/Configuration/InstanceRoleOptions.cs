namespace NdtBundleService.Configuration;

/// <summary>Deployment role for a single NdtBundleService process (monolith, shared dashboard, or one mill).</summary>
public sealed class InstanceRoleOptions
{
    public const string SectionName = "InstanceRole";

    /// <summary>
    /// When the <see cref="SectionName"/> config section is absent, defaults to <see cref="InstanceRoleModes.Monolith"/>
    /// for zero behavior change vs pre-split deployments.
    /// </summary>
    public string Mode { get; set; } = InstanceRoleModes.Monolith;

    /// <summary>Mill numbers owned by this instance when <see cref="Mode"/> is <see cref="InstanceRoleModes.Mill"/>.</summary>
    public int[] OwnedMillNos { get; set; } = Array.Empty<int>();

    /// <summary>Human-readable label for logs and Windows Service display (e.g. Shared, Mill-1).</summary>
    public string InstanceDisplayName { get; set; } = string.Empty;

    public bool EnableDashboardApi { get; set; } = true;

    public bool EnableMillWorkers { get; set; } = true;

    public bool EnablePoPlanWipImport { get; set; } = true;

    /// <summary>Lease TTL seconds for <c>Mill_Instance_Lease</c> (Mill / Monolith). Default 45.</summary>
    public int LeaseTtlSeconds { get; set; } = 45;

    /// <summary>How often to renew the mill lease (seconds). Default 15.</summary>
    public int LeaseRenewIntervalSeconds { get; set; } = 15;

    /// <summary>
    /// On transient SQL errors during renew, attempt this many times (including the first) before stopping the host.
    /// Lost-lease (rows-affected 0) never retries — host stops immediately. Default 3.
    /// </summary>
    public int LeaseRenewMaxTransientAttempts { get; set; } = 3;

    /// <summary>Delay between transient renew retries (seconds). Default 2.</summary>
    public int LeaseRenewTransientRetryDelaySeconds { get; set; } = 2;

    public bool IsMonolith => string.Equals(Mode, InstanceRoleModes.Monolith, StringComparison.OrdinalIgnoreCase);

    public bool IsShared => string.Equals(Mode, InstanceRoleModes.Shared, StringComparison.OrdinalIgnoreCase);

    public bool IsMill => string.Equals(Mode, InstanceRoleModes.Mill, StringComparison.OrdinalIgnoreCase);

    /// <summary>Resolved display name for Serilog enrichment.</summary>
    public string ResolveDisplayName()
    {
        if (!string.IsNullOrWhiteSpace(InstanceDisplayName))
            return InstanceDisplayName.Trim();

        if (IsShared)
            return "Shared";

        if (IsMill && OwnedMillNos.Length == 1)
            return $"Mill-{OwnedMillNos[0]}";

        return Mode;
    }
}

public static class InstanceRoleModes
{
    public const string Monolith = "Monolith";
    public const string Shared = "Shared";
    public const string Mill = "Mill";
}
