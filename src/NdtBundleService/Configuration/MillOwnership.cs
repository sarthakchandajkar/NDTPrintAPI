using Microsoft.Extensions.Options;

namespace NdtBundleService.Configuration;

/// <summary>Which mills this process is allowed to operate (defense in depth beyond config trim).</summary>
public interface IMillOwnership
{
    /// <summary>Owned mills for Mill mode; 1–4 for Monolith; empty for Shared.</summary>
    IReadOnlySet<int> OwnedMills { get; }

    bool Owns(int millNo);

    /// <summary>Single owned mill when Mode=Mill; otherwise null.</summary>
    int? SingleOwnedMill { get; }

    /// <summary>True when this process runs mill workers (Monolith or Mill with EnableMillWorkers).</summary>
    bool RunsMillWorkers { get; }
}

public sealed class MillOwnership : IMillOwnership
{
    private readonly HashSet<int> _owned;

    public MillOwnership(IOptions<InstanceRoleOptions> roleOptions)
    {
        var role = roleOptions.Value;
        if (role.IsShared)
        {
            _owned = new HashSet<int>();
            RunsMillWorkers = false;
        }
        else if (role.IsMill)
        {
            _owned = new HashSet<int>(role.OwnedMillNos ?? Array.Empty<int>());
            RunsMillWorkers = role.EnableMillWorkers;
        }
        else
        {
            _owned = new HashSet<int> { 1, 2, 3, 4 };
            RunsMillWorkers = true;
        }
    }

    public IReadOnlySet<int> OwnedMills => _owned;

    public bool Owns(int millNo) => _owned.Contains(millNo);

    public int? SingleOwnedMill => _owned.Count == 1 ? _owned.First() : null;

    public bool RunsMillWorkers { get; }
}

public static class MillOwnershipExtensions
{
    /// <summary>
    /// Shared (empty owned set) may read/write every mill; mill-n only its own.
    /// Use this — not <see cref="IMillOwnership.Owns"/> — for SQL mill-state tables.
    /// </summary>
    public static bool Allows(this IMillOwnership ownership, int millNo) =>
        ownership.OwnedMills.Count == 0 || ownership.Owns(millNo);
}
