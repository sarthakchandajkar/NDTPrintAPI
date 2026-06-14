namespace NdtBundleService.Configuration;

/// <summary>
/// Controls pruning of idle PO/mill entries from <c>NdtBundleRuntimeState.json</c>.
/// </summary>
public sealed class RuntimeStatePruningOptions
{
    /// <summary>When false, no slots are removed from runtime state.</summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Run pruning once during service initialization (after state load and mill-floor sync).</summary>
    public bool RunOnStartup { get; set; } = true;

    /// <summary>
    /// Keep completed idle slots for this many days after last activity to tolerate late input slits after restart.
    /// </summary>
    public int GracePeriodDays { get; set; } = 14;
}
