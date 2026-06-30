namespace NdtBundleService.Configuration;

/// <summary>
/// File-based PO end options. PO-end source per mill is configured via
/// <see cref="MillConfig.PoEndSource"/> (<c>File</c> | <c>Plc</c> | <c>TcpOpen</c>).
/// </summary>
public sealed class FileBasedPoEndOptions
{
    /// <summary>
    /// Deprecated: use per-mill <see cref="MillConfig.PoEndSource"/> = <c>File</c> instead.
    /// </summary>
    [Obsolete("Use per-mill MillConfig.PoEndSource instead.")]
    public bool Enabled { get; set; }

    /// <summary>When true, advance the PO plan file after file-based PO end (mills with PoEndSource=File).</summary>
    public bool AdvancePoPlanFileOnPoEnd { get; set; }

    /// <summary>When true, periodically scan WIP bundle folders for missed file-based PO-end events.</summary>
    public bool ReconciliationEnabled { get; set; } = true;

    /// <summary>Interval in minutes between WIP bundle reconciliation scans (default 5).</summary>
    public int ReconciliationIntervalMinutes { get; set; } = 5;
}
