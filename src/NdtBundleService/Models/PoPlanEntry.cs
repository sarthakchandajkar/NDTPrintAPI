namespace NdtBundleService.Models;

/// <summary>
/// PO plan information from SAP PO Accepted / WIP files (per mill row).
/// </summary>
public sealed class PoPlanEntry
{
    public string PoNumber { get; init; } = string.Empty;
    public int MillNo { get; init; }
    /// <summary>SAP planned production month (e.g. <c>5</c> = May, <c>6</c> = June).</summary>
    public string PlannedMonth { get; init; } = string.Empty;
    public int NdtPcsPerBundle { get; init; }
    public string SourceFile { get; init; } = string.Empty;
}

