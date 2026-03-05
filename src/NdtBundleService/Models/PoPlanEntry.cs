namespace NdtBundleService.Models;

/// <summary>
/// PO plan information coming from SAP, including how many NDT pieces per bundle are required.
/// </summary>
public sealed class PoPlanEntry
{
    public string PoNumber { get; init; } = string.Empty;
    public int NdtPcsPerBundle { get; init; }
}

