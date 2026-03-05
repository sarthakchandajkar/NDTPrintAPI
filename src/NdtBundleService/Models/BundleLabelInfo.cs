namespace NdtBundleService.Models;

/// <summary>
/// Label fields for the NDT bundle tag (Telerik report), loaded from the bundle label CSV file by (PO Number, Mill No).
/// </summary>
public sealed class BundleLabelInfo
{
    public string PoNumber { get; init; } = string.Empty;
    public int MillNo { get; init; }
    public string Specification { get; init; } = string.Empty;
    public string Type { get; init; } = string.Empty;
    public string PipeSize { get; init; } = string.Empty;
    public string Length { get; init; } = string.Empty;
}
