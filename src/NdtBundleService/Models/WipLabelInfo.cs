namespace NdtBundleService.Models;

/// <summary>
/// WIP/label fields from the PO plan (WIP) CSV for printing on the NDT tag.
/// </summary>
public sealed class WipLabelInfo
{
    public string PipeGrade { get; init; } = string.Empty;
    public string PipeSize { get; init; } = string.Empty;
    public string PipeThickness { get; init; } = string.Empty;
    public string PipeLength { get; init; } = string.Empty;
    public string PipeWeightPerMeter { get; init; } = string.Empty;
    public string PipeType { get; init; } = string.Empty;
}
