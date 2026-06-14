namespace NdtBundleService.Models;

/// <summary>WIP plan fields merged from PO Accepted CSVs (enrichment, SQL import, and NDT tag lookup).</summary>
public sealed class PoPlanWipRow
{
    public int MillNo { get; init; }
    public string PoNumber { get; init; } = string.Empty;
    public string PlannedMonth { get; init; } = string.Empty;
    public string PipeGrade { get; init; } = string.Empty;
    public string PipeSize { get; init; } = string.Empty;
    public string PipeThickness { get; init; } = string.Empty;
    public string PipeLength { get; init; } = string.Empty;
    public string PipeWeightPerMeter { get; init; } = string.Empty;
    public string PipeType { get; init; } = string.Empty;
    public string OutputItemcode { get; init; } = string.Empty;
    public string ItemDescription { get; init; } = string.Empty;
    public string ProductType { get; init; } = string.Empty;
    public string PoSpecification { get; init; } = string.Empty;
    public string InputWipItemcode { get; init; } = string.Empty;
    public string PiecesPerBundle { get; init; } = string.Empty;
    public string NdtPcsPerBundle { get; init; } = string.Empty;
    public string TotalPieces { get; init; } = string.Empty;
}
