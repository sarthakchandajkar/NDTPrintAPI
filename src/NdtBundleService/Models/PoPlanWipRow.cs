namespace NdtBundleService.Models;

/// <summary>WIP plan fields merged from PO Accepted CSVs (enrichment only; current PO comes from slits).</summary>
public sealed class PoPlanWipRow
{
    public int MillNo { get; init; }
    public string PoNumber { get; init; } = string.Empty;
    public string PlannedMonth { get; init; } = string.Empty;
    public string PipeGrade { get; init; } = string.Empty;
    public string PipeSize { get; init; } = string.Empty;
    public string PipeType { get; init; } = string.Empty;
    public string PipeLength { get; init; } = string.Empty;
    public string PiecesPerBundle { get; init; } = string.Empty;
    public string TotalPieces { get; init; } = string.Empty;
}
