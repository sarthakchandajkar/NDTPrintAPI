namespace NdtBundleService.Models;

/// <summary>
/// NDT bundle formation rule per pipe size (or category).
/// Assumed columns per document: pipe size identifier and required NDT pieces per bundle for that size.
/// </summary>
public sealed class FormationChartEntry
{
    public string PipeSize { get; init; } = string.Empty;
    public int RequiredNdtPcs { get; init; }
}

