namespace NdtBundleService.Models;

/// <summary>One slit identity row mirrored from a Slitting Slit Accepted CSV.</summary>
public sealed class SlitAcceptedRow
{
    public string SlitNo { get; init; } = string.Empty;
    public string SlitWidth { get; init; } = string.Empty;
    public string HrcNumber { get; init; } = string.Empty;
    public string SlitThick { get; init; } = string.Empty;
    public string PoNumber { get; init; } = string.Empty;
    public int? MillNo { get; init; }
    public string Nss { get; init; } = string.Empty;
    public int SourceRowNumber { get; init; }
}

/// <summary>One FG bundle data row mirrored from <c>FG_*.csv</c> (Bundle / Bundle Accepted).</summary>
public sealed class FgBundleRow
{
    public string PoNumber { get; init; } = string.Empty;
    public int? MillNo { get; init; }
    public string PipeGrade { get; init; } = string.Empty;
    public string PipeSize { get; init; } = string.Empty;
    public string PipeThickness { get; init; } = string.Empty;
    public string PipeLength { get; init; } = string.Empty;
    public string PipeWeightPerMeter { get; init; } = string.Empty;
    public string PipeType { get; init; } = string.Empty;
    public string ActPcsInBundle { get; init; } = string.Empty;
    public string BundleNo { get; init; } = string.Empty;
    public string BundleStatus { get; init; } = string.Empty;
    public string Slit1Num { get; init; } = string.Empty;
    public string Slit1OkPcs { get; init; } = string.Empty;
    public string Slit2Num { get; init; } = string.Empty;
    public string Slit2OkPcs { get; init; } = string.Empty;
    public string Slit3Num { get; init; } = string.Empty;
    public string Slit3OkPcs { get; init; } = string.Empty;
    public string Slit4Num { get; init; } = string.Empty;
    public string Slit4OkPcs { get; init; } = string.Empty;
    public string BundleWt { get; init; } = string.Empty;
    public string BundleStart { get; init; } = string.Empty;
    public string BundleEnd { get; init; } = string.Empty;
    public string OperatorDone { get; init; } = string.Empty;
    public string NonStandardSlit { get; init; } = string.Empty;
    public string PoSpecification { get; init; } = string.Empty;
}
