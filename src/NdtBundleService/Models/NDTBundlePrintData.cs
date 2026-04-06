namespace NdtBundleService.Models;

/// <summary>
/// Print data for an NDT bundle tag. Matches the shape of NDTBundlePrintData from the NDT_Bundle_Printing_POC reference.
/// Maps to report fields: Specification, Type, Size, Length, Pcs/Bnd, Slit No, Bundle No, reprint indicator.
/// </summary>
public sealed class NDTBundlePrintData
{
    /// <summary>Bundle number (displayed and encoded in the label QR code).</summary>
    public string BundleNo { get; init; } = string.Empty;

    /// <summary>Batch number (optional identifier).</summary>
    public string? BatchNo { get; init; }

    /// <summary>NDT pieces per bundle (Pcs/Bnd on label).</summary>
    public int NDT_Pcs { get; init; }

    /// <summary>PO number.</summary>
    public string? PO_No { get; init; }

    /// <summary>Pipe grade / PO specification (Specification on label).</summary>
    public string? Pipe_Grade { get; init; }

    /// <summary>Pipe type (Type on label).</summary>
    public string? Pipe_Type { get; init; }

    /// <summary>Pipe size (Size on label; suffix ″ is added when printing).</summary>
    public string? Pipe_Size { get; init; }

    /// <summary>Pipe length (Length on label; suffix ′ is added when printing).</summary>
    public string? Pipe_Len { get; init; }

    /// <summary>Slit number (Slit No on label).</summary>
    public string? SlitNo { get; init; }

    /// <summary>Bundle start time (optional; for logging/audit).</summary>
    public DateTime? BundleStartTime { get; init; }

    /// <summary>Bundle end time (optional; for logging/audit).</summary>
    public DateTime? BundleEndTime { get; init; }

    /// <summary>True when this is a reprint (shows "R" on label).</summary>
    public bool IsReprint { get; init; }

    /// <summary>Mill line / mill number (optional; for multi-line).</summary>
    public int MillLine { get; init; }

    /// <summary>Mill PO id or equivalent (optional; for DB/report parameters).</summary>
    public int? MillPOId { get; init; }

    /// <summary>NDT bundle ID (optional; for DB).</summary>
    public int? NDTBundleID { get; init; }
}
