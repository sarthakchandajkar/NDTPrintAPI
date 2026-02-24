namespace NDTPrintApi.Core.Models;

/// <summary>
/// Request model for the NDT pipe bundle tag print endpoint.
/// </summary>
public class NdtPrintRequest
{
    /// <summary>Bundle number (required).</summary>
    public string BundleNo { get; set; } = string.Empty;

    /// <summary>Mill number (required).</summary>
    public int MillNo { get; set; }

    /// <summary>Whether this is a reprint.</summary>
    public bool Reprint { get; set; }

    /// <summary>Optional NDT bundle identifier.</summary>
    public int? NdtBundleId { get; set; }

    /// <summary>Optional PO plan identifier.</summary>
    public int? PoPlanId { get; set; }
}
