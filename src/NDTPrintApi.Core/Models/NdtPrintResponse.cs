namespace NDTPrintApi.Core.Models;

/// <summary>
/// Response model for the NDT pipe bundle tag print endpoint.
/// </summary>
public class NdtPrintResponse
{
    /// <summary>Whether the print operation succeeded.</summary>
    public bool Success { get; set; }

    /// <summary>Human-readable message (e.g. success or error description).</summary>
    public string Message { get; set; } = string.Empty;

    /// <summary>Echo of the bundle number (when successful).</summary>
    public string? BundleNo { get; set; }

    /// <summary>Echo of the mill number (when successful).</summary>
    public int? MillNo { get; set; }

    /// <summary>Time the tag was printed (UTC).</summary>
    public DateTime? PrintTime { get; set; }

    /// <summary>Whether the request was a reprint.</summary>
    public bool? Reprint { get; set; }
}
