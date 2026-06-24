namespace NdtBundleService.Services.FileBasedPoChange;

/// <summary>Queued PO change detected from a new WIP bundle filename for one mill.</summary>
public sealed class FileBasedPoChangeRequest
{
    public int MillNo { get; init; }

    /// <summary>Previous running PO (from WIP tracking or slit fallback).</summary>
    public string EndedPo { get; init; } = string.Empty;

    /// <summary>PO from the new WIP bundle file.</summary>
    public string NewPo { get; init; } = string.Empty;

    public DateTime WipStampUtc { get; init; }

    public string WipFileName { get; init; } = string.Empty;
}
