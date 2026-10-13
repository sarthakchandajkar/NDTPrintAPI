namespace NdtBundleService.Services.LinkedPo;

/// <summary>Queued linked-PO cutover (no PLC PO-end trigger).</summary>
public sealed class LinkedPoCutoverRequest
{
    public int MillNo { get; init; }
    public string EndedPo { get; init; } = string.Empty;
    public string NewPo { get; init; } = string.Empty;
    public DateTime WipStampUtc { get; init; }
    public string WipFileName { get; init; } = string.Empty;
    /// <summary>Optional slit id from the second-half Input Slit (may include <c>_91</c>).</summary>
    public string? SlitNoHint { get; init; }
    /// <summary>e.g. <c>WipPoChange</c> or <c>InputSlit</c>.</summary>
    public string Trigger { get; init; } = "WipPoChange";
    public Guid CorrelationId { get; init; } = Guid.NewGuid();
}
