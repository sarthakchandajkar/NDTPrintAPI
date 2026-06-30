namespace NdtBundleService.Services.PlcHandshake.PlcPoEnd;

/// <summary>PO-end event enqueued after PLC rising-edge detection and immediate MES ack.</summary>
public sealed class PlcPoEndRequest
{
    public int MillNo { get; init; }
    public int PoId { get; init; }
    public int NdtCountFinal { get; init; }
    public Guid CorrelationId { get; init; }
    public DateTimeOffset DetectedAtUtc { get; init; }
    public bool StartupRecovery { get; init; }
}
