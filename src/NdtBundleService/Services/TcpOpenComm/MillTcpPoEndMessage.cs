namespace NdtBundleService.Services.TcpOpenComm;

/// <summary>Decoded PO-end signal from TCP open communication (wire layout in <see cref="Mill4MessageCodec"/>).</summary>
public sealed record MillTcpPoEndMessage(
    int MillNo,
    ushort PoTypeId,
    bool TriggerActive,
    byte[] RawPayload,
    DateTimeOffset ReceivedAtUtc);
