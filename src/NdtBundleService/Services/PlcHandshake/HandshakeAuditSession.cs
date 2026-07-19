namespace NdtBundleService.Services.PlcHandshake;

/// <summary>In-flight handshake audit row updated across edge → ack → clear → ack drop.</summary>
internal sealed class HandshakeAuditSession
{
    public required int MillNo { get; init; }
    public required Guid CorrelationId { get; init; }
    public required DateTimeOffset EdgeAtUtc { get; init; }
    public int PlcPoId { get; init; }
    public int PlcNdtCount { get; init; }
    public DateTimeOffset? AckAtUtc { get; set; }
    public DateTimeOffset? ClearedAtUtc { get; set; }
    public DateTimeOffset? AckDroppedAtUtc { get; set; }
    public HandshakeOutcome Outcome { get; set; } = HandshakeOutcome.InProgress;
    public string? ErrorMessage { get; set; }
    public bool StuckTriggerLogged { get; set; }

    public HandshakeEventRecord ToRecord() =>
        new()
        {
            MillNo = MillNo,
            EdgeAtUtc = EdgeAtUtc,
            AckAtUtc = AckAtUtc,
            ClearedAtUtc = ClearedAtUtc,
            AckDroppedAtUtc = AckDroppedAtUtc,
            PlcPoId = PlcPoId,
            PlcNdtCount = PlcNdtCount,
            CorrelationId = CorrelationId,
            Outcome = Outcome,
            ErrorMessage = ErrorMessage
        };
}
