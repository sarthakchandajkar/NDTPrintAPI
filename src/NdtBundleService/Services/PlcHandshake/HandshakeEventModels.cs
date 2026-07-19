namespace NdtBundleService.Services.PlcHandshake;

public enum HandshakeOutcome
{
    InProgress,
    Completed,
    AckWriteFailed,
    StuckTrigger,
    TriggerTimeoutForceAckDrop,
    StartupRecoveryCompleted
}

public sealed class HandshakeEventRecord
{
    public int MillNo { get; init; }
    public DateTimeOffset EdgeAtUtc { get; init; }
    public DateTimeOffset? AckAtUtc { get; init; }
    public DateTimeOffset? ClearedAtUtc { get; init; }
    public DateTimeOffset? AckDroppedAtUtc { get; init; }
    public int PlcPoId { get; init; }
    public int PlcNdtCount { get; init; }
    public Guid CorrelationId { get; init; }
    public HandshakeOutcome Outcome { get; init; }
    public string? ErrorMessage { get; init; }
}

public interface IHandshakeEventRepository
{
    /// <summary>Insert or update by <see cref="HandshakeEventRecord.CorrelationId"/>. No-op when SQL disabled.</summary>
    Task UpsertAsync(HandshakeEventRecord record, CancellationToken cancellationToken = default);
}
