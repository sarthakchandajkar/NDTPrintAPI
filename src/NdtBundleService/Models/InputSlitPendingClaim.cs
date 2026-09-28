namespace NdtBundleService.Models;

/// <summary>Durable Input Slit claim awaiting fill-target stamp (or already completed).</summary>
public sealed class InputSlitPendingClaim
{
    public long PendingId { get; init; }
    public string SourceFileName { get; init; } = string.Empty;
    public string? SourceFileClaimed { get; init; }
    public DateTime SourceLastWriteTimeUtc { get; init; }
    public int MillNo { get; init; }
    public string Status { get; init; } = InputSlitPendingStatus.AwaitingTarget;
    public string? NdtBatchNo { get; init; }
    public string? OutputFile { get; init; }
    public IReadOnlyList<InputSlitPendingRow> Rows { get; init; } = Array.Empty<InputSlitPendingRow>();
}

/// <summary>One parsed slit line stored on an <see cref="InputSlitPendingClaim"/>.</summary>
public sealed class InputSlitPendingRow
{
    public int SourceRowNumber { get; init; }
    public InputSlitRecord Record { get; init; } = new();
    public string? PipeSize { get; init; }
}

/// <summary>Status values for <c>dbo.Input_Slit_Pending.Status</c>.</summary>
public static class InputSlitPendingStatus
{
    public const string AwaitingTarget = "AwaitingTarget";
    public const string Completed = "Completed";
}
