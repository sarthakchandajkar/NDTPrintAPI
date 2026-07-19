namespace NdtBundleService.Models;

/// <summary>
/// Represents a recorded NDT bundle (one tag print). Used for listing and reconciliation.
/// </summary>
public sealed class NdtBundleRecord
{
    public string BundleNo { get; init; } = string.Empty;
    public string PoNumber { get; init; } = string.Empty;
    public int MillNo { get; init; }
    public int TotalNdtPcs { get; init; }
    public string SlitNo { get; init; } = string.Empty;
    public DateTime? SlitStartTime { get; init; }
    public DateTime? SlitFinishTime { get; init; }
    public DateTime? PrintedAt { get; init; }
    public string PrintStatus { get; init; } = BundlePrintStatus.Pending;
    public DateTime? PrintAttemptedAt { get; init; }
    public string? PrintError { get; init; }
    public int RejectedPipes { get; init; }
    public string NdtShortLengthPipe { get; init; } = string.Empty;
    public string RejectedShortLengthPipe { get; init; } = string.Empty;

    /// <summary><c>File</c> or <c>Plc</c>; null when column absent / legacy rows.</summary>
    public string? CloseSource { get; init; }

    public bool AwaitingCsvRecon { get; init; }

    public bool CountDiscrepancy { get; init; }
}
