namespace NdtBundleService.Models;

/// <summary>
/// Represents one row from an Input Slit CSV file.
/// </summary>
public sealed class InputSlitRecord
{
    public string PoNumber { get; init; } = string.Empty;
    public string SlitNo { get; init; } = string.Empty;
    public int NdtPipes { get; init; }
    public int RejectedPipes { get; init; }
    public DateTime? SlitStartTime { get; init; }
    public DateTime? SlitFinishTime { get; init; }
    public int MillNo { get; init; }
    public string NdtShortLengthPipe { get; init; } = string.Empty;
    public string RejectedShortLengthPipe { get; init; } = string.Empty;
}

