namespace NdtBundleService.Models;

/// <summary>
/// Represents one row from an Input Slit CSV file.
/// </summary>
public sealed class InputSlitRecord
{
    public string PoNumber { get; init; } = string.Empty;
    public string SlitNo { get; init; } = string.Empty;
    public int NdtPipes { get; init; }
    /// <summary>Rejected pipe count from TM Input Slit. Always a non-negative integer when <see cref="HasRejectedPipes"/> is true.</summary>
    public int RejectedPipes { get; init; }
    /// <summary>
    /// False when the Rejected P / Rejected Pipes cell was blank, missing, or unparseable.
    /// Incomplete source files must not be stamped — TM regenerates them with a real value (including 0).
    /// </summary>
    public bool HasRejectedPipes { get; init; } = true;
    public DateTime? SlitStartTime { get; init; }
    public DateTime? SlitFinishTime { get; init; }
    public int MillNo { get; init; }
    public string NdtShortLengthPipe { get; init; } = string.Empty;
    public string RejectedShortLengthPipe { get; init; } = string.Empty;
}

