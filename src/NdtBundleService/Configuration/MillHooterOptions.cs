namespace NdtBundleService.Configuration;

/// <summary>
/// Mill-1 NDT bundle hooter: when PAS enable is on and MW56 (accumulated) exceeds MW58 (threshold),
/// pulse Q6.7 for <see cref="DurationMs"/> (matches PLC networks 90–92).
/// </summary>
public sealed class MillHooterOptions
{
    public bool Enabled { get; set; }

    /// <summary>DB260.DBX3.6 — L2L1_PASEn.</summary>
    public int PasEnableDbNumber { get; set; } = 260;

    public int PasEnableByteOffset { get; set; } = 3;

    public int PasEnableBit { get; set; } = 6;

    /// <summary>MW56 — accumulated NDT pipes toward the next bundle (written from bundle engine state).</summary>
    public int AccumulatedWordOffset { get; set; } = 56;

    /// <summary>MW58 — size/bed-capacity threshold (written from formation chart for running PO).</summary>
    public int ThresholdWordOffset { get; set; } = 58;

    /// <summary>Q6.7 — hooter / sizing jog output.</summary>
    public int OutputByte { get; set; } = 6;

    public int OutputBit { get; set; } = 7;

    public int DurationMs { get; set; } = 10_000;
}
