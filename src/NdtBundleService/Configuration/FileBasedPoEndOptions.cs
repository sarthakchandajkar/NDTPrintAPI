namespace NdtBundleService.Configuration;

/// <summary>
/// PO end / PO change detection from TM Bundle WIP filenames instead of the PLC M-bit trigger.
/// When <see cref="Enabled"/> is true, PO end is driven from WIP filenames instead of the PLC M-bit.
/// Pair with <see cref="PlcHandshakeOptions.TelemetryOnly"/> so the S7 client only reads counts and line running.
/// </summary>
public sealed class FileBasedPoEndOptions
{
    /// <summary>When true, PO change is detected from new <c>WIP_*</c> files in the TM Bundle folder.</summary>
    public bool Enabled { get; set; }

    /// <summary>When true, advance the PO plan file after file-based PO end (same as legacy PLC path).</summary>
    public bool AdvancePoPlanFileOnPoEnd { get; set; }
}
