namespace NdtBundleService.Configuration;

/// <summary>
/// Read-only Mill-1 PO-change ladder observation (DB50 / MD232 / ReconOn / ManualButtEnd / M40.6/7 / MW314 / DB251).
/// No PLC writes. Bound from <c>NdtBundle:PlcHandshake:PoChangeLadderDiagnostics</c>.
/// </summary>
public sealed class PoChangeLadderDiagnosticsOptions
{
    /// <summary>When true, the handshake poll logs ladder-signal snapshots on change.</summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Matches PLC <c>CMP &gt; R</c> on MD232 (default 30000). Crossing this threshold either way
    /// (or MD232 resetting to 0) is treated as a log-worthy MD232 event; other MD232 chatter is ignored.
    /// </summary>
    public float Md232Threshold { get; set; } = 30_000f;

    /// <summary>DB for Slit1/Slit2 PO_ID (default 50).</summary>
    public int SlitPoDbNumber { get; set; } = 50;

    /// <summary>Byte offset of Slit1.PO_ID INT (DB50.DBW0).</summary>
    public int Slit1PoIdByteOffset { get; set; }

    /// <summary>Byte offset of Slit2.PO_ID INT (DB50.DBW8).</summary>
    public int Slit2PoIdByteOffset { get; set; } = 8;

    /// <summary>Merker byte offset of MD232 REAL (MB232–MB235).</summary>
    public int Md232ByteOffset { get; set; } = 232;

    /// <summary>DB for L2L1_ReconOn (default 260).</summary>
    public int ReconOnDbNumber { get; set; } = 260;

    /// <summary>Byte offset of L2L1_ReconOn (DB260.DBX3.7).</summary>
    public int ReconOnByteOffset { get; set; } = 3;

    /// <summary>Bit of L2L1_ReconOn.</summary>
    public int ReconOnBit { get; set; } = 7;

    /// <summary>Merker byte of SCADA/ManualButtEnd (M74.4).</summary>
    public int ManualButtEndByte { get; set; } = 74;

    /// <summary>Bit of SCADA/ManualButtEnd.</summary>
    public int ManualButtEndBit { get; set; } = 4;

    /// <summary>Merker byte offset of MW314.</summary>
    public int Mw314ByteOffset { get; set; } = 314;
}
