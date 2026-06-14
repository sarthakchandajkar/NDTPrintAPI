namespace NdtBundleService.Configuration;

/// <summary>
/// Persistent per-mill S7 PO-change handshake (M-bit trigger/ack). Bound from NdtBundle:PlcHandshake.
/// When <see cref="Enabled"/> is true, legacy <see cref="PlcPoEndOptions"/> polling in
/// <see cref="Services.SlitMonitoringWorker"/> is disabled to avoid duplicate S7 clients.
/// </summary>
public sealed class PlcHandshakeOptions
{
    /// <summary>When true, <see cref="Services.PlcHandshake.PlcHandshakeWorker"/> owns PO-end I/O.</summary>
    public bool Enabled { get; set; }

    /// <summary>Default poll interval for all mills (ms).</summary>
    public int PollIntervalMs { get; set; } = 500;

    /// <summary>When true, PO change also advances the PO plan folder.</summary>
    public bool AdvancePoPlanFileOnPoEnd { get; set; }

    /// <summary>Initial reconnect delay (ms); doubles up to <see cref="MaxReconnectDelayMs"/>.</summary>
    public int InitialReconnectDelayMs { get; set; } = 1000;

    /// <summary>Cap for exponential reconnect backoff (ms).</summary>
    public int MaxReconnectDelayMs { get; set; } = 30_000;

    /// <summary>S7 DB for live OK/NOK/NDT counts (same as plc-server: DB251).</summary>
    public int CountsDbNumber { get; set; } = 251;

    /// <summary>Byte offset of OK count INT in <see cref="CountsDbNumber"/> (DBW2).</summary>
    public int OkCountByteOffset { get; set; } = 2;

    /// <summary>Byte offset of NOK count INT (DBW4).</summary>
    public int NokCountByteOffset { get; set; } = 4;

    /// <summary>Byte offset of NDT count INT (DBW6).</summary>
    public int NdtCountByteOffset { get; set; } = 6;

    /// <summary>Byte offset of PO ID INT (DBW8).</summary>
    public int PoIdByteOffset { get; set; } = 8;

    /// <summary>Byte offset of Slit ID INT (DBW10).</summary>
    public int SlitIdByteOffset { get; set; } = 10;

    /// <summary>
    /// Consecutive poll cycles with trigger FALSE required after a completed handshake before the next
    /// rising edge is accepted (filters immediate PLC re-pulses on the same PO change).
    /// </summary>
    public int MinimumTriggerFalsePollsBeforeRearm { get; set; } = 2;

    public List<MillConfig> Mills { get; set; } = new();
}
