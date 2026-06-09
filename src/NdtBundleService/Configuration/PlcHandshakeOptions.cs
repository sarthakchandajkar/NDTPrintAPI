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

    public List<MillConfig> Mills { get; set; } = new();
}
