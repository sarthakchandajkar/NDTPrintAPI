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

    /// <summary>
    /// When true, all mills use S7 read-only (counts, line running, hooter) — no PO-change handshake on any mill.
    /// Per-mill <see cref="MillConfig.PoEndSource"/> = <c>File</c> or <c>TcpOpen</c> also skips handshake for that mill.
    /// </summary>
    public bool TelemetryOnly { get; set; }

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
    /// When true, M40.6 rising edge does <b>not</b> auto-ack or flush. MES arms a pending
    /// manual confirm; the operator confirms on the dashboard after the last slit finishes
    /// (flush remainder using live PLC NDT merge, then MES ack). PLC may leave the trigger TRUE until then.
    /// </summary>
    public bool ManualConfirmPoEnd { get; set; }

    /// <summary>
    /// Optional DB number for slit-end bit (PLC→MES), e.g. <c>270</c> for <c>DB270.DBX13.0</c> (<c>L1_ButtEnd</c>).
    /// When &gt; 0, rising edge is read from DataBlock at
    /// <see cref="SlitEndTriggerByte"/>.<see cref="SlitEndTriggerBit"/> (PLC resets the bit; MES does not write).
    /// When ≤ 0, <see cref="SlitEndTriggerByte"/> is treated as a merker (M) address if ≥ 0.
    /// When neither DB nor merker is configured, slit-end falls back to Slit ID change on DB251.
    /// </summary>
    public int SlitEndTriggerDbNumber { get; set; }

    /// <summary>
    /// Byte offset for slit-end bit: DB byte when <see cref="SlitEndTriggerDbNumber"/> &gt; 0,
    /// otherwise merker byte. When &lt; 0 and no DB is set, slit-end uses Slit ID change.
    /// </summary>
    public int SlitEndTriggerByte { get; set; } = -1;

    /// <summary>Bit within the slit-end trigger byte (0–7).</summary>
    public int SlitEndTriggerBit { get; set; }

    /// <summary>True when a DB or merker slit-end bit is configured (not Slit-ID-only mode).</summary>
    public bool IsSlitEndBitTriggerConfigured =>
        SlitEndTriggerDbNumber > 0
            ? SlitEndTriggerByte >= 0
            : SlitEndTriggerByte >= 0;

    /// <summary>
    /// NDT count DB for live close (default 251). Config-driven; mirrors MillSlitLive.S7.DbNumber.
    /// </summary>
    public int NdtCountDb { get; set; } = 251;

    /// <summary>When true, read line-running bit from each mill PLC for dashboard SCADA lamp.</summary>
    public bool ReadLineRunning { get; set; } = true;

    /// <summary>DB250.DBX2.0 — line running for Mill-1 … Mill-4 (same offset on each mill PLC).</summary>
    public int LineRunningDbNumber { get; set; } = 250;

    public int LineRunningByteOffset { get; set; } = 2;

    public int LineRunningBit { get; set; } = 0;

    /// <summary>
    /// Consecutive poll cycles with trigger FALSE required after a completed handshake before the next
    /// rising edge is accepted (filters immediate PLC re-pulses on the same PO change).
    /// </summary>
    public int MinimumTriggerFalsePollsBeforeRearm { get; set; } = 1;

    /// <summary>
    /// When true, a PO-end trigger already latched TRUE at service connect is cleared via the MES ack sequence
    /// instead of waiting indefinitely for the PLC to clear without MES ack.
    /// </summary>
    public bool RecoverLatchedTriggerAtStartup { get; set; } = true;

    /// <summary>
    /// When <see cref="RecoverLatchedTriggerAtStartup"/> runs, also execute the PO end workflow (close bundles, wait for new WIP).
    /// Leave false when a latched trigger may be stale while the same PO is still running on the mill.
    /// </summary>
    public bool RunPoEndWorkflowOnStartupRecovery { get; set; }

    /// <summary>
    /// When true (default), persist <c>Handshake_Event</c> audit rows for each PO-change handshake.
    /// </summary>
    public bool HandshakeAuditEnabled { get; set; } = true;

    /// <summary>
    /// M40.6 (trigger) TRUE longer than this without a completed handshake → WRN + status alert. Default 30.
    /// </summary>
    public int StuckTriggerAlarmSeconds { get; set; } = 30;

    /// <summary>Ack merker write attempts before ERR + alert. Default 3.</summary>
    public int AckWriteRetryCount { get; set; } = 3;

    /// <summary>Initial backoff (ms) between ack write retries; doubles each attempt. Default 100.</summary>
    public int AckWriteRetryInitialBackoffMs { get; set; } = 100;

    public List<MillConfig> Mills { get; set; } = new();
}
