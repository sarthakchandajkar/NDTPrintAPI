namespace NdtBundleService.Configuration;

/// <summary>
/// Per-mill Siemens S7 PO-change handshake mapping (trigger M-bit from PLC, ack M-bit to PLC).
/// Bound from <see cref="PlcHandshakeOptions.Mills"/>.
/// </summary>
public sealed class MillConfig
{
    /// <summary>Display name (e.g. <c>Mill-1</c>).</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// PO-end trigger: <c>Plc</c> (S7 handshake), <c>File</c> (WIP filename), or <c>TcpOpen</c> (Phase 6).
    /// Defaults to <c>File</c> when missing or unknown.
    /// </summary>
    public string PoEndSource { get; set; } = "File";

    /// <summary>Mill number 1–4. When 0, parsed from <see cref="Name"/> (<c>Mill-N</c>).</summary>
    public int MillNo { get; set; }

    /// <summary>
    /// When false, no S7 handshake loop is started for this mill (frees the PLC PG connection slot for TIA Portal).
    /// Mill-4 production uses <c>PoEndSource=File</c> with <c>PlcHandshakeEnabled=false</c>.
    /// Set true and restart to enable, or use <c>POST /api/Settings/plc/mill/{millNo}/connect</c> when the loop is running.
    /// </summary>
    public bool PlcHandshakeEnabled { get; set; } = true;

    /// <summary>PLC IP address (ISO-on-TCP port 102).</summary>
    public string IpAddress { get; set; } = string.Empty;

    /// <summary>TCP open-communication host for <c>PoEndSource=TcpOpen</c> (falls back to <see cref="IpAddress"/>).</summary>
    public string? TcpOpenCommHost { get; set; }

    /// <summary>TCP open-communication port (open-comm / AG_SEND pool — not S7 port 102).</summary>
    public int TcpOpenPort { get; set; }

    /// <summary>Legacy alias for <see cref="TcpOpenPort"/>; used when <see cref="TcpOpenPort"/> is 0.</summary>
    public int TcpOpenCommPort { get; set; }

    /// <summary>TCP connect timeout (ms) for <c>PoEndSource=TcpOpen</c>.</summary>
    public int TcpOpenConnectTimeoutMs { get; set; } = 5000;

    /// <summary>TCP read timeout (ms); 0 = no read timeout (persistent connection, reconnect handles loss).</summary>
    public int TcpOpenReceiveTimeoutMs { get; set; }

    /// <summary>S7 rack (typically 0).</summary>
    public short Rack { get; set; }

    /// <summary>Primary S7 slot (typically 2 on S7-300).</summary>
    public short Slot { get; set; } = 2;

    /// <summary>Fallback slots tried after <see cref="Slot"/> (default: slot 1).</summary>
    public List<short> SlotFallback { get; set; } = new() { 1 };

    /// <summary><c>S7300</c>, <c>S7400</c>, <c>S71200</c>, or <c>S71500</c>.</summary>
    public string CpuType { get; set; } = "S7300";

    /// <summary>Merker byte for PLC→MES trigger (e.g. 40 for M40.6).</summary>
    public int TriggerByte { get; set; }

    /// <summary>Bit index 0–7 within <see cref="TriggerByte"/>.</summary>
    public int TriggerBit { get; set; }

    /// <summary>Merker byte for MES→PLC ack (e.g. 40 for M40.7).</summary>
    public int AckByte { get; set; }

    /// <summary>Bit index 0–7 within <see cref="AckByte"/>.</summary>
    public int AckBit { get; set; }

    /// <summary>Per-mill poll interval override; 0 uses <see cref="PlcHandshakeOptions.PollIntervalMs"/>.</summary>
    public int PollIntervalMs { get; set; }

    /// <summary>S7 connect/read timeout for this mill.</summary>
    public int ConnectTimeoutMs { get; set; } = 8000;

    /// <summary>Max wait for trigger to clear after ack (ms).</summary>
    public int TriggerClearTimeoutMs { get; set; } = 60_000;

    /// <summary>Optional NDT bundle hooter (Mill-1: MW56/MW58 compare → Q6.7 pulse).</summary>
    public MillHooterOptions? Hooter { get; set; }

    public int ResolveMillNo()
    {
        if (MillNo is >= 1 and <= 4)
            return MillNo;

        var name = (Name ?? string.Empty).Trim();
        if (name.StartsWith("Mill-", StringComparison.OrdinalIgnoreCase) &&
            int.TryParse(name.AsSpan(5), out var n) &&
            n is >= 1 and <= 4)
            return n;

        return 0;
    }

    public string TriggerAddress => $"M{TriggerByte}.{TriggerBit}";

    public string AckAddress => $"M{AckByte}.{AckBit}";

    public string? ResolveTcpOpenHost()
    {
        if (!string.IsNullOrWhiteSpace(TcpOpenCommHost))
            return TcpOpenCommHost.Trim();

        return string.IsNullOrWhiteSpace(IpAddress) ? null : IpAddress.Trim();
    }

    public int ResolveTcpOpenPort() => TcpOpenPort > 0 ? TcpOpenPort : TcpOpenCommPort;
}
