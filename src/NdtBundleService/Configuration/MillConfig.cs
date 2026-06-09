namespace NdtBundleService.Configuration;

/// <summary>
/// Per-mill Siemens S7 PO-change handshake mapping (trigger M-bit from PLC, ack M-bit to PLC).
/// Bound from <see cref="PlcHandshakeOptions.Mills"/>.
/// </summary>
public sealed class MillConfig
{
    /// <summary>Display name (e.g. <c>Mill-1</c>).</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>Mill number 1–4. When 0, parsed from <see cref="Name"/> (<c>Mill-N</c>).</summary>
    public int MillNo { get; set; }

    /// <summary>PLC IP address (ISO-on-TCP port 102).</summary>
    public string IpAddress { get; set; } = string.Empty;

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
}
