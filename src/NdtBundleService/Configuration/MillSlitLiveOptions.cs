namespace NdtBundleService.Configuration;

/// <summary>
/// When enabled for a mill (e.g. Mill-3), slit processing uses live NDT count from the Siemens PLC
/// and the running PO from the TM WIP bundle folder instead of the Input Slit CSV cells for those values.
/// Pipe size / formation chart still come from <see cref="NdtBundleOptions.PoPlanFolder"/> / <see cref="NdtBundleOptions.PipeSizeCsvPath"/> keyed by that PO.
/// </summary>
public sealed class MillSlitLiveOptions
{
    /// <summary>When true, <see cref="ApplyToMillNo"/> uses <see cref="WipBundleFolder"/> + S7 NDT read.</summary>
    public bool Enabled { get; set; }

    /// <summary>Mill number (1–4) whose slit rows use live data.</summary>
    public int ApplyToMillNo { get; set; } = 3;

    /// <summary>Folder with <c>WIP_MM_…</c> bundle CSV files (running PO in filename).</summary>
    public string WipBundleFolder { get; set; } = @"Z:\To SAP\TM\Bundle";

    /// <summary>
    /// Optional folder where bundle files are moved after posting (e.g. SAP accepted).
    /// Running PO detection considers both <see cref="WipBundleFolder"/> and this folder and chooses the most recently added file per mill.
    /// </summary>
    public string WipBundleAcceptedFolder { get; set; } = @"Z:\To SAP\TM\Bundle Accepted";

    /// <summary>S7 connection for <c>DB*.INT</c> NDT counter (same signal as Mill-3 plc-server).</summary>
    public MillS7NdtOptions? S7 { get; set; }
}

/// <summary>S7-300/400/1200/1500 read of NDT pipe counter (16-bit INT in a data block).</summary>
public sealed class MillS7NdtOptions
{
    public string Host { get; set; } = "";

    public short Rack { get; set; }

    public short Slot { get; set; } = 2;

    /// <summary><c>S7300</c>, <c>S7400</c>, <c>S71200</c>, <c>S71500</c>.</summary>
    public string CpuType { get; set; } = "S7300";

    public ushort DbNumber { get; set; } = 251;

    /// <summary>Byte offset of the NDT count INT in the DB (plc-server: DB251,INT6 → default 6).</summary>
    public int NdtCountByteOffset { get; set; } = 6;
}
