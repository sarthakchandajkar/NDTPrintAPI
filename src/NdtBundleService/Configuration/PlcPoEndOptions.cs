namespace NdtBundleService.Configuration;

/// <summary>
/// Per-mill PO end signals from PLCs (e.g. Modbus TCP coil per mill). Bound from NdtBundle:PlcPoEnd.
/// </summary>
public sealed class PlcPoEndOptions
{
    /// <summary>When true, <see cref="SlitMonitoringWorker"/> polls PLCs and runs PO-end workflow on rising edges.</summary>
    public bool Enabled { get; set; }

    /// <summary><c>Stub</c> (no I/O) or <c>ModbusTcp</c> (read one coil per mill).</summary>
    public string Driver { get; set; } = "Stub";

    /// <summary>
    /// <c>CoilRisingEdge</c>: read PO-end / POChangeTOMES coil per entry in <see cref="Mills"/>. <c>ModbusPoIdTransition</c> is optional (PLC PO_Id registers).
    /// </summary>
    public string DetectionMode { get; set; } = "CoilRisingEdge";

    /// <summary>
    /// When true, each PO end also advances the current PO plan file (see <c>ICurrentPoPlanService.AdvanceToNextPoAsync</c>).
    /// Use only when PO plan files rotate one-at-a-time for the whole line; with four concurrent POs leave false.
    /// </summary>
    public bool AdvancePoPlanFileOnPoEnd { get; set; }

    /// <summary>TCP connect timeout per PLC read attempt.</summary>
    public int ModbusConnectTimeoutMs { get; set; } = 3000;

    /// <summary>Inclusive lower bound for a valid PO_Id from the PLC (see mill Modbus mapping).</summary>
    public int MinValidPoId { get; set; } = 1;

    /// <summary>Inclusive upper bound for a valid PO_Id from the PLC.</summary>
    public int MaxValidPoId { get; set; } = int.MaxValue;

    /// <summary>Invariant-culture format for converting PLC PO_Id to PO number string (e.g. <c>{0}</c> or <c>PO{0}</c>).</summary>
    public string PoNumberFormatFromPlc { get; set; } = "{0}";

    /// <summary>
    /// When current PO_Id is invalid but previous was valid, update tracked previous to current without firing PO end (idle / drop-out handling).
    /// </summary>
    public bool ResyncPrevPoWhenCurrentInvalid { get; set; }

    /// <summary>Modbus endpoints to poll (e.g. a single Mill 4 PLC).</summary>
    public List<MillModbusPoEndEndpoint> Mills { get; set; } = new();

    /// <summary>
    /// After a successful PO-end workflow, write the per-mill MES ack coil (PLC <c>RESETMESTOPLC</c> / e.g. M20.2). If false, ack is still written when configured (use with care).
    /// </summary>
    public bool WriteMesAckOnlyOnWorkflowSuccess { get; set; } = true;

    /// <summary>True when <see cref="DetectionMode"/> is Modbus PO_Id transition detection.</summary>
    public static bool IsModbusPoIdTransition(PlcPoEndOptions? o) =>
        o is not null &&
        string.Equals(o.DetectionMode, "ModbusPoIdTransition", StringComparison.OrdinalIgnoreCase);
}

/// <summary>Modbus TCP endpoint for one mill: PO-end coil and/or PO_Id / optional slit-valid Modbus map.</summary>
public sealed class MillModbusPoEndEndpoint
{
    public int MillNo { get; set; }

    /// <summary>PLC IP or hostname.</summary>
    public string Host { get; set; } = string.Empty;

    public int Port { get; set; } = 502;

    public byte SlaveId { get; set; } = 1;

    /// <summary>
    /// Zero-based Modbus coil (PDU) mapped to PLC POChangeTOMES latch (e.g. M20.1). Read on each poll; rising edge triggers PO-end workflow.
    /// </summary>
    public ushort CoilAddress { get; set; }

    /// <summary>
    /// Zero-based Modbus coil mapped to PLC RESETMESTOPLC (e.g. M20.2). Written after workflow (and optional pulse) to reset <see cref="CoilAddress"/> per ladder Network 106.
    /// </summary>
    public ushort? MesAckWriteCoilAddress { get; set; }

    /// <summary>
    /// After writing ack true: wait this many ms then write false (pulse). Use 0 to write true once only (if your PLC expects a level).
    /// </summary>
    public int MesAckPulseMs { get; set; } = 150;

    /// <summary>
    /// When set in <c>ModbusPoIdTransition</c> mode, <see cref="IPlcClient.GetPoEndSignalsByMillAsync"/> reflects this coil for dashboard parity with PLC latched flags.
    /// </summary>
    public ushort? PoEndStatusCoilAddress { get; set; }

    /// <summary><c>Holding</c> or <c>Input</c> for PO_Id registers.</summary>
    public string PoIdRegisterKind { get; set; } = "Holding";

    /// <summary>Zero-based start address for PO_Id (16- or 32-bit per <see cref="PoIdBitSize"/>).</summary>
    public ushort PoIdStartAddress { get; set; }

    /// <summary>16 or 32.</summary>
    public int PoIdBitSize { get; set; } = 16;

    /// <summary>Swap low/high 16-bit words when reading 32-bit PO_Id (gateway / endian variants).</summary>
    public bool SwapPoIdDintWordOrder { get; set; }

    public ushort? PoTypeIdStartAddress { get; set; }

    /// <summary>If null, uses <see cref="PoIdRegisterKind"/>.</summary>
    public string? PoTypeIdRegisterKind { get; set; }

    public int PoTypeIdBitSize { get; set; } = 16;

    public bool SwapPoTypeIdDintWordOrder { get; set; }

    /// <summary>Optional coil; when null, slit entry is treated as always valid for transition gating.</summary>
    public ushort? SlitEntryValidCoilAddress { get; set; }

    public ushort? SlitEntryCountStartAddress { get; set; }

    public string SlitEntryCountRegisterKind { get; set; } = "Holding";

    public int SlitEntryCountBitSize { get; set; } = 16;

    public bool SwapSlitEntryCountDintWordOrder { get; set; }

    /// <summary>
    /// When true and <see cref="SlitEntryCountStartAddress"/> is set, suppress PO_Id transition until slit entry count changes (optional stricter gating).
    /// </summary>
    public bool RequireSlitEntryCountChange { get; set; }

    /// <summary>
    /// When true, require latest slit CSV PO for this mill to match the ended PLC PO (after <see cref="PlcPoEndOptions.PoNumberFormatFromPlc"/>); skip workflow on mismatch.
    /// </summary>
    public bool RequireLatestCsvPoMatchesEndedPlcPo { get; set; }
}
