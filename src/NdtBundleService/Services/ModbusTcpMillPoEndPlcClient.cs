using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NModbus;

namespace NdtBundleService.Services;

/// <summary>
/// Modbus TCP: per-mill PO-end coil (legacy) or per-mill PO_Id / slit-valid register reads (multi-mill transition mode).
/// </summary>
public sealed class ModbusTcpMillPoEndPlcClient : IPlcClient
{
    private readonly PlcPoEndOptions _plcPoEnd;
    private readonly PlcConnectionHealth _health;
    private readonly ILogger<ModbusTcpMillPoEndPlcClient> _logger;

    public ModbusTcpMillPoEndPlcClient(
        IOptions<NdtBundleOptions> bundleOptions,
        PlcConnectionHealth health,
        ILogger<ModbusTcpMillPoEndPlcClient> logger)
    {
        _plcPoEnd = bundleOptions.Value.PlcPoEnd ?? new PlcPoEndOptions();
        _health = health;
        _logger = logger;
    }

    public async Task<IReadOnlyDictionary<int, bool>> GetPoEndSignalsByMillAsync(CancellationToken cancellationToken)
    {
        var result = new Dictionary<int, bool> { [1] = false, [2] = false, [3] = false, [4] = false };
        var attempted = 0;
        var failed = 0;

        if (PlcPoEndOptions.IsModbusPoIdTransition(_plcPoEnd))
        {
            foreach (var ep in _plcPoEnd.Mills)
            {
                if (ep.MillNo is < 1 or > 4 || string.IsNullOrWhiteSpace(ep.Host))
                    continue;
                if (!ep.PoEndStatusCoilAddress.HasValue)
                    continue;
                attempted++;
                try
                {
                    result[ep.MillNo] = await ReadCoilAsync(ep, ep.PoEndStatusCoilAddress.Value, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    failed++;
                    _logger.LogWarning(ex, "Modbus PO-end status coil read failed for Mill {Mill}.", ep.MillNo);
                }
            }

            _health.RecordModbusPoll(
                attempted > 0,
                attempted > 0 && failed == 0,
                attempted == 0
                    ? "PoId mode: no mill has PoEndStatusCoilAddress and Host, so no Modbus read was attempted."
                    : null);
            return result;
        }

        foreach (var ep in _plcPoEnd.Mills)
        {
            if (ep.MillNo is < 1 or > 4)
            {
                _logger.LogWarning("Ignoring PlcPoEnd mill config with invalid MillNo {Mill}.", ep.MillNo);
                continue;
            }

            if (string.IsNullOrWhiteSpace(ep.Host))
            {
                _logger.LogWarning("PlcPoEnd Mill {Mill} has empty Host; signal reads false.", ep.MillNo);
                continue;
            }

            attempted++;
            try
            {
                result[ep.MillNo] = await ReadCoilAsync(ep, ep.CoilAddress, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                failed++;
                _logger.LogWarning(ex, "Modbus read failed for Mill {Mill} at {Host}:{Port}; treating PO end as false.", ep.MillNo, ep.Host, ep.Port);
                result[ep.MillNo] = false;
            }
        }

        _health.RecordModbusPoll(
            attempted > 0,
            attempted > 0 && failed == 0,
            attempted == 0 ? "PlcPoEnd.Mills has no endpoints with a Host configured." : null);

        return result;
    }

    public async Task<bool> GetPoEndAsync(CancellationToken cancellationToken)
    {
        var map = await GetPoEndSignalsByMillAsync(cancellationToken).ConfigureAwait(false);
        return map.Values.Any(v => v);
    }

    public async Task<IReadOnlyDictionary<int, MillPoPlcSnapshot>?> ReadMillPoSnapshotsAsync(CancellationToken cancellationToken)
    {
        if (!PlcPoEndOptions.IsModbusPoIdTransition(_plcPoEnd))
            return null;

        var result = new Dictionary<int, MillPoPlcSnapshot>();
        var attempted = 0;
        var failed = 0;

        foreach (var ep in _plcPoEnd.Mills)
        {
            if (ep.MillNo is < 1 or > 4)
                continue;
            if (string.IsNullOrWhiteSpace(ep.Host))
            {
                _logger.LogWarning("PlcPoEnd Mill {Mill} has empty Host; skipping PO_Id snapshot.", ep.MillNo);
                continue;
            }

            attempted++;
            try
            {
                result[ep.MillNo] = await ReadSnapshotForMillAsync(ep, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                failed++;
                _logger.LogWarning(ex, "Modbus PO_Id snapshot failed for Mill {Mill}; marking read not OK.", ep.MillNo);
                result[ep.MillNo] = new MillPoPlcSnapshot { ReadOk = false };
            }
        }

        _health.RecordModbusPoll(
            attempted > 0,
            attempted > 0 && failed == 0,
            attempted == 0 ? "No mill with Host configured for PO_Id snapshot." : null);

        return result;
    }

    private async Task<bool> ReadCoilAsync(MillModbusPoEndEndpoint ep, ushort coilAddress, CancellationToken cancellationToken)
    {
        using var tcp = new TcpClient();
        var timeout = Math.Clamp(_plcPoEnd.ModbusConnectTimeoutMs, 500, 60_000);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linked.CancelAfter(timeout);

        await tcp.ConnectAsync(ep.Host.Trim(), ep.Port, linked.Token).ConfigureAwait(false);
        tcp.SendTimeout = timeout;
        tcp.ReceiveTimeout = timeout;

        var factory = new ModbusFactory();
        using var master = factory.CreateMaster(tcp);
        var coils = master.ReadCoils(ep.SlaveId, coilAddress, 1);
        return coils.Length > 0 && coils[0];
    }

    private async Task<MillPoPlcSnapshot> ReadSnapshotForMillAsync(MillModbusPoEndEndpoint ep, CancellationToken cancellationToken)
    {
        using var tcp = new TcpClient();
        var timeout = Math.Clamp(_plcPoEnd.ModbusConnectTimeoutMs, 500, 60_000);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linked.CancelAfter(timeout);

        await tcp.ConnectAsync(ep.Host.Trim(), ep.Port, linked.Token).ConfigureAwait(false);
        tcp.SendTimeout = timeout;
        tcp.ReceiveTimeout = timeout;

        var factory = new ModbusFactory();
        using var master = factory.CreateMaster(tcp);

        var poIdWords = WordCount(ep.PoIdBitSize);
        var poIdRegs = ReadRegisters(master, ep.SlaveId, ep.PoIdRegisterKind, ep.PoIdStartAddress, poIdWords);
        var poId = RegistersToInt(poIdRegs, 0, ep.PoIdBitSize, ep.SwapPoIdDintWordOrder);

        int? poType = null;
        if (ep.PoTypeIdStartAddress.HasValue)
        {
            var kind = string.IsNullOrWhiteSpace(ep.PoTypeIdRegisterKind) ? ep.PoIdRegisterKind : ep.PoTypeIdRegisterKind;
            var w = WordCount(ep.PoTypeIdBitSize);
            var regs = ReadRegisters(master, ep.SlaveId, kind!, ep.PoTypeIdStartAddress.Value, w);
            poType = RegistersToInt(regs, 0, ep.PoTypeIdBitSize, ep.SwapPoTypeIdDintWordOrder);
        }

        var slitValid = true;
        if (ep.SlitEntryValidCoilAddress.HasValue)
            slitValid = ReadCoilOnMaster(master, ep.SlaveId, ep.SlitEntryValidCoilAddress.Value);

        int? slitCount = null;
        if (ep.SlitEntryCountStartAddress.HasValue)
        {
            var w = WordCount(ep.SlitEntryCountBitSize);
            var regs = ReadRegisters(master, ep.SlaveId, ep.SlitEntryCountRegisterKind, ep.SlitEntryCountStartAddress.Value, w);
            slitCount = RegistersToInt(regs, 0, ep.SlitEntryCountBitSize, ep.SwapSlitEntryCountDintWordOrder);
        }

        return new MillPoPlcSnapshot
        {
            PoId = poId,
            PoTypeId = poType,
            SlitEntryValid = slitValid,
            SlitEntryCount = slitCount,
            ReadOk = true
        };
    }

    private static ushort[] ReadRegisters(IModbusMaster master, byte slaveId, string kind, ushort start, int wordCount)
    {
        if (wordCount <= 0)
            wordCount = 1;
        var k = kind.Trim();
        if (string.Equals(k, "Input", StringComparison.OrdinalIgnoreCase))
            return master.ReadInputRegisters(slaveId, start, (ushort)wordCount);
        return master.ReadHoldingRegisters(slaveId, start, (ushort)wordCount);
    }

    private static bool ReadCoilOnMaster(IModbusMaster master, byte slaveId, ushort coilAddress)
    {
        var coils = master.ReadCoils(slaveId, coilAddress, 1);
        return coils.Length > 0 && coils[0];
    }

    private static int WordCount(int bitSize) => bitSize >= 32 ? 2 : 1;

    private static int RegistersToInt(ushort[] regs, int start, int bitSize, bool swapDintWords)
    {
        if (regs.Length == 0)
            return 0;
        if (bitSize >= 32 && start + 1 < regs.Length)
        {
            var hi = regs[start];
            var lo = regs[start + 1];
            if (swapDintWords)
                (hi, lo) = (lo, hi);
            unchecked
            {
                return (int)((uint)hi << 16 | lo);
            }
        }

        return unchecked((short)regs[start]);
    }

    /// <inheritdoc />
    public async Task AcknowledgeMesPoChangeAsync(int millNo, CancellationToken cancellationToken)
    {
        MillModbusPoEndEndpoint? ep = null;
        foreach (var m in _plcPoEnd.Mills)
        {
            if (m.MillNo == millNo)
            {
                ep = m;
                break;
            }
        }

        if (ep is null || string.IsNullOrWhiteSpace(ep.Host) || !ep.MesAckWriteCoilAddress.HasValue)
            return;

        var ackAddr = ep.MesAckWriteCoilAddress.Value;
        var pulseMs = Math.Clamp(ep.MesAckPulseMs, 0, 60_000);

        try
        {
            await WriteCoilAsync(ep, ackAddr, true, cancellationToken).ConfigureAwait(false);
            _logger.LogInformation(
                "MES ack: wrote coil {Addr}=true at Mill {Mill} {Host}:{Port} (RESETMESTOPLC / M20.2 mapping).",
                ackAddr,
                millNo,
                ep.Host.Trim(),
                ep.Port);

            if (pulseMs > 0)
            {
                await Task.Delay(pulseMs, cancellationToken).ConfigureAwait(false);
                await WriteCoilAsync(ep, ackAddr, false, cancellationToken).ConfigureAwait(false);
                _logger.LogDebug("MES ack pulse: coil {Addr}=false after {Ms}ms.", ackAddr, pulseMs);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "MES ack write failed for Mill {Mill}; PLC POChangeTOMES may stay latched.", millNo);
        }
    }

    private async Task WriteCoilAsync(MillModbusPoEndEndpoint ep, ushort coilAddress, bool value, CancellationToken cancellationToken)
    {
        using var tcp = new TcpClient();
        var timeout = Math.Clamp(_plcPoEnd.ModbusConnectTimeoutMs, 500, 60_000);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linked.CancelAfter(timeout);

        await tcp.ConnectAsync(ep.Host.Trim(), ep.Port, linked.Token).ConfigureAwait(false);
        tcp.SendTimeout = timeout;
        tcp.ReceiveTimeout = timeout;

        var factory = new ModbusFactory();
        using var master = factory.CreateMaster(tcp);
        master.WriteSingleCoil(ep.SlaveId, coilAddress, value);
    }
}
