using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using S7.Net;
using S7.Net.Types;

namespace NdtBundleService.Services;

/// <summary>
/// Siemens S7 PO-end polling for mills 1–4 (same M-bit / DB251 PO_Id map as plc-server).
/// </summary>
public sealed class S7MillPoEndPlcClient : IPlcClient
{
    private readonly PlcPoEndOptions _plcPoEnd;
    private readonly PlcConnectionHealth _health;
    private readonly ILogger<S7MillPoEndPlcClient> _logger;
    private readonly object _cacheLock = new();
    private Dictionary<int, MillPoPlcSnapshot> _lastSnapshots = new();

    public S7MillPoEndPlcClient(
        IOptions<NdtBundleOptions> bundleOptions,
        PlcConnectionHealth health,
        ILogger<S7MillPoEndPlcClient> logger)
    {
        _plcPoEnd = bundleOptions.Value.PlcPoEnd ?? new PlcPoEndOptions();
        _health = health;
        _logger = logger;
    }

    public async Task<IReadOnlyDictionary<int, bool>> GetPoEndSignalsByMillAsync(CancellationToken cancellationToken)
    {
        var result = new Dictionary<int, bool> { [1] = false, [2] = false, [3] = false, [4] = false };
        var snapshots = new Dictionary<int, MillPoPlcSnapshot>();
        var attempted = 0;
        var failed = 0;

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

            if (string.IsNullOrWhiteSpace(ep.S7PoEndAddress))
            {
                _logger.LogWarning("PlcPoEnd Mill {Mill} has empty S7PoEndAddress; signal reads false.", ep.MillNo);
                continue;
            }

            attempted++;
            try
            {
                var (poEnd, poId) = await ReadPoEndAndPoIdAsync(ep, cancellationToken).ConfigureAwait(false);
                result[ep.MillNo] = poEnd;
                snapshots[ep.MillNo] = new MillPoPlcSnapshot
                {
                    PoId = poId,
                    SlitEntryValid = true,
                    ReadOk = true
                };
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                failed++;
                _logger.LogWarning(
                    ex,
                    "S7 PO-end read failed for Mill {Mill} at {Host}; treating PO end as false.",
                    ep.MillNo,
                    ep.Host);
                result[ep.MillNo] = false;
                snapshots[ep.MillNo] = new MillPoPlcSnapshot { ReadOk = false };
            }
        }

        lock (_cacheLock)
            _lastSnapshots = snapshots;

        _health.RecordModbusPoll(
            attempted > 0,
            attempted > 0 && failed == 0,
            attempted == 0 ? "PlcPoEnd.Mills has no S7 endpoints with Host and S7PoEndAddress configured." : null);

        return result;
    }

    public async Task<bool> GetPoEndAsync(CancellationToken cancellationToken)
    {
        var map = await GetPoEndSignalsByMillAsync(cancellationToken).ConfigureAwait(false);
        return map.Values.Any(v => v);
    }

    public Task<IReadOnlyDictionary<int, MillPoPlcSnapshot>?> ReadMillPoSnapshotsAsync(CancellationToken cancellationToken)
    {
        lock (_cacheLock)
            return Task.FromResult<IReadOnlyDictionary<int, MillPoPlcSnapshot>?>(
                _lastSnapshots.Count == 0 ? null : new Dictionary<int, MillPoPlcSnapshot>(_lastSnapshots));
    }

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

        if (ep is null || string.IsNullOrWhiteSpace(ep.Host) || string.IsNullOrWhiteSpace(ep.S7MesAckAddress))
            return;

        var pulseMs = Math.Clamp(ep.MesAckPulseMs, 0, 60_000);
        var ackTag = ep.S7MesAckAddress.Trim();

        try
        {
            await WithPlcVoidAsync(ep, cancellationToken, plc =>
            {
                plc.Write(ackTag, true);
            }).ConfigureAwait(false);

            _logger.LogInformation(
                "MES ack: wrote {Tag}=true at Mill {Mill} {Host} (S7 RESETMESTOPLC mapping).",
                ackTag,
                millNo,
                ep.Host.Trim());

            if (pulseMs > 0)
            {
                await Task.Delay(pulseMs, cancellationToken).ConfigureAwait(false);
                await WithPlcVoidAsync(ep, cancellationToken, plc =>
                {
                    plc.Write(ackTag, false);
                }).ConfigureAwait(false);
                _logger.LogDebug("MES ack pulse: {Tag}=false after {Ms}ms.", ackTag, pulseMs);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "MES ack S7 write failed for Mill {Mill}; PLC PO-end latch may stay set.", millNo);
        }
    }

    private async Task<(bool PoEnd, int PoId)> ReadPoEndAndPoIdAsync(
        MillModbusPoEndEndpoint ep,
        CancellationToken cancellationToken)
    {
        return await WithPlcAsync(ep, cancellationToken, plc =>
        {
            cancellationToken.ThrowIfCancellationRequested();
            var poEndObj = plc.Read(ep.S7PoEndAddress.Trim());
            var poEnd = poEndObj is bool b && b;

            var poId = 0;
            if (ep.S7PoIdByteOffset >= 0)
            {
                var raw = plc.Read(DataType.DataBlock, ep.S7PoIdDbNumber, ep.S7PoIdByteOffset, VarType.Int, 1);
                if (raw is not null)
                    poId = Convert.ToInt32(raw);
            }

            return Task.FromResult((poEnd, poId));
        }).ConfigureAwait(false);
    }

    private Task WithPlcVoidAsync(
        MillModbusPoEndEndpoint ep,
        CancellationToken cancellationToken,
        Action<Plc> action) =>
        WithPlcAsync(ep, cancellationToken, plc =>
        {
            action(plc);
            return Task.FromResult(0);
        });

    private async Task<T> WithPlcAsync<T>(
        MillModbusPoEndEndpoint ep,
        CancellationToken cancellationToken,
        Func<Plc, Task<T>> action)
    {
        var slots = new[] { ep.Slot, (short)1 }.Distinct().ToArray();
        Exception? lastErr = null;

        foreach (var slot in slots)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var cpu = ParseCpu(ep.CpuType);
            var plc = new Plc(cpu, ep.Host.Trim(), ep.Rack, slot);
            try
            {
                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeoutCts.CancelAfter(Math.Clamp(ep.S7ConnectTimeoutMs, 500, 60_000));

                await Task.Run(() => plc.Open(), timeoutCts.Token).ConfigureAwait(false);
                return await action(plc).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                lastErr = ex;
            }
            finally
            {
                try
                {
                    plc.Close();
                }
                catch
                {
                    /* ignore */
                }
            }
        }

        throw lastErr ?? new InvalidOperationException($"S7 connection failed for Mill {ep.MillNo} at {ep.Host}.");
    }

    private static CpuType ParseCpu(string? raw)
    {
        return raw?.Trim().ToUpperInvariant() switch
        {
            "S71200" => CpuType.S71200,
            "S71500" => CpuType.S71500,
            "S7400" => CpuType.S7400,
            _ => CpuType.S7300,
        };
    }
}
