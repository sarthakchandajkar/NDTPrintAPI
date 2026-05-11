using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using S7.Net;
using S7.Net.Types;

namespace NdtBundleService.Services;

/// <summary>Reads DB INT NDT counter (Mill-3: DB251 byte offset per <c>MillS7NdtOptions</c>).</summary>
public sealed class S7MillNdtCountReader : IMillNdtCountReader
{
    private readonly IOptions<NdtBundleOptions> _options;
    private readonly ILogger<S7MillNdtCountReader> _logger;

    public S7MillNdtCountReader(IOptions<NdtBundleOptions> options, ILogger<S7MillNdtCountReader> logger)
    {
        _options = options;
        _logger = logger;
    }

    public Task<int?> TryReadNdtPipesCountAsync(CancellationToken cancellationToken)
    {
        var live = _options.Value.MillSlitLive;
        if (!live.Enabled || live.S7 is null || string.IsNullOrWhiteSpace(live.S7.Host))
            return Task.FromResult<int?>(null);

        var s7 = live.S7;
        if (s7.NdtCountByteOffset < 0 || s7.NdtCountByteOffset > 1_000_000)
        {
            _logger.LogWarning("MillSlitLive.S7.NdtCountByteOffset {Off} is out of range.", s7.NdtCountByteOffset);
            return Task.FromResult<int?>(null);
        }

        cancellationToken.ThrowIfCancellationRequested();
        var cpu = ParseCpu(s7.CpuType);
        var plc = new Plc(cpu, s7.Host.Trim(), s7.Rack, s7.Slot);
        try
        {
            plc.Open();
            cancellationToken.ThrowIfCancellationRequested();
            var raw = plc.Read(DataType.DataBlock, s7.DbNumber, s7.NdtCountByteOffset, VarType.Int, 1);
            if (raw is null)
                return Task.FromResult<int?>(null);
            var v = Convert.ToInt32(raw);
            if (v < 0)
                v = 0;
            return Task.FromResult<int?>(v);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "S7 NDT read failed (host {Host} DB{Db} @{Off}).", s7.Host, s7.DbNumber, s7.NdtCountByteOffset);
            return Task.FromResult<int?>(null);
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
