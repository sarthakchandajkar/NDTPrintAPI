using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.PlcHandshake.S7;
using S7.Net;
using S7.Net.Types;

namespace NdtBundleService.Services;

/// <summary>
/// Reads DB INT NDT counter via the shared mill <see cref="IS7ConnectionProvider"/>
/// (no independent <c>Plc.Open()</c>).
/// </summary>
public sealed class S7MillNdtCountReader : IMillNdtCountReader
{
    private readonly IOptions<NdtBundleOptions> _options;
    private readonly IS7ConnectionProviderRegistry _s7Registry;
    private readonly ILogger<S7MillNdtCountReader> _logger;
    private bool _loggedMissingProvider;

    public S7MillNdtCountReader(
        IOptions<NdtBundleOptions> options,
        IS7ConnectionProviderRegistry s7Registry,
        ILogger<S7MillNdtCountReader> logger)
    {
        _options = options;
        _s7Registry = s7Registry;
        _logger = logger;
    }

    public async Task<int?> TryReadNdtPipesCountAsync(CancellationToken cancellationToken)
    {
        var live = _options.Value.MillSlitLive;
        // PLC read is allowed whenever S7 is configured. MillSlitLive.Enabled gates slit CSV live overrides only.
        if (live.S7 is null || string.IsNullOrWhiteSpace(live.S7.Host))
            return null;

        var s7 = live.S7;
        if (s7.NdtCountByteOffset < 0 || s7.NdtCountByteOffset > 1_000_000)
        {
            _logger.LogWarning("MillSlitLive.S7.NdtCountByteOffset {Off} is out of range.", s7.NdtCountByteOffset);
            return null;
        }

        cancellationToken.ThrowIfCancellationRequested();

        var millNo = live.ApplyToMillNo is >= 1 and <= 4 ? live.ApplyToMillNo : 0;
        var provider = millNo > 0 ? _s7Registry.TryGet(millNo) : null;
        if (provider is null)
        {
            if (!_loggedMissingProvider)
            {
                _loggedMissingProvider = true;
                _logger.LogWarning(
                    "S7 NDT read skipped: no shared IS7ConnectionProvider for mill {Mill} (enable PlcHandshake for that mill).",
                    millNo);
            }

            return null;
        }

        if (!provider.IsHealthy && !provider.IsConnected)
            return null;

        try
        {
            var dbNumber = (int)s7.DbNumber;
            var offset = s7.NdtCountByteOffset;
            var value = await provider.ReadAsync(ops =>
            {
                var raw = ops.Read(DataType.DataBlock, dbNumber, offset, VarType.Int, 1);
                if (raw is null)
                    return (int?)null;
                var v = Convert.ToInt32(raw);
                return v < 0 ? 0 : v;
            }, cancellationToken).ConfigureAwait(false);

            return value;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (S7ConnectionReentrancyException)
        {
            throw;
        }
        catch (Exception)
        {
            // Provider logs failed↔ok transitions; avoid per-poll stack traces (RC-1 soak requirement).
            return null;
        }
    }
}
