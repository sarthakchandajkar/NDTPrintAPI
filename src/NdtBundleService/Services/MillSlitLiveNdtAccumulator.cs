using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>PLC baseline cache; PO end clears state. Slit bundle/tag logic uses Input Slit CSV counts in <see cref="SlitMonitoringWorker"/>.</summary>
public sealed class MillSlitLiveNdtAccumulator : IMillSlitLiveNdtAccumulator
{
    private readonly IOptions<NdtBundleOptions> _options;
    private readonly ILogger<MillSlitLiveNdtAccumulator> _logger;
    private readonly object _lock = new();

    /// <summary>Key: "{mill}:{normalizedPo}".</summary>
    private string? _lastKey;

    private int? _lastRaw;

    public MillSlitLiveNdtAccumulator(IOptions<NdtBundleOptions> options, ILogger<MillSlitLiveNdtAccumulator> logger)
    {
        _options = options;
        _logger = logger;
    }

    /// <inheritdoc />
    public IReadOnlyList<int>? TryConsumeRawForBundleIncrements(string normalizedPoNumber, int millNo, int plcRawNdt)
    {
        var live = _options.Value.MillSlitLive;
        if (!live.Enabled || millNo != live.ApplyToMillNo)
            return null;

        var po = (normalizedPoNumber ?? string.Empty).Trim();
        var key = $"{millNo}:{po}";
        var raw = Math.Max(0, plcRawNdt);

        lock (_lock)
        {
            if (!string.Equals(_lastKey, key, StringComparison.OrdinalIgnoreCase))
            {
                if (_lastKey != null)
                {
                    _logger.LogInformation(
                        "MillSlitLive NDT accumulator: PO/mill key changed ({Old} → {New}); priming new PLC baseline.",
                        _lastKey,
                        key);
                }

                _lastKey = key;
                _lastRaw = null;
            }

            if (_lastRaw is null)
            {
                _lastRaw = raw;
                if (raw > 0)
                {
                    _logger.LogDebug("MillSlitLive NDT accumulator primed for {Key} at raw {Raw}.", key, raw);
                    return new[] { raw };
                }

                return Array.Empty<int>();
            }

            var last = _lastRaw.Value;
            if (raw < last)
            {
                var parts = new List<int>(2);
                if (last > 0)
                    parts.Add(last);
                _lastRaw = raw;
                if (raw > 0)
                    parts.Add(raw);
                _logger.LogInformation(
                    "MillSlitLive NDT accumulator: PLC count dropped ({Last} → {Raw}) for {Key}; slit reset. Bundle increments for this file: [{Parts}].",
                    last,
                    raw,
                    key,
                    parts.Count == 0 ? "none" : string.Join(", ", parts));
                return parts;
            }

            if (raw > last)
            {
                var d = raw - last;
                _lastRaw = raw;
                if (d > 0)
                    return new[] { d };
                return Array.Empty<int>();
            }

            _logger.LogDebug("MillSlitLive NDT accumulator: unchanged PLC count {Raw} for {Key}.", raw, key);
            return Array.Empty<int>();
        }
    }

    /// <inheritdoc />
    public void OnPoEndForMill(string normalizedPoNumber, int millNo)
    {
        var live = _options.Value.MillSlitLive;
        if (!live.Enabled || millNo != live.ApplyToMillNo)
            return;

        var po = (normalizedPoNumber ?? string.Empty).Trim();
        var want = $"{millNo}:{po}";
        lock (_lock)
        {
            if (string.Equals(_lastKey, want, StringComparison.OrdinalIgnoreCase))
            {
                _logger.LogInformation("MillSlitLive NDT accumulator reset on PO end for {Key}.", want);
                _lastKey = null;
                _lastRaw = null;
            }
        }
    }
}
