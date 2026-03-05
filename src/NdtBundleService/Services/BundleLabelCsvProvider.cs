using System.Globalization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Loads bundle label info (Specification, Type, Pipe Size, Length) from a CSV file.
/// Expected columns: PO Number, Mill No, Specification, Type, Pipe Size, Length. Key: (PO Number, Mill No).
/// </summary>
public sealed class BundleLabelCsvProvider : IBundleLabelInfoProvider
{
    private readonly NdtBundleOptions _options;
    private readonly ILogger<BundleLabelCsvProvider> _logger;

    public BundleLabelCsvProvider(IOptions<NdtBundleOptions> options, ILogger<BundleLabelCsvProvider> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<IReadOnlyDictionary<(string PoNumber, int MillNo), BundleLabelInfo>> GetBundleLabelInfoAsync(CancellationToken cancellationToken)
    {
        var path = _options.BundleLabelCsvPath;
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
        {
            _logger.LogDebug("Bundle label CSV path not configured or file not found: {Path}. Label printing will use empty label fields.", path);
            return new Dictionary<(string, int), BundleLabelInfo>();
        }

        var result = new Dictionary<(string, int), BundleLabelInfo>();

        await using var stream = File.OpenRead(path);
        using var reader = new StreamReader(stream);

        string? headerLine = await reader.ReadLineAsync(cancellationToken);
        if (headerLine is null)
            return result;

        var headers = headerLine.Split(',');
        int poIndex = Array.FindIndex(headers, h => h.Trim().Equals("PO Number", StringComparison.OrdinalIgnoreCase));
        int millIndex = Array.FindIndex(headers, h => h.Trim().Equals("Mill No", StringComparison.OrdinalIgnoreCase));
        int specIndex = Array.FindIndex(headers, h => h.Trim().Equals("Specification", StringComparison.OrdinalIgnoreCase));
        int typeIndex = Array.FindIndex(headers, h => h.Trim().Equals("Type", StringComparison.OrdinalIgnoreCase));
        int sizeIndex = Array.FindIndex(headers, h => h.Trim().Equals("Pipe Size", StringComparison.OrdinalIgnoreCase));
        int lenIndex = Array.FindIndex(headers, h => h.Trim().Equals("Length", StringComparison.OrdinalIgnoreCase));

        if (poIndex < 0 || millIndex < 0)
        {
            _logger.LogWarning("Bundle label CSV must contain at least 'PO Number' and 'Mill No'.");
            return result;
        }

        int maxIndex = new[] { poIndex, millIndex, specIndex, typeIndex, sizeIndex, lenIndex }.Where(i => i >= 0).DefaultIfEmpty(-1).Max();

        while (await reader.ReadLineAsync(cancellationToken) is { } line)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;

            var cols = line.Split(',');
            if (cols.Length <= maxIndex)
                continue;

            var poNumber = cols[poIndex].Trim();
            if (string.IsNullOrEmpty(poNumber))
                continue;

            if (!int.TryParse(cols[millIndex].Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var millNo))
                continue;

            var key = (poNumber, millNo);
            if (result.ContainsKey(key))
                continue;

            result[key] = new BundleLabelInfo
            {
                PoNumber = poNumber,
                MillNo = millNo,
                Specification = specIndex >= 0 ? cols[specIndex].Trim() : string.Empty,
                Type = typeIndex >= 0 ? cols[typeIndex].Trim() : string.Empty,
                PipeSize = sizeIndex >= 0 ? cols[sizeIndex].Trim() : string.Empty,
                Length = lenIndex >= 0 ? cols[lenIndex].Trim() : string.Empty
            };
        }

        _logger.LogInformation("Loaded bundle label info for {Count} PO/Mill(s) from {Path}", result.Count, path);
        return result;
    }
}
