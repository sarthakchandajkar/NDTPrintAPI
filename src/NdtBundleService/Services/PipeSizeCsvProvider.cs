using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Loads PO Number -> Pipe Size from a CSV file in a separate folder.
/// Expected columns: PO Number, Pipe Size. Rows are linked to Input Slit data by PO Number.
/// </summary>
public sealed class PipeSizeCsvProvider : IPipeSizeProvider
{
    private readonly NdtBundleOptions _options;
    private readonly ILogger<PipeSizeCsvProvider> _logger;

    public PipeSizeCsvProvider(IOptions<NdtBundleOptions> options, ILogger<PipeSizeCsvProvider> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<IReadOnlyDictionary<string, string>> GetPipeSizeByPoAsync(CancellationToken cancellationToken)
    {
        var path = _options.PipeSizeCsvPath;
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
        {
            _logger.LogWarning("Pipe size CSV path not configured or file not found: {Path}. Size-based bundle logic will use Default formation only.", path);
            return new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        }

        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        await using var stream = File.OpenRead(path);
        using var reader = new StreamReader(stream);

        string? headerLine = await reader.ReadLineAsync(cancellationToken);
        if (headerLine is null)
            return result;

        var headers = headerLine.Split(',');

        // Support both generic format and the WIP file format.
        // PO: "PO Number" or "PO_No"
        int poIndex = Array.FindIndex(headers, h =>
            h.Trim().Equals("PO Number", StringComparison.OrdinalIgnoreCase) ||
            h.Trim().Equals("PO_No", StringComparison.OrdinalIgnoreCase));
        int sizeIndex = Array.FindIndex(headers, h => h.Trim().Equals("Pipe Size", StringComparison.OrdinalIgnoreCase));

        if (poIndex < 0 || sizeIndex < 0)
        {
            _logger.LogWarning("Pipe size CSV does not contain expected columns 'PO Number'/'PO_No' and 'Pipe Size'.");
            return result;
        }

        while (await reader.ReadLineAsync(cancellationToken) is { } line)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;

            var cols = line.Split(',');
            if (cols.Length <= Math.Max(poIndex, sizeIndex))
                continue;

            var poNumber = cols[poIndex].Trim();
            var pipeSize = cols[sizeIndex].Trim();

            if (string.IsNullOrEmpty(poNumber))
                continue;

            if (!result.ContainsKey(poNumber))
                result[poNumber] = pipeSize;
        }

        _logger.LogInformation("Loaded pipe size for {Count} PO(s) from {Path}", result.Count, path);
        return result;
    }
}
