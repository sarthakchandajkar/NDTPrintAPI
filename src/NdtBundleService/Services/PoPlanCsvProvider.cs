using System.Globalization;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace NdtBundleService.Services;

/// <summary>
/// Loads PO plan data (including NDTPcsPerBundle) from a CSV file.
/// Expected columns at minimum: PO Number, NDTPcsPerBundle.
/// </summary>
public sealed class PoPlanCsvProvider : IPoPlanProvider
{
    private readonly NdtBundleOptions _options;
    private readonly ILogger<PoPlanCsvProvider> _logger;

    public PoPlanCsvProvider(IOptions<NdtBundleOptions> options, ILogger<PoPlanCsvProvider> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<IReadOnlyDictionary<string, PoPlanEntry>> GetPoPlansAsync(CancellationToken cancellationToken)
    {
        var path = _options.PoPlanCsvPath;
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
        {
            _logger.LogWarning("PO plan CSV path not configured or file not found: {Path}", path);
            return new Dictionary<string, PoPlanEntry>(StringComparer.OrdinalIgnoreCase);
        }

        var result = new Dictionary<string, PoPlanEntry>(StringComparer.OrdinalIgnoreCase);

        await using var stream = File.OpenRead(path);
        using var reader = new StreamReader(stream);

        string? headerLine = await reader.ReadLineAsync();
        if (headerLine is null)
            return result;

        var headers = headerLine.Split(',');

        // Support both generic PO-plan format and the specific WIP file format.
        // PO: "PO Number" or "PO_No"
        // Pieces per bundle: "NDTPcsPerBundle" or "Pieces Per Bundle"
        int poIndex = Array.FindIndex(headers, h =>
            h.Trim().Equals("PO Number", StringComparison.OrdinalIgnoreCase) ||
            h.Trim().Equals("PO_No", StringComparison.OrdinalIgnoreCase));

        int ndtIndex = Array.FindIndex(headers, h =>
            h.Trim().Equals("NDTPcsPerBundle", StringComparison.OrdinalIgnoreCase) ||
            h.Trim().Equals("Pieces Per Bundle", StringComparison.OrdinalIgnoreCase));

        if (poIndex < 0 || ndtIndex < 0)
        {
            _logger.LogWarning("PO plan CSV does not contain expected columns for PO number and pieces per bundle ('PO Number'/'PO_No' and 'NDTPcsPerBundle'/'Pieces Per Bundle').");
            return result;
        }

        while (!reader.EndOfStream)
        {
            var line = await reader.ReadLineAsync();
            if (string.IsNullOrWhiteSpace(line))
                continue;

            var cols = line.Split(',');
            if (cols.Length <= Math.Max(poIndex, ndtIndex))
                continue;

            var poNumber = cols[poIndex].Trim();
            if (string.IsNullOrEmpty(poNumber))
                continue;

            if (!int.TryParse(cols[ndtIndex].Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var ndtPerBundle))
                continue;

            result[poNumber] = new PoPlanEntry
            {
                PoNumber = poNumber,
                NdtPcsPerBundle = ndtPerBundle
            };
        }

        return result;
    }
}

