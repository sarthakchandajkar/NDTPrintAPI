using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Loads PO Number -> Pipe Size from a CSV file. When PoPlanFolder is set, uses the current PO plan file (one file at a time; advance on PO End). Otherwise uses PipeSizeCsvPath.
/// Expected columns: PO Number (or PO_No), Pipe Size.
/// </summary>
public sealed class PipeSizeCsvProvider : IPipeSizeProvider
{
    private readonly NdtBundleOptions _options;
    private readonly ICurrentPoPlanService? _currentPoPlanService;
    private readonly ILogger<PipeSizeCsvProvider> _logger;

    public PipeSizeCsvProvider(IOptions<NdtBundleOptions> options, ICurrentPoPlanService? currentPoPlanService, ILogger<PipeSizeCsvProvider> logger)
    {
        _options = options.Value;
        _currentPoPlanService = currentPoPlanService;
        _logger = logger;
    }

    public async Task<IReadOnlyDictionary<string, string>> GetPipeSizeByPoAsync(CancellationToken cancellationToken)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var paths = new List<string>();
        var planFolder = (_options.PoPlanFolder ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(planFolder) && Directory.Exists(planFolder))
        {
            var minUtc = SourceFileEligibility.ParseMinUtc(_options);
            paths.AddRange(
                Directory.EnumerateFiles(planFolder, "*.csv")
                    .Where(p => SourceFileEligibility.IncludeFileUtc(File.GetLastWriteTimeUtc(p), minUtc))
                    .OrderBy(p => File.GetLastWriteTimeUtc(p)));
        }
        else
        {
            var path = _options.PipeSizeCsvPath;
            if (!string.IsNullOrWhiteSpace(path) && File.Exists(path))
                paths.Add(path);
        }

        if (paths.Count == 0)
        {
            _logger.LogWarning("Pipe size CSV source not found (PoPlanFolder/PipeSizeCsvPath). Size-based bundle logic will use Default formation only.");
            return result;
        }

        foreach (var path in paths)
        {
            await using var stream = File.OpenRead(path);
            using var reader = new StreamReader(stream);

            string? headerLine = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            if (headerLine is null)
                continue;

            var headers = InputSlitCsvParsing.SplitCsvFields(InputSlitCsvParsing.StripBom(headerLine));

            // Support both generic format and the WIP file format.
            // PO: "PO Number" or "PO_No"
            var poIndex = InputSlitCsvParsing.HeaderIndex(headers, "PO Number", "PO_No", "PO No");
            var sizeIndex = InputSlitCsvParsing.HeaderIndex(headers, "Pipe Size");
            if (poIndex < 0 || sizeIndex < 0)
            {
                _logger.LogWarning("Pipe size CSV {Path} does not contain expected columns 'PO Number'/'PO_No' and 'Pipe Size'.", path);
                continue;
            }

            while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                var cols = InputSlitCsvParsing.SplitCsvFields(line);
                if (cols.Length <= Math.Max(poIndex, sizeIndex))
                    continue;

                var poNumber = InputSlitCsvParsing.NormalizePo(cols[poIndex].Trim());
                var pipeSize = cols[sizeIndex].Trim();
                if (string.IsNullOrEmpty(poNumber))
                    continue;

                // Newer PO-plan files should override older rows for the same PO.
                result[poNumber] = pipeSize;
            }
        }

        _logger.LogInformation("Loaded pipe size for {Count} PO(s) from {Files} file(s).", result.Count, paths.Count);
        return result;
    }
}
