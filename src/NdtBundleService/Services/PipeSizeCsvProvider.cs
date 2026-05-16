using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Loads PO Number -> Pipe Size from a CSV file. When PoPlanFolder is set, scans eligible PO plan CSVs in that folder (newer files override older rows for the same PO). Otherwise uses PipeSizeCsvPath.
/// Expected columns: PO Number (or PO_No), Pipe Size.
/// </summary>
public sealed class PipeSizeCsvProvider : IPipeSizeProvider
{
    private readonly NdtBundleOptions _options;
    private readonly ILogger<PipeSizeCsvProvider> _logger;
    private readonly object _cacheLock = new();
    private IReadOnlyDictionary<string, string>? _cached;
    private string? _cacheSignature;

    public PipeSizeCsvProvider(IOptions<NdtBundleOptions> options, ILogger<PipeSizeCsvProvider> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<IReadOnlyDictionary<string, string>> GetPipeSizeByPoAsync(CancellationToken cancellationToken)
    {
        var paths = ResolvePipeSizeSourcePaths();
        if (paths.Count == 0)
        {
            _logger.LogWarning("Pipe size CSV source not found (PoPlanFolder/PipeSizeCsvPath). Size-based bundle logic will use Default formation only.");
            return EmptyPipeSizeMap();
        }

        var signature = BuildCacheSignature(paths);
        lock (_cacheLock)
        {
            if (_cached is not null && string.Equals(_cacheSignature, signature, StringComparison.Ordinal))
                return _cached;
        }

        var result = await LoadPipeSizeMapAsync(paths, cancellationToken).ConfigureAwait(false);

        lock (_cacheLock)
        {
            _cached = result;
            _cacheSignature = signature;
        }

        return result;
    }

    private List<string> ResolvePipeSizeSourcePaths()
    {
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

        return paths;
    }

    private static string BuildCacheSignature(IReadOnlyList<string> paths)
    {
        if (paths.Count == 0)
            return string.Empty;

        return string.Join(
            ';',
            paths.Select(path => $"{path}|{File.GetLastWriteTimeUtc(path).Ticks}"));
    }

    private async Task<IReadOnlyDictionary<string, string>> LoadPipeSizeMapAsync(
        IReadOnlyList<string> paths,
        CancellationToken cancellationToken)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var path in paths)
        {
            cancellationToken.ThrowIfCancellationRequested();

            await using var stream = File.OpenRead(path);
            using var reader = new StreamReader(stream);

            string? headerLine = await reader.ReadLineAsync().ConfigureAwait(false);
            if (headerLine is null)
                continue;

            var headers = InputSlitCsvParsing.SplitCsvFields(InputSlitCsvParsing.StripBom(headerLine));

            var poIndex = InputSlitCsvParsing.HeaderIndex(headers, "PO Number", "PO_No", "PO No");
            var sizeIndex = InputSlitCsvParsing.HeaderIndex(headers, "Pipe Size");
            if (poIndex < 0 || sizeIndex < 0)
            {
                _logger.LogWarning("Pipe size CSV {Path} does not contain expected columns 'PO Number'/'PO_No' and 'Pipe Size'.", path);
                continue;
            }

            while (await reader.ReadLineAsync().ConfigureAwait(false) is { } line)
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

                result[poNumber] = pipeSize;
            }
        }

        _logger.LogInformation("Loaded pipe size for {Count} PO(s) from {Files} file(s).", result.Count, paths.Count);
        return result;
    }

    private static IReadOnlyDictionary<string, string> EmptyPipeSizeMap() =>
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
}
