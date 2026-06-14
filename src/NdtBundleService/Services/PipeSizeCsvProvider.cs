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
        var wipFolders = ResolveWipBundleFolders();
        var signature = BuildCacheSignature(paths, wipFolders);
        lock (_cacheLock)
        {
            if (_cached is not null && string.Equals(_cacheSignature, signature, StringComparison.Ordinal))
                return _cached;
        }

        var result = paths.Count > 0
            ? new Dictionary<string, string>(await LoadPipeSizeMapAsync(paths, cancellationToken).ConfigureAwait(false), StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        await MergeWipBundlePipeSizesAsync(result, wipFolders, cancellationToken).ConfigureAwait(false);

        if (result.Count == 0)
        {
            _logger.LogWarning(
                "Pipe size not found in PoPlanFolder, PipeSizeCsvPath, or TM WIP bundle CSVs. Size-based bundle logic will use Default formation only.");
        }
        else
        {
            _logger.LogInformation("Loaded pipe size for {Count} PO(s).", result.Count);
        }

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
            paths.AddRange(
                Directory.EnumerateFiles(planFolder, "*.csv")
                    .Where(p => SourceFileEligibility.IncludePoPlanFolderFileUtc(File.GetLastWriteTimeUtc(p), _options))
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

    private static string BuildCacheSignature(IReadOnlyList<string> paths, IReadOnlyList<string> wipFolders)
    {
        var planPart = paths.Count == 0
            ? string.Empty
            : string.Join(';', paths.Select(path => $"{path}|{File.GetLastWriteTimeUtc(path).Ticks}"));

        var wipPart = wipFolders.Count == 0
            ? string.Empty
            : string.Join(';', wipFolders.Select(folder =>
            {
                try
                {
                    return $"{folder}|{Directory.GetLastWriteTimeUtc(folder).Ticks}";
                }
                catch
                {
                    return folder;
                }
            }));

        return planPart + "||" + wipPart;
    }

    private List<string> ResolveWipBundleFolders()
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var live = _options.MillSlitLive ?? new MillSlitLiveOptions();
        var folders = new List<string>();
        foreach (var folder in new[]
                 {
                     live.WipBundleFolder,
                     live.WipBundleAcceptedFolder,
                     _options.FgBundleFolder,
                     _options.FgBundleAcceptedFolder
                 })
        {
            var trimmed = (folder ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(trimmed) || !Directory.Exists(trimmed) || !seen.Add(trimmed))
                continue;
            folders.Add(trimmed);
        }

        return folders;
    }

    private static async Task MergeWipBundlePipeSizesAsync(
        Dictionary<string, string> result,
        IReadOnlyList<string> wipFolders,
        CancellationToken cancellationToken)
    {
        foreach (var folder in wipFolders)
        {
            IEnumerable<string> files;
            try
            {
                files = Directory.EnumerateFiles(folder)
                    .Where(name => Path.GetFileName(name).StartsWith("WIP_", StringComparison.OrdinalIgnoreCase))
                    .OrderBy(path => File.GetLastWriteTimeUtc(path));
            }
            catch
            {
                continue;
            }

            foreach (var path in files)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await TryMergePipeSizeFromWipCsvAsync(path, result, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static async Task TryMergePipeSizeFromWipCsvAsync(
        string filePath,
        Dictionary<string, string> result,
        CancellationToken cancellationToken)
    {
        var fileName = Path.GetFileName(filePath);
        var meta = WipBundleFileName.TryParse(fileName);

        await using var stream = File.OpenRead(filePath);
        using var reader = new StreamReader(stream);
        var headerLine = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
        if (headerLine is null)
            return;

        var headers = InputSlitCsvParsing.SplitDataFields(InputSlitCsvParsing.StripBom(headerLine));
        var poIndex = InputSlitCsvParsing.HeaderIndex(headers, "PO Number", "PO_No", "PO No");
        var sizeIndex = InputSlitCsvParsing.HeaderIndex(headers, "Pipe Size", "Size");
        if (sizeIndex < 0)
            return;

        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;

            var cols = InputSlitCsvParsing.SplitDataFields(line);
            if (cols.Length <= sizeIndex)
                continue;

            var poNumber = poIndex >= 0 && poIndex < cols.Length
                ? InputSlitCsvParsing.NormalizePo(cols[poIndex].Trim())
                : meta?.PoNumber ?? string.Empty;
            var pipeSize = cols[sizeIndex].Trim();
            if (string.IsNullOrWhiteSpace(poNumber) || string.IsNullOrWhiteSpace(pipeSize))
                continue;

            result[poNumber] = pipeSize;
        }
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

                var cols = InputSlitCsvParsing.SplitDataFields(line);
                if (cols.Length <= Math.Max(poIndex, sizeIndex))
                    continue;

                var poNumber = InputSlitCsvParsing.NormalizePo(cols[poIndex].Trim());
                var pipeSize = cols[sizeIndex].Trim();
                if (string.IsNullOrEmpty(poNumber))
                    continue;

                result[poNumber] = pipeSize;
            }
        }

        _logger.LogInformation("Loaded pipe size for {Count} PO(s) from {Files} PO plan file(s).", result.Count, paths.Count);
        return result;
    }
}
