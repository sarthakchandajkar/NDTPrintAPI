using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Loads PO Number -> Pipe Size from <c>dbo.PO_Plan_WIP</c> when SQL is preferred, otherwise from PO plan CSV folders.
/// </summary>
public sealed class PipeSizeCsvProvider : IPipeSizeProvider
{
    private readonly NdtBundleOptions _options;
    private readonly IPoPlanWipRepository _poPlanWipRepository;
    private readonly ILogger<PipeSizeCsvProvider> _logger;
    private readonly object _cacheLock = new();
    private readonly SemaphoreSlim _buildLock = new(1, 1);
    private IReadOnlyDictionary<string, string>? _cached;
    private string? _cacheSignature;

    public PipeSizeCsvProvider(
        IOptions<NdtBundleOptions> options,
        IPoPlanWipRepository poPlanWipRepository,
        ILogger<PipeSizeCsvProvider> logger)
    {
        _options = options.Value;
        _poPlanWipRepository = poPlanWipRepository;
        _logger = logger;
    }

    public IReadOnlyDictionary<string, string>? TryGetCachedPipeSizes()
    {
        lock (_cacheLock)
        {
            return _cached;
        }
    }

    public async Task<string?> TryGetPipeSizeForPoAsync(string poNumber, CancellationToken cancellationToken)
    {
        var normalized = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(normalized))
            return null;

        lock (_cacheLock)
        {
            if (_cached is not null && _cached.TryGetValue(normalized, out var cachedSize) && !string.IsNullOrWhiteSpace(cachedSize))
                return cachedSize.Trim();
        }

        var map = await GetPipeSizeByPoAsync(cancellationToken).ConfigureAwait(false);
        return map.TryGetValue(normalized, out var size) && !string.IsNullOrWhiteSpace(size)
            ? size.Trim()
            : null;
    }

    public async Task<IReadOnlyDictionary<string, string>> GetPipeSizeByPoAsync(CancellationToken cancellationToken)
    {
        if (PoPlanWipSql.IsEnabled(_options))
        {
            var sqlSignature = await _poPlanWipRepository.TryGetDataSignatureAsync(cancellationToken).ConfigureAwait(false);
            if (!string.IsNullOrWhiteSpace(sqlSignature))
                return await GetPipeSizeFromSqlAsync(sqlSignature, cancellationToken).ConfigureAwait(false);
        }

        return await GetPipeSizeFromCsvAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<IReadOnlyDictionary<string, string>> GetPipeSizeFromSqlAsync(
        string signature,
        CancellationToken cancellationToken)
    {
        lock (_cacheLock)
        {
            if (_cached is not null && string.Equals(_cacheSignature, signature, StringComparison.Ordinal))
                return _cached;
        }

        await _buildLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            lock (_cacheLock)
            {
                if (_cached is not null && string.Equals(_cacheSignature, signature, StringComparison.Ordinal))
                    return _cached;
            }

            var result = new Dictionary<string, string>(
                await _poPlanWipRepository.GetLatestPipeSizeByPoAsync(cancellationToken).ConfigureAwait(false),
                StringComparer.OrdinalIgnoreCase);

            if (_options.MergeWipBundlePipeSizesWhenUsingSqlPoPlan)
                await MergeWipBundlePipeSizesAsync(result, ResolveWipBundleFolders(), CancellationToken.None).ConfigureAwait(false);

            if (result.Count == 0)
            {
                _logger.LogWarning(
                    "No pipe sizes found in dbo.PO_Plan_WIP; falling back to PO plan CSV folders.");
                return await GetPipeSizeFromCsvAsync(cancellationToken).ConfigureAwait(false);
            }

            _logger.LogInformation(
                "Loaded pipe size for {Count} PO(s) from JazeeraMES_Prod.dbo.PO_Plan_WIP.",
                result.Count);

            lock (_cacheLock)
            {
                _cached = result;
                _cacheSignature = signature;
            }

            return result;
        }
        finally
        {
            _buildLock.Release();
        }
    }

    private async Task<IReadOnlyDictionary<string, string>> GetPipeSizeFromCsvAsync(CancellationToken cancellationToken)
    {
        var paths = ResolvePipeSizeSourcePaths();
        var wipFolders = ResolveWipBundleFolders();
        var signature = "csv:" + BuildCacheSignature(paths, wipFolders);
        lock (_cacheLock)
        {
            if (_cached is not null && string.Equals(_cacheSignature, signature, StringComparison.Ordinal))
                return _cached;
        }

        await _buildLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            lock (_cacheLock)
            {
                if (_cached is not null && string.Equals(_cacheSignature, signature, StringComparison.Ordinal))
                    return _cached;
            }

            var result = paths.Count > 0
                ? new Dictionary<string, string>(await LoadPipeSizeMapAsync(paths, CancellationToken.None).ConfigureAwait(false), StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            await MergeWipBundlePipeSizesAsync(result, wipFolders, CancellationToken.None).ConfigureAwait(false);

            if (result.Count == 0)
            {
                _logger.LogWarning(
                    "Pipe size not found in PoPlanFolder, PipeSizeCsvPath, or TM WIP bundle CSVs. Size-based bundle logic will use Default formation only.");
            }
            else if (paths.Count > 0)
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
        finally
        {
            _buildLock.Release();
        }
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
                    return $"{folder}|{LatestWipFileWriteTicks(folder)}";
                }
                catch
                {
                    return folder;
                }
            }));

        return planPart + "||" + wipPart;
    }

    private static long LatestWipFileWriteTicks(string folder)
    {
        return Directory.EnumerateFiles(folder)
            .Where(name => Path.GetFileName(name).StartsWith("WIP_", StringComparison.OrdinalIgnoreCase))
            .Select(path => File.GetLastWriteTimeUtc(path).Ticks)
            .DefaultIfEmpty(0)
            .Max();
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
