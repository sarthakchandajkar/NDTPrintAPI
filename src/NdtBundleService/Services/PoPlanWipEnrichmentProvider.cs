using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Cached PO plan WIP rows for dashboard enrichment and slit hollow-pipe checks.
/// Reads from <c>dbo.PO_Plan_WIP</c> when SQL is preferred, otherwise merges PO plan CSV folders.
/// </summary>
public sealed class PoPlanWipEnrichmentProvider : IPoPlanWipEnrichmentProvider
{
    private readonly NdtBundleOptions _options;
    private readonly IPoPlanWipRepository _poPlanWipRepository;
    private readonly ICurrentPoPlanService? _currentPoPlanService;
    private readonly ILogger<PoPlanWipEnrichmentProvider> _logger;
    private readonly object _cacheLock = new();
    private readonly SemaphoreSlim _buildLock = new(1, 1);
    private PoPlanWipEnrichmentSnapshot? _cached;
    private string? _cacheSignature;

    public PoPlanWipEnrichmentProvider(
        IOptions<NdtBundleOptions> options,
        IPoPlanWipRepository poPlanWipRepository,
        ILogger<PoPlanWipEnrichmentProvider> logger,
        ICurrentPoPlanService? currentPoPlanService = null)
    {
        _options = options.Value;
        _poPlanWipRepository = poPlanWipRepository;
        _logger = logger;
        _currentPoPlanService = currentPoPlanService;
    }

    public PoPlanWipEnrichmentSnapshot? TryGetCachedEnrichment()
    {
        lock (_cacheLock)
        {
            return _cached;
        }
    }

    public async Task<PoPlanWipEnrichmentSnapshot> GetEnrichmentAsync(CancellationToken cancellationToken)
    {
        var signature = await BuildSignatureAsync(cancellationToken).ConfigureAwait(false);
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

            var snapshot = await BuildSnapshotAsync(signature, CancellationToken.None).ConfigureAwait(false);
            lock (_cacheLock)
            {
                _cached = snapshot;
                _cacheSignature = signature;
            }

            return snapshot;
        }
        finally
        {
            _buildLock.Release();
        }
    }

    private async Task<string> BuildSignatureAsync(CancellationToken cancellationToken)
    {
        if (PoPlanWipSql.IsEnabled(_options))
        {
            var sqlSignature = await _poPlanWipRepository.TryGetDataSignatureAsync(cancellationToken).ConfigureAwait(false);
            if (!string.IsNullOrWhiteSpace(sqlSignature))
                return sqlSignature;
        }

        var planFolder = (_options.PoPlanFolder ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(planFolder) && Directory.Exists(planFolder))
        {
            var files = PoPlanWipCsvMerger.ResolveEligiblePoPlanFiles(_options);
            return "folder:" + PoPlanWipCsvMerger.BuildPoPlanFilesSignature(files);
        }

        var singlePath = await ResolveSinglePlanPathAsync(cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(singlePath))
            return "none";

        return "file||" + singlePath + "||" + File.GetLastWriteTimeUtc(singlePath).Ticks;
    }

    private async Task<string?> ResolveSinglePlanPathAsync(CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(_options.PoPlanFolder) && _currentPoPlanService != null)
        {
            var path = await _currentPoPlanService.GetCurrentPoPlanPathAsync(cancellationToken).ConfigureAwait(false);
            if (!string.IsNullOrWhiteSpace(path) && File.Exists(path))
                return path;
        }

        var configured = _options.PoPlanCsvPath;
        if (!string.IsNullOrWhiteSpace(configured) && File.Exists(configured))
            return configured;

        return null;
    }

    private async Task<PoPlanWipEnrichmentSnapshot> BuildSnapshotAsync(string signature, CancellationToken cancellationToken)
    {
        if (signature.StartsWith("sql:", StringComparison.Ordinal))
        {
            var sqlSnapshot = await _poPlanWipRepository.GetLatestEnrichmentAsync(cancellationToken).ConfigureAwait(false);
            return new PoPlanWipEnrichmentSnapshot(
                sqlSnapshot.ByMill,
                sqlSnapshot.ByPo,
                sqlSnapshot.SourceDescription);
        }

        var merge = new PoPlanWipCsvMerger.MergeResult();
        string sourceDescription;

        if (signature.StartsWith("folder:", StringComparison.Ordinal))
        {
            var files = PoPlanWipCsvMerger.ResolveEligiblePoPlanFiles(_options);
            if (files.Count == 0)
            {
                return new PoPlanWipEnrichmentSnapshot(
                    merge.ByMill,
                    merge.ByPo,
                    $"{_options.PoPlanFolder} (no eligible WIP CSV files)");
            }

            foreach (var file in files)
            {
                try
                {
                    await PoPlanWipCsvMerger.MergeFileAsync(file, merge, _logger, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogWarning(ex, "Skipping PO plan WIP file after read/parse error: {File}", file);
                }
            }

            sourceDescription =
                $"{_options.PoPlanFolder} ({files.Count} WIP CSV file(s); PO per mill from slits, else TM Bundle / Bundle Accepted WIP filenames)";
            _logger.LogInformation(
                "Cached PO plan WIP enrichment for {PoCount} PO(s) from {FileCount} file(s).",
                merge.ByPo.Count,
                files.Count);
        }
        else if (signature.StartsWith("file||", StringComparison.Ordinal))
        {
            var remainder = signature["file||".Length..];
            var split = remainder.LastIndexOf("||", StringComparison.Ordinal);
            var path = split > 0 ? remainder[..split] : remainder;

            if (!await PoPlanWipCsvMerger.MergeFileAsync(path, merge, _logger, cancellationToken).ConfigureAwait(false))
            {
                return new PoPlanWipEnrichmentSnapshot(
                    merge.ByMill,
                    merge.ByPo,
                    $"{path} (missing required PO/mill columns)");
            }

            sourceDescription = path;
        }
        else
        {
            sourceDescription = "PO plan folder not configured or not reachable";
        }

        return new PoPlanWipEnrichmentSnapshot(merge.ByMill, merge.ByPo, sourceDescription);
    }
}

public sealed class PoPlanWipEnrichmentSnapshot
{
    public PoPlanWipEnrichmentSnapshot(
        IReadOnlyDictionary<int, PoPlanWipRow> byMill,
        IReadOnlyDictionary<string, PoPlanWipRow> byPo,
        string sourceDescription)
    {
        ByMill = byMill;
        ByPo = byPo;
        SourceDescription = sourceDescription;
    }

    public IReadOnlyDictionary<int, PoPlanWipRow> ByMill { get; }
    public IReadOnlyDictionary<string, PoPlanWipRow> ByPo { get; }
    public string SourceDescription { get; }
}
