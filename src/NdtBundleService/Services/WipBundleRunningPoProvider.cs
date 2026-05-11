using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Resolves running PO per mill from WIP bundle filenames in the TM bundle folders.
/// Reads only; never modifies/moves files.
///
/// Efficiency:
/// - Maintains an in-memory cache per mill.
/// - Watches both Bundle and Bundle Accepted folders for changes.
/// - Falls back to a quick rescan when the cache is empty/stale or watchers miss events.
/// </summary>
public sealed class WipBundleRunningPoProvider : IWipBundleRunningPoProvider, IDisposable
{
    private static readonly Regex ReFull = new(
        @"^WIP_(\d{2})_(\d+)_(\d+)_(\d{6})_(\d{6})(\.csv)?$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex ReShort = new(
        @"^WIP_(\d{2})_(\d+)_(\d{6})_(\d{6})\.csv$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private readonly IOptions<NdtBundleOptions> _options;
    private readonly ILogger<WipBundleRunningPoProvider> _logger;

    private readonly object _lock = new();
    private readonly Dictionary<int, (DateTime StampUtc, string Po)> _latestPoByMill = new();
    private DateTime _lastRescanUtc = DateTime.MinValue;

    private FileSystemWatcher? _watchBundle;
    private FileSystemWatcher? _watchAccepted;

    public WipBundleRunningPoProvider(IOptions<NdtBundleOptions> options, ILogger<WipBundleRunningPoProvider> logger)
    {
        _options = options;
        _logger = logger;

        TryStartWatchers();
    }

    public Task<string?> TryGetRunningPoForMillAsync(int millNo, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (millNo is < 1 or > 4)
            return Task.FromResult<string?>(null);

        var nowUtc = DateTime.UtcNow;
        lock (_lock)
        {
            if (_latestPoByMill.TryGetValue(millNo, out var v))
                return Task.FromResult<string?>(v.Po);
        }

        // If cache is empty for this mill, do a rescan (limited cadence).
        MaybeRescan(nowUtc, cancellationToken);
        lock (_lock)
        {
            return Task.FromResult<string?>(_latestPoByMill.TryGetValue(millNo, out var v) ? v.Po : null);
        }
    }

    public void Dispose()
    {
        try { _watchBundle?.Dispose(); } catch { /* ignore */ }
        try { _watchAccepted?.Dispose(); } catch { /* ignore */ }
    }

    private void TryStartWatchers()
    {
        var live = _options.Value.MillSlitLive;
        var bundle = (live.WipBundleFolder ?? string.Empty).Trim();
        var accepted = (live.WipBundleAcceptedFolder ?? string.Empty).Trim();

        // Best-effort initial scan.
        try
        {
            RescanAllUnsafe(DateTime.UtcNow);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Initial WIP bundle scan failed.");
        }

        _watchBundle = TryCreateWatcher(bundle);
        _watchAccepted = TryCreateWatcher(accepted);
    }

    private FileSystemWatcher? TryCreateWatcher(string folder)
    {
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return null;

        try
        {
            var w = new FileSystemWatcher(folder)
            {
                IncludeSubdirectories = false,
                Filter = "*",
                NotifyFilter = NotifyFilters.FileName | NotifyFilters.CreationTime | NotifyFilters.LastWrite
            };
            w.Created += OnFsEvent;
            w.Renamed += OnFsEvent;
            w.Changed += OnFsEvent;
            w.EnableRaisingEvents = true;
            return w;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to start FileSystemWatcher for {Folder}. Falling back to periodic rescans.", folder);
            return null;
        }
    }

    private void OnFsEvent(object sender, FileSystemEventArgs e)
    {
        // Never throw on watcher threads.
        try
        {
            var name = Path.GetFileName(e.FullPath);
            if (!name.StartsWith("WIP_", StringComparison.OrdinalIgnoreCase))
                return;

            var meta = ParseName(name);
            if (meta is null)
                return;

            DateTime stampUtc;
            try
            {
                // Prefer actual file timestamp; if it isn't accessible yet, use now.
                stampUtc = File.GetLastWriteTimeUtc(e.FullPath);
                if (stampUtc == DateTime.MinValue)
                    stampUtc = DateTime.UtcNow;
            }
            catch
            {
                stampUtc = DateTime.UtcNow;
            }

            var millNo = int.TryParse(meta.MillDigits, NumberStyles.Integer, CultureInfo.InvariantCulture, out var m) ? m : 0;
            if (millNo is < 1 or > 4)
                return;

            var po = InputSlitCsvParsing.NormalizePo(meta.PoNumber);
            if (string.IsNullOrWhiteSpace(po))
                return;

            lock (_lock)
            {
                if (_latestPoByMill.TryGetValue(millNo, out var existing) && existing.StampUtc >= stampUtc)
                    return;
                _latestPoByMill[millNo] = (stampUtc, po);
            }
        }
        catch
        {
            /* ignore */
        }
    }

    private void MaybeRescan(DateTime nowUtc, CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            // Rescan at most once every 5 seconds, even if many requests arrive.
            if ((nowUtc - _lastRescanUtc) < TimeSpan.FromSeconds(5))
                return;
            _lastRescanUtc = nowUtc;
        }

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            RescanAllUnsafe(nowUtc);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "WIP bundle rescan failed.");
        }
    }

    private void RescanAllUnsafe(DateTime nowUtc)
    {
        var live = _options.Value.MillSlitLive;
        var bundle = (live.WipBundleFolder ?? string.Empty).Trim();
        var accepted = (live.WipBundleAcceptedFolder ?? string.Empty).Trim();

        var candidates = new List<(int MillNo, DateTime StampUtc, string Po)>();
        ScanFolder(bundle, candidates);
        ScanFolder(accepted, candidates);

        if (candidates.Count == 0)
            return;

        lock (_lock)
        {
            foreach (var c in candidates)
            {
                if (!_latestPoByMill.TryGetValue(c.MillNo, out var existing) || existing.StampUtc < c.StampUtc)
                    _latestPoByMill[c.MillNo] = (c.StampUtc, c.Po);
            }
        }
    }

    private void ScanFolder(string folder, List<(int MillNo, DateTime StampUtc, string Po)> outCandidates)
    {
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return;

        foreach (var path in Directory.EnumerateFiles(folder))
        {
            var name = Path.GetFileName(path);
            if (!name.StartsWith("WIP_", StringComparison.OrdinalIgnoreCase))
                continue;
            var meta = ParseName(name);
            if (meta is null)
                continue;

            if (!int.TryParse(meta.MillDigits, NumberStyles.Integer, CultureInfo.InvariantCulture, out var millNo) || millNo is < 1 or > 4)
                continue;

            var po = InputSlitCsvParsing.NormalizePo(meta.PoNumber);
            if (string.IsNullOrWhiteSpace(po))
                continue;

            DateTime stampUtc;
            try
            {
                stampUtc = File.GetLastWriteTimeUtc(path);
                if (stampUtc == DateTime.MinValue)
                    stampUtc = File.GetCreationTimeUtc(path);
            }
            catch
            {
                stampUtc = DateTime.UtcNow;
            }

            outCandidates.Add((millNo, stampUtc, po));
        }
    }

    private sealed record WipMeta(string MillDigits, string PoNumber, string SortKey);

    private static WipMeta? ParseName(string fileName)
    {
        var m = ReFull.Match(fileName);
        if (m.Success)
            return new WipMeta(m.Groups[1].Value, m.Groups[2].Value, m.Groups[4].Value + "_" + m.Groups[5].Value);

        m = ReShort.Match(fileName);
        if (m.Success)
            return new WipMeta(m.Groups[1].Value, m.Groups[2].Value, m.Groups[3].Value + "_" + m.Groups[4].Value);

        return null;
    }
}
