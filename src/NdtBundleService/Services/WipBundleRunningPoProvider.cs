using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Resolves running PO per mill from WIP bundle filenames in the TM Bundle folders.
/// After PO end (<see cref="NotifyPoEndForMill"/>), returns null until a newer WIP file arrives for that mill.
/// </summary>
public sealed class WipBundleRunningPoProvider : IWipBundleRunningPoProvider, IDisposable
{
    private sealed class MillState
    {
        public string? RunningPo;
        public DateTime RunningWipStampUtc;
        public bool WaitingForNewWip;
        public string? EndedPo;
        public DateTime PoEndUtc;
        public DateTime BaselineWipStampUtc;
    }

    private readonly IOptions<NdtBundleOptions> _options;
    private readonly ILogger<WipBundleRunningPoProvider> _logger;

    private readonly object _lock = new();
    private readonly MillState[] _mills = { new(), new(), new(), new() };
    private DateTime _lastRescanUtc = DateTime.MinValue;

    private FileSystemWatcher? _watchBundle;
    private FileSystemWatcher? _watchAccepted;
    private FileSystemWatcher? _watchFgBundle;
    private FileSystemWatcher? _watchFgAccepted;

    public WipBundleRunningPoProvider(IOptions<NdtBundleOptions> options, ILogger<WipBundleRunningPoProvider> logger)
    {
        _options = options;
        _logger = logger;

        TryStartWatchers();
    }

    public Task<string?> TryGetRunningPoForMillAsync(int millNo, CancellationToken cancellationToken)
    {
        if (millNo is < 1 or > 4)
            return Task.FromResult<string?>(null);

        MaybeRescan(DateTime.UtcNow);

        lock (_lock)
        {
            var st = _mills[millNo - 1];
            if (st.WaitingForNewWip)
                return Task.FromResult<string?>(null);

            return Task.FromResult(st.RunningPo);
        }
    }

    public void NotifyPoEndForMill(int millNo, string endedPo)
    {
        if (millNo is < 1 or > 4)
            return;

        if (!_options.Value.WaitForWipBundleAfterPoEnd)
        {
            _logger.LogDebug(
                "Mill {Mill}: WaitForWipBundleAfterPoEnd is disabled; not waiting for new WIP bundle after PO end of {Po}.",
                millNo,
                endedPo);
            return;
        }

        var normalizedEnded = InputSlitCsvParsing.NormalizePo(endedPo);
        var baseline = FindLatestWipStampUtcForMill(millNo);

        lock (_lock)
        {
            var st = _mills[millNo - 1];
            st.WaitingForNewWip = true;
            st.EndedPo = normalizedEnded;
            st.PoEndUtc = DateTime.UtcNow;
            st.BaselineWipStampUtc = baseline;
            st.RunningPo = null;
        }

        _logger.LogInformation(
            "Mill {Mill}: PO end for {EndedPo}; waiting for new WIP bundle file in TM Bundle folder (baseline WIP stamp {Baseline:o}).",
            millNo,
            normalizedEnded,
            baseline);

        TryAcceptNewWipAfterPoEnd(millNo);
    }

    public bool IsWaitingForNewWipAfterPoEnd(int millNo)
    {
        if (millNo is < 1 or > 4)
            return false;

        lock (_lock)
        {
            return _mills[millNo - 1].WaitingForNewWip;
        }
    }

    public void Dispose()
    {
        try { _watchBundle?.Dispose(); } catch { /* ignore */ }
        try { _watchAccepted?.Dispose(); } catch { /* ignore */ }
        try { _watchFgBundle?.Dispose(); } catch { /* ignore */ }
        try { _watchFgAccepted?.Dispose(); } catch { /* ignore */ }
    }

    private void TryStartWatchers()
    {
        try
        {
            SeedFromLatestWipFilesUnsafe();
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Initial WIP bundle scan failed.");
        }

        foreach (var folder in ResolveBundleFolders())
        {
            var watcher = TryCreateWatcher(folder);
            if (watcher is null)
                continue;

            if (_watchBundle is null)
                _watchBundle = watcher;
            else if (_watchAccepted is null)
                _watchAccepted = watcher;
            else if (_watchFgBundle is null)
                _watchFgBundle = watcher;
            else
                _watchFgAccepted = watcher;
        }
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
        try
        {
            var name = Path.GetFileName(e.FullPath);
            if (!name.StartsWith("WIP_", StringComparison.OrdinalIgnoreCase))
                return;

            var meta = WipBundleFileName.TryParse(name);
            if (meta is null)
                return;

            DateTime stampUtc;
            try
            {
                stampUtc = File.GetLastWriteTimeUtc(e.FullPath);
                if (stampUtc == DateTime.MinValue)
                    stampUtc = DateTime.UtcNow;
            }
            catch
            {
                stampUtc = DateTime.UtcNow;
            }

            lock (_lock)
            {
                var st = _mills[meta.MillNo - 1];
                if (st.WaitingForNewWip)
                {
                    TryAcceptCandidateUnsafe(meta.MillNo, meta.PoNumber, stampUtc, name);
                    return;
                }

                if (st.RunningWipStampUtc >= stampUtc)
                    return;

                st.RunningPo = meta.PoNumber;
                st.RunningWipStampUtc = stampUtc;
            }

            _logger.LogInformation(
                "Mill {Mill}: running PO updated from WIP bundle file {File} → PO {Po}.",
                meta.MillNo,
                name,
                meta.PoNumber);
        }
        catch
        {
            /* ignore */
        }
    }

    private void MaybeRescan(DateTime nowUtc)
    {
        lock (_lock)
        {
            if ((nowUtc - _lastRescanUtc) < TimeSpan.FromSeconds(5))
                return;
            _lastRescanUtc = nowUtc;
        }

        try
        {
            RescanAllUnsafe(nowUtc);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "WIP bundle rescan failed.");
        }
    }

    private void RescanAllUnsafe(DateTime nowUtc)
    {
        var candidates = ScanAllWipCandidates();
        if (candidates.Count == 0)
            return;

        lock (_lock)
        {
            for (var millNo = 1; millNo <= 4; millNo++)
            {
                var st = _mills[millNo - 1];
                if (st.WaitingForNewWip)
                {
                    TryAcceptNewWipAfterPoEndUnsafe(millNo, candidates);
                    continue;
                }

                var best = candidates
                    .Where(c => c.MillNo == millNo)
                    .OrderByDescending(c => c.StampUtc)
                    .ThenByDescending(c => c.SortKey, StringComparer.OrdinalIgnoreCase)
                    .FirstOrDefault();

                if (best.MillNo == 0)
                    continue;

                if (st.RunningWipStampUtc >= best.StampUtc)
                    continue;

                st.RunningPo = best.PoNumber;
                st.RunningWipStampUtc = best.StampUtc;
            }
        }
    }

    private void SeedFromLatestWipFilesUnsafe()
    {
        var candidates = ScanAllWipCandidates();
        lock (_lock)
        {
            for (var millNo = 1; millNo <= 4; millNo++)
            {
                var best = candidates
                    .Where(c => c.MillNo == millNo)
                    .OrderByDescending(c => c.StampUtc)
                    .ThenByDescending(c => c.SortKey, StringComparer.OrdinalIgnoreCase)
                    .FirstOrDefault();

                if (best.MillNo == 0)
                    continue;

                var st = _mills[millNo - 1];
                st.RunningPo = best.PoNumber;
                st.RunningWipStampUtc = best.StampUtc;
            }
        }
    }

    private void TryAcceptNewWipAfterPoEnd(int millNo)
    {
        lock (_lock)
        {
            TryAcceptNewWipAfterPoEndUnsafe(millNo, ScanAllWipCandidates());
        }
    }

    private void TryAcceptNewWipAfterPoEndUnsafe(int millNo, List<WipCandidate> candidates)
    {
        var st = _mills[millNo - 1];
        if (!st.WaitingForNewWip)
            return;

        foreach (var c in candidates
                     .Where(x => x.MillNo == millNo)
                     .OrderByDescending(x => x.StampUtc)
                     .ThenByDescending(x => x.SortKey, StringComparer.OrdinalIgnoreCase))
        {
            if (TryAcceptCandidateUnsafe(millNo, c.PoNumber, c.StampUtc, c.FileName))
                return;
        }
    }

    private bool TryAcceptCandidateUnsafe(int millNo, string poNumber, DateTime stampUtc, string fileName)
    {
        var st = _mills[millNo - 1];
        if (!st.WaitingForNewWip)
            return false;

        var normalized = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(normalized))
            return false;

        if (stampUtc <= st.BaselineWipStampUtc)
            return false;

        if (!string.IsNullOrWhiteSpace(st.EndedPo) && InputSlitCsvParsing.PoEquals(normalized, st.EndedPo))
            return false;

        st.WaitingForNewWip = false;
        st.RunningPo = normalized;
        st.RunningWipStampUtc = stampUtc;
        st.EndedPo = null;

        _logger.LogInformation(
            "Mill {Mill}: new running PO {Po} accepted from WIP bundle file {File} after PO end.",
            millNo,
            normalized,
            fileName);

        return true;
    }

    private DateTime FindLatestWipStampUtcForMill(int millNo)
    {
        return ScanAllWipCandidates()
            .Where(c => c.MillNo == millNo)
            .Select(c => c.StampUtc)
            .DefaultIfEmpty(DateTime.MinValue)
            .Max();
    }

    private List<WipCandidate> ScanAllWipCandidates()
    {
        var candidates = new List<WipCandidate>();
        foreach (var folder in ResolveBundleFolders())
        {
            ScanFolder(folder, candidates);
        }

        return candidates;
    }

    private void ScanFolder(string folder, List<WipCandidate> outCandidates)
    {
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return;

        foreach (var path in Directory.EnumerateFiles(folder))
        {
            var name = Path.GetFileName(path);
            if (!name.StartsWith("WIP_", StringComparison.OrdinalIgnoreCase))
                continue;

            var meta = WipBundleFileName.TryParse(name);
            if (meta is null)
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

            outCandidates.Add(new WipCandidate(meta.MillNo, stampUtc, meta.PoNumber, meta.SortKey, name));
        }
    }

    private IEnumerable<string> ResolveBundleFolders()
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var opt = _options.Value;
        var live = opt.MillSlitLive ?? new MillSlitLiveOptions();

        foreach (var folder in new[]
                 {
                     live.WipBundleFolder,
                     live.WipBundleAcceptedFolder,
                     opt.FgBundleFolder,
                     opt.FgBundleAcceptedFolder
                 })
        {
            var trimmed = (folder ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(trimmed) || !seen.Add(trimmed))
                continue;
            yield return trimmed;
        }
    }

    private readonly record struct WipCandidate(int MillNo, DateTime StampUtc, string PoNumber, string SortKey, string FileName);
}
