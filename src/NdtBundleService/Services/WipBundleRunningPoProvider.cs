using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.FileBasedPoChange;

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
    private readonly FileBasedPoChangeQueue? _fileBasedPoChangeQueue;
    private readonly ILogger<WipBundleRunningPoProvider> _logger;

    private readonly object _lock = new();
    private readonly MillState[] _mills = { new(), new(), new(), new() };
    private DateTime _lastRescanUtc = DateTime.MinValue;

    private FileSystemWatcher? _watchBundle;
    private FileSystemWatcher? _watchAccepted;
    private FileSystemWatcher? _watchFgBundle;
    private FileSystemWatcher? _watchFgAccepted;

    public WipBundleRunningPoProvider(
        IOptions<NdtBundleOptions> options,
        ILogger<WipBundleRunningPoProvider> logger,
        FileBasedPoChangeQueue? fileBasedPoChangeQueue = null)
    {
        _options = options;
        _logger = logger;
        _fileBasedPoChangeQueue = fileBasedPoChangeQueue;

        TryStartWatchers();
    }

    private bool IsFileBasedPoEndForMill(int millNo) =>
        millNo is >= 1 and <= 4 &&
        MillPoEndSourceResolver.ForMill(millNo, _options.Value) == MillPoEndSource.File &&
        _fileBasedPoChangeQueue is not null;

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

    public bool ResumeRunningWipForMill(int millNo)
    {
        if (millNo is < 1 or > 4)
            return false;

        lock (_lock)
        {
            var st = _mills[millNo - 1];
            if (!st.WaitingForNewWip)
                return false;

            var endedPo = st.EndedPo;
            st.WaitingForNewWip = false;
            st.EndedPo = null;
            st.PoEndUtc = default;

            var best = ScanAllWipCandidates()
                .Where(c => c.MillNo == millNo)
                .OrderByDescending(c => c.StampUtc)
                .ThenByDescending(c => c.SortKey, StringComparer.OrdinalIgnoreCase)
                .FirstOrDefault();

            if (best.MillNo != 0)
            {
                st.RunningPo = InputSlitCsvParsing.NormalizePo(best.PoNumber);
                st.RunningWipStampUtc = best.StampUtc;
            }

            _logger.LogWarning(
                "Mill {Mill}: resumed WIP tracking after false PO end (ended PO was {EndedPo}); running PO is now {RunningPo} from latest WIP file.",
                millNo,
                endedPo ?? "(unknown)",
                st.RunningPo ?? "(none from WIP folder)");

            return true;
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

                TryApplyRunningPoUpdateUnsafe(meta.MillNo, meta.PoNumber, stampUtc, name);
            }
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

                if (st.WaitingForNewWip)
                    continue;

                TryApplyRunningPoUpdateUnsafe(millNo, best.PoNumber, best.StampUtc, best.FileName);
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

    private void TryAcceptNewWipAfterPoEndUnsafe(int millNo, List<WipBundleFolderScanner.WipBundleFileCandidate> candidates)
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

    public bool TrySetRunningPoFromWipFile(int millNo, string newPo, DateTime wipStampUtc, string wipFileName)
    {
        if (millNo is < 1 or > 4)
            return false;

        lock (_lock)
        {
            var st = _mills[millNo - 1];
            if (st.WaitingForNewWip)
                return TryAcceptCandidateUnsafe(millNo, newPo, wipStampUtc, wipFileName);

            var normalized = InputSlitCsvParsing.NormalizePo(newPo);
            if (string.IsNullOrWhiteSpace(normalized))
                return false;

            if (st.RunningWipStampUtc >= wipStampUtc &&
                InputSlitCsvParsing.PoEquals(st.RunningPo, normalized))
                return false;

            st.RunningPo = normalized;
            st.RunningWipStampUtc = wipStampUtc;
            st.WaitingForNewWip = false;
            st.EndedPo = null;
            return true;
        }
    }

    private bool TryEnqueueFileBasedPoChange(
        int millNo,
        string endedPo,
        string newPo,
        DateTime wipStampUtc,
        string wipFileName)
    {
        if (!IsFileBasedPoEndForMill(millNo))
        {
            var source = MillPoEndSourceResolver.ForMill(millNo, _options.Value);
            if (source is MillPoEndSource.Plc or MillPoEndSource.TcpOpen)
            {
                _logger.LogWarning(
                    "Mill {Mill}: file-based PO change {OldPo} → {NewPo} from {File} rejected — PoEndSource={Source} (expected File).",
                    millNo,
                    endedPo,
                    newPo,
                    wipFileName,
                    MillPoEndSourceResolver.ToConfigValue(source));
            }

            return false;
        }

        var normalizedEnded = InputSlitCsvParsing.NormalizePo(endedPo);
        var normalizedNew = InputSlitCsvParsing.NormalizePo(newPo);
        if (string.IsNullOrWhiteSpace(normalizedNew))
            return false;

        if (!string.IsNullOrWhiteSpace(normalizedEnded) &&
            InputSlitCsvParsing.PoEquals(normalizedEnded, normalizedNew))
            return false;

        var enqueued = _fileBasedPoChangeQueue!.TryEnqueue(new FileBasedPoChangeRequest
        {
            MillNo = millNo,
            EndedPo = normalizedEnded,
            NewPo = normalizedNew,
            WipStampUtc = wipStampUtc,
            WipFileName = wipFileName
        });

        if (enqueued)
        {
            _logger.LogInformation(
                "Mill {Mill}: WIP bundle PO change {OldPo} → {NewPo} from {File}; file-based PO end queued.",
                millNo,
                string.IsNullOrWhiteSpace(normalizedEnded) ? "(none)" : normalizedEnded,
                normalizedNew,
                wipFileName);
        }

        return enqueued;
    }

    private bool TryApplyRunningPoUpdateUnsafe(
        int millNo,
        string newPo,
        DateTime wipStampUtc,
        string wipFileName)
    {
        var st = _mills[millNo - 1];
        var normalizedNew = InputSlitCsvParsing.NormalizePo(newPo);
        if (string.IsNullOrWhiteSpace(normalizedNew))
            return false;

        if (!string.IsNullOrWhiteSpace(st.RunningPo) &&
            InputSlitCsvParsing.PoEquals(st.RunningPo, normalizedNew) &&
            st.RunningWipStampUtc >= wipStampUtc)
            return false;

        if (IsFileBasedPoEndForMill(millNo) &&
            !string.IsNullOrWhiteSpace(st.RunningPo) &&
            !InputSlitCsvParsing.PoEquals(st.RunningPo, normalizedNew))
        {
            return TryEnqueueFileBasedPoChange(millNo, st.RunningPo, normalizedNew, wipStampUtc, wipFileName);
        }

        var poEndSource = MillPoEndSourceResolver.ForMill(millNo, _options.Value);
        if (!string.IsNullOrWhiteSpace(st.RunningPo) &&
            !InputSlitCsvParsing.PoEquals(st.RunningPo, normalizedNew) &&
            poEndSource is MillPoEndSource.Plc or MillPoEndSource.TcpOpen)
        {
            _logger.LogWarning(
                "Mill {Mill}: WIP bundle PO change {OldPo} → {NewPo} from {File} ignored for PO end — PoEndSource={Source} (expected File).",
                millNo,
                st.RunningPo,
                normalizedNew,
                wipFileName,
                MillPoEndSourceResolver.ToConfigValue(poEndSource));
        }

        if (st.RunningWipStampUtc >= wipStampUtc && InputSlitCsvParsing.PoEquals(st.RunningPo, normalizedNew))
            return false;

        st.RunningPo = normalizedNew;
        st.RunningWipStampUtc = wipStampUtc;
        _logger.LogInformation(
            "Mill {Mill}: running PO updated from WIP bundle file {File} → PO {Po}.",
            millNo,
            wipFileName,
            normalizedNew);
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

    private List<WipBundleFolderScanner.WipBundleFileCandidate> ScanAllWipCandidates() =>
        WipBundleFolderScanner.Scan(_options.Value).ToList();

    private IEnumerable<string> ResolveBundleFolders() =>
        WipBundleFolderScanner.ResolveBundleFolders(_options.Value);
}
