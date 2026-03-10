using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// When PoPlanFolder is configured: maintains one "current" PO plan file from that folder.
/// A new file is used only when PO End is triggered (button or eventually PLC). Files are processed in arrival order (oldest first).
/// </summary>
public sealed class CurrentPoPlanService : ICurrentPoPlanService
{
    private readonly NdtBundleOptions _options;
    private readonly ILogger<CurrentPoPlanService> _logger;
    private readonly object _lock = new();

    private string[] _orderedPaths = Array.Empty<string>();
    private int _currentIndex = -1;
    /// <summary>Path of the last PO plan we finished (when we advanced past end). Used so we don't re-select the same file until a new one appears.</summary>
    private string? _lastFinishedPath;

    public CurrentPoPlanService(IOptions<NdtBundleOptions> options, ILogger<CurrentPoPlanService> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public Task<string?> GetCurrentPoPlanPathAsync(CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            RefreshListIfNeeded();
            if (_currentIndex < 0 || _currentIndex >= _orderedPaths.Length)
                return Task.FromResult<string?>(null);
            return Task.FromResult<string?>(_orderedPaths[_currentIndex]);
        }
    }

    public async Task<string?> GetCurrentPoNumberAsync(CancellationToken cancellationToken)
    {
        var path = await GetCurrentPoPlanPathAsync(cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrEmpty(path) || !File.Exists(path))
            return null;

        await using var stream = File.OpenRead(path);
        using var reader = new StreamReader(stream);
        var headerLine = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
        if (headerLine is null)
            return null;

        var headers = headerLine.Split(',');
        var poIndex = Array.FindIndex(headers, h =>
            h.Trim().Equals("PO_No", StringComparison.OrdinalIgnoreCase) ||
            h.Trim().Equals("PO Number", StringComparison.OrdinalIgnoreCase));
        if (poIndex < 0)
            return null;

        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;
            var cols = line.Split(',');
            if (poIndex < cols.Length)
            {
                var po = cols[poIndex].Trim();
                if (!string.IsNullOrEmpty(po))
                    return po;
            }
        }

        return null;
    }

    public Task AdvanceToNextPoAsync(CancellationToken cancellationToken)
    {
        lock (_lock)
        {
            RefreshListIfNeeded();
            if (_orderedPaths.Length == 0)
            {
                _logger.LogInformation("PO End: no files in PoPlanFolder; no current PO.");
                return Task.CompletedTask;
            }

            var previousPath = _currentIndex >= 0 && _currentIndex < _orderedPaths.Length
                ? _orderedPaths[_currentIndex]
                : null;
            _currentIndex++;
            if (_currentIndex >= _orderedPaths.Length)
            {
                _currentIndex = -1;
                _lastFinishedPath = previousPath;
                _logger.LogInformation("PO End: PO marked finished. Advanced past last file in PoPlanFolder. Next poll will load new PO when a new file appears. Last finished: {Path}", previousPath != null ? Path.GetFileName(previousPath) : "(none)");
                return Task.CompletedTask;
            }

            var nextPath = _orderedPaths[_currentIndex];
            _logger.LogInformation("PO End: advanced to next PO plan file. Previous={Previous}, Next={Next}", previousPath != null ? Path.GetFileName(previousPath) : "(none)", Path.GetFileName(nextPath));
            return Task.CompletedTask;
        }
    }

    private void RefreshListIfNeeded()
    {
        var folder = _options.PoPlanFolder;
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
        {
            _orderedPaths = Array.Empty<string>();
            _currentIndex = -1;
            return;
        }

        var files = Directory.EnumerateFiles(folder, "*.csv")
            .OrderBy(f => new FileInfo(f).LastWriteTimeUtc)
            .ToArray();

        if (files.Length == 0)
        {
            _orderedPaths = Array.Empty<string>();
            _currentIndex = -1;
            return;
        }

        // If we had no current: either first run (use oldest) or we just finished a PO (use newest only if it's a new file).
        if (_currentIndex < 0 || _currentIndex >= _orderedPaths.Length)
        {
            _orderedPaths = files;
            var newestPath = _orderedPaths[_orderedPaths.Length - 1];
            if (_lastFinishedPath != null && string.Equals(newestPath, _lastFinishedPath, StringComparison.OrdinalIgnoreCase))
            {
                // No new file yet; keep waiting. Do not re-select the same finished PO.
                _currentIndex = -1;
                _logger.LogDebug("PoPlanFolder: no new PO file yet (newest still is last finished). Waiting for new file.");
                return;
            }
            // First run: use oldest so we process POs in order. After finishing all: use newest (the newly added file).
            _currentIndex = _lastFinishedPath == null ? 0 : files.Length - 1;
            if (_lastFinishedPath == null)
                _logger.LogInformation("Current PO plan: {File} (oldest of {Count} in folder; will advance to next on PO End)", Path.GetFileName(_orderedPaths[_currentIndex]), _orderedPaths.Length);
            else
            {
                _lastFinishedPath = null;
                _logger.LogInformation("Current PO plan: {File} (new PO loaded; {Count} file(s) in folder)", Path.GetFileName(_orderedPaths[_currentIndex]), _orderedPaths.Length);
            }
            return;
        }

        // Keep same index if list changed (e.g. new file arrived); reorder and try to preserve position.
        _orderedPaths = files;
        if (_currentIndex >= _orderedPaths.Length)
            _currentIndex = _orderedPaths.Length - 1;
    }
}
