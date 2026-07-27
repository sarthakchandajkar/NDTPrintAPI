using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services.PoLifecycle;

/// <inheritdoc />
/// <remarks>
/// Draining/Closed phases are persisted to <c>PoLifecycleState.json</c> (next to the bundle runtime
/// state file). Without persistence a service restart silently reset every PO to Running, which
/// disabled closed-PO traceability-only routing and let late slit files re-open closed bundles
/// (2026-07-26 incident: restarts at 17:20/17:52 dropped the Closed phase for PO 1000060288).
/// </remarks>
public sealed class PoLifecycleService : IPoLifecycleService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly ILogger<PoLifecycleService>? _logger;
    private readonly object _lock = new();
    private readonly Dictionary<string, Entry> _entries = new(StringComparer.OrdinalIgnoreCase);
    private bool _loaded;

    public PoLifecycleService(IOptionsMonitor<NdtBundleOptions> options, ILogger<PoLifecycleService>? logger = null)
    {
        _options = options;
        _logger = logger;
    }

    public bool TryMarkDraining(int millNo, string poNumber, DateTime endedAtUtc)
    {
        if (!IsPlcLifecycleMill(millNo))
            return false;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po) || millNo is < 1 or > 4)
            return false;

        lock (_lock)
        {
            EnsureLoaded();
            _entries[MakeKey(millNo, po)] = new Entry
            {
                MillNo = millNo,
                PoNumber = po,
                EndedAtUtc = endedAtUtc,
                Phase = PoLifecyclePhase.Draining
            };
            Save();
        }

        return true;
    }

    public bool TryMarkClosed(int millNo, string poNumber)
    {
        if (!IsPlcLifecycleMill(millNo))
            return false;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po) || millNo is < 1 or > 4)
            return false;

        lock (_lock)
        {
            EnsureLoaded();
            if (!_entries.TryGetValue(MakeKey(millNo, po), out var entry))
                return false;

            entry.Phase = PoLifecyclePhase.Closed;
            Save();
            return true;
        }
    }

    public bool TryReopen(int millNo, string poNumber)
    {
        if (!IsPlcLifecycleMill(millNo))
            return false;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po) || millNo is < 1 or > 4)
            return false;

        lock (_lock)
        {
            EnsureLoaded();
            if (!_entries.TryGetValue(MakeKey(millNo, po), out var entry))
                return false;

            if (entry.Phase != PoLifecyclePhase.Closed)
                return false;

            entry.Phase = PoLifecyclePhase.Running;
            entry.IsResumeCandidate = false;
            Save();
            return true;
        }
    }

    public bool TryMarkResumeCandidate(int millNo, string poNumber)
    {
        if (!IsPlcLifecycleMill(millNo))
            return false;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po) || millNo is < 1 or > 4)
            return false;

        lock (_lock)
        {
            EnsureLoaded();
            if (!_entries.TryGetValue(MakeKey(millNo, po), out var entry))
                return false;

            if (entry.Phase != PoLifecyclePhase.Closed)
                return false;

            entry.IsResumeCandidate = true;
            Save();
            return true;
        }
    }

    public bool IsResumeCandidate(int millNo, string poNumber)
    {
        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po) || millNo is < 1 or > 4)
            return false;

        lock (_lock)
        {
            EnsureLoaded();
            return _entries.TryGetValue(MakeKey(millNo, po), out var entry)
                   && entry.Phase == PoLifecyclePhase.Closed
                   && entry.IsResumeCandidate;
        }
    }

    public PoLifecyclePhase GetPhase(int millNo, string poNumber)
    {
        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po) || millNo is < 1 or > 4)
            return PoLifecyclePhase.Running;

        lock (_lock)
        {
            EnsureLoaded();
            return _entries.TryGetValue(MakeKey(millNo, po), out var entry)
                ? entry.Phase
                : PoLifecyclePhase.Running;
        }
    }

    public IReadOnlyList<PoLifecycleDrainEntry> GetExpiredDrains(DateTime utcNow, TimeSpan drainWindow)
    {
        lock (_lock)
        {
            EnsureLoaded();
            return _entries.Values
                .Where(e => e.Phase == PoLifecyclePhase.Draining && utcNow - e.EndedAtUtc >= drainWindow)
                .Select(e => new PoLifecycleDrainEntry(e.MillNo, e.PoNumber, e.EndedAtUtc, e.Phase))
                .ToList();
        }
    }

    public IReadOnlyList<PoLifecycleDrainEntry> GetClosedEntries()
    {
        lock (_lock)
        {
            EnsureLoaded();
            return _entries.Values
                .Where(e => e.Phase == PoLifecyclePhase.Closed)
                .Select(e => new PoLifecycleDrainEntry(e.MillNo, e.PoNumber, e.EndedAtUtc, e.Phase))
                .ToList();
        }
    }

    private bool IsPlcLifecycleMill(int millNo) =>
        MillPoEndSourceResolver.ForMill(millNo, _options.CurrentValue) == MillPoEndSource.Plc;

    private static string MakeKey(int millNo, string po) => $"{millNo}|{po}";

    private string? GetStateFilePath()
    {
        var opt = _options.CurrentValue;
        if (!opt.EnableNdtBundleRuntimeStatePersistence)
            return null;

        var runtimeStateFile = (opt.NdtBundleRuntimeStateFile ?? string.Empty).Trim();
        if (!string.IsNullOrEmpty(runtimeStateFile))
        {
            var dir = Path.GetDirectoryName(runtimeStateFile);
            if (!string.IsNullOrEmpty(dir))
                return Path.Combine(dir, "PoLifecycleState.json");
        }

        var folder = (opt.OutputBundleFolder ?? string.Empty).Trim();
        if (!string.IsNullOrEmpty(folder))
            return Path.Combine(folder, "PoLifecycleState.json");

        return null;
    }

    private void EnsureLoaded()
    {
        if (_loaded)
            return;

        _loaded = true;
        var path = GetStateFilePath();
        if (path is null || !File.Exists(path))
            return;

        try
        {
            var json = File.ReadAllText(path);
            var persisted = JsonSerializer.Deserialize<List<PersistedEntry>>(json, JsonOptions);
            if (persisted is null)
                return;

            foreach (var p in persisted)
            {
                if (string.IsNullOrWhiteSpace(p.PoNumber)
                    || p.MillNo is < 1 or > 4
                    || p.Phase == PoLifecyclePhase.Running)
                {
                    continue;
                }

                _entries[MakeKey(p.MillNo, p.PoNumber)] = new Entry
                {
                    MillNo = p.MillNo,
                    PoNumber = p.PoNumber,
                    EndedAtUtc = p.EndedAtUtc,
                    Phase = p.Phase,
                    IsResumeCandidate = p.IsResumeCandidate
                };
            }

            if (_entries.Count > 0)
            {
                _logger?.LogInformation(
                    "Restored {Count} PO lifecycle phase(s) from {Path}: {Entries}.",
                    _entries.Count,
                    path,
                    string.Join(", ", _entries.Values.Select(e => $"M{e.MillNo}/{e.PoNumber}={e.Phase}")));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to load PO lifecycle state from {Path}; starting with all POs Running.", path);
        }
    }

    private void Save()
    {
        var path = GetStateFilePath();
        if (path is null)
            return;

        try
        {
            var persisted = _entries.Values
                .Where(e => e.Phase != PoLifecyclePhase.Running)
                .Select(e => new PersistedEntry
                {
                    MillNo = e.MillNo,
                    PoNumber = e.PoNumber,
                    EndedAtUtc = e.EndedAtUtc,
                    Phase = e.Phase,
                    IsResumeCandidate = e.IsResumeCandidate
                })
                .ToList();
            File.WriteAllText(path, JsonSerializer.Serialize(persisted, JsonOptions));
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to persist PO lifecycle state to {Path}; phases will reset on restart.", path);
        }
    }

    private sealed class Entry
    {
        public int MillNo { get; set; }
        public string PoNumber { get; set; } = string.Empty;
        public DateTime EndedAtUtc { get; set; }
        public PoLifecyclePhase Phase { get; set; }
        public bool IsResumeCandidate { get; set; }
    }

    private sealed class PersistedEntry
    {
        public int MillNo { get; set; }
        public string PoNumber { get; set; } = string.Empty;
        public DateTime EndedAtUtc { get; set; }
        [JsonConverter(typeof(JsonStringEnumConverter))]
        public PoLifecyclePhase Phase { get; set; }
        public bool IsResumeCandidate { get; set; }
    }
}
