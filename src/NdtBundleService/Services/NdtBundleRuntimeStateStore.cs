using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

public interface INdtBundleRuntimeStateStore
{
    Task EnsureInitializedAsync(CancellationToken cancellationToken);

    Task<MillSequenceStatusSnapshot> GetStatusAsync(CancellationToken cancellationToken);

    /// <summary>Completed bundle count for the mill. Next printed/indexed bundle is <see cref="GetBatchOffset"/> + 1.</summary>
    int GetBatchOffset(string poNumber, int millNo);

    int GetRunningTotal(string poNumber, int millNo);

    void ApplySlitContribution(string poNumber, int millNo, int ndtPipes, int threshold, out int batchNumberForRow, out int totalSoFar);

    /// <summary>Increments completed-bundle count and returns the sequence used for the closed bundle (print + summary CSV).</summary>
    int CloseBundle(string poNumber, int millNo, int closedTotalPcs, int threshold);

    void AdvanceOnPoEnd(string poNumber, int millNo, int threshold);

    int GetEngineBatchNo(string poNumber, int millNo);

    void SetEngineBatchNo(string poNumber, int millNo, int batchNo);

    Dictionary<string, int> GetSizeCounts(string poNumber, int millNo);

    void SetSizeCounts(string poNumber, int millNo, IReadOnlyDictionary<string, int> counts);

    InputSlitRecord? GetLastRecord(string poNumber, int millNo);

    void SetLastRecord(string poNumber, int millNo, InputSlitRecord? record);

    /// <summary>Per-mill high-water marks (batch offset and engine batch no).</summary>
    IReadOnlyDictionary<int, (int BatchOffset, int EngineBatchNo)> GetMillSequenceSnapshot();

    /// <summary>Clears PO/mill aggregation and optionally sets mill sequence counters (for rebuild).</summary>
    void ResetAllStateForRebuild(IReadOnlyDictionary<int, int>? startingSequenceByMill = null);

    /// <summary>Resets in-memory state for rebuild and skips disk hydration on subsequent EnsureInitialized calls.</summary>
    void PrepareForRebuild(IReadOnlyDictionary<int, int>? startingSequenceByMill = null);

    Task SaveAsync(CancellationToken cancellationToken);
}

public sealed class NdtBundleRuntimeStateStore : INdtBundleRuntimeStateStore
{
    private const int CurrentSchemaVersion = 2;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly INdtBundleRepository _bundleRepository;
    private readonly ILogger<NdtBundleRuntimeStateStore> _logger;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly object _stateLock = new();

    private PersistedRoot _root = new();
    private bool _initialized;
    private bool _stateFileLoaded;
    private readonly List<string> _hydrationSources = new();

    public NdtBundleRuntimeStateStore(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        INdtBundleRepository bundleRepository,
        ILogger<NdtBundleRuntimeStateStore> logger)
    {
        _optionsMonitor = optionsMonitor;
        _bundleRepository = bundleRepository;
        _logger = logger;
    }

    private NdtBundleOptions Opt => _optionsMonitor.CurrentValue;

    public async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized)
            return;

        await _initLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_initialized)
                return;

            _hydrationSources.Clear();
            _stateFileLoaded = false;

            if (Opt.EnableNdtBundleRuntimeStatePersistence && TryLoadFromDisk(out var loaded))
            {
                _root = loaded;
                _stateFileLoaded = true;
                _hydrationSources.Add("StateFile");
                _logger.LogInformation(
                    "Loaded NDT bundle runtime state from {Path} (schema v{Version}, {MillCount} mill sequence(s), {PoCount} PO/mill slot(s)).",
                    GetStateFilePath(),
                    loaded.Version,
                    loaded.MillSequences.Count,
                    loaded.Mills.Count);
                await MergeMaxSequenceFromAuthoritativeSourcesAsync(mergeIntoExisting: true, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await HydrateFromBundlesAsync(cancellationToken).ConfigureAwait(false);
            }

            await ValidateSequenceHydrationAsync(cancellationToken).ConfigureAwait(false);

            if (Opt.EnableNdtBundleRuntimeStatePersistence)
                await SaveCoreAsync(cancellationToken).ConfigureAwait(false);

            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    public int GetBatchOffset(string poNumber, int millNo)
    {
        lock (_stateLock)
            return GetMillSequence(millNo).BatchOffset;
    }

    public int GetRunningTotal(string poNumber, int millNo)
    {
        lock (_stateLock)
            return GetPoSlot(poNumber, millNo).RunningTotal;
    }

    public void ApplySlitContribution(string poNumber, int millNo, int ndtPipes, int threshold, out int batchNumberForRow, out int totalSoFar)
    {
        lock (_stateLock)
        {
            var poSlot = GetPoSlot(poNumber, millNo);
            var millSeq = GetMillSequence(millNo);
            if (ndtPipes <= 0)
            {
                totalSoFar = poSlot.RunningTotal;
                batchNumberForRow = 0;
                return;
            }

            poSlot.RunningTotal += ndtPipes;
            totalSoFar = poSlot.RunningTotal;
            batchNumberForRow = millSeq.BatchOffset + 1;

            if (poSlot.RunningTotal >= threshold)
            {
                millSeq.BatchOffset += 1;
                poSlot.RunningTotal = 0;
            }
        }
    }

    public int CloseBundle(string poNumber, int millNo, int closedTotalPcs, int threshold)
    {
        lock (_stateLock)
        {
            var millSeq = GetMillSequence(millNo);
            if (closedTotalPcs <= 0)
                return millSeq.EngineBatchNo;

            millSeq.EngineBatchNo += 1;
            if (millSeq.BatchOffset < millSeq.EngineBatchNo)
                millSeq.BatchOffset = millSeq.EngineBatchNo;

            return millSeq.EngineBatchNo;
        }
    }

    public void AdvanceOnPoEnd(string poNumber, int millNo, int threshold)
    {
        lock (_stateLock)
        {
            var poSlot = GetPoSlot(poNumber, millNo);
            var millSeq = GetMillSequence(millNo);
            var total = poSlot.RunningTotal;
            if (total <= 0)
            {
                poSlot.RunningTotal = 0;
                return;
            }

            var offset = millSeq.BatchOffset;
            var sequence = Math.Max(1, ((total - 1) / threshold) + 1);
            millSeq.BatchOffset = offset + sequence;
            poSlot.RunningTotal = 0;
            millSeq.EngineBatchNo = millSeq.BatchOffset;
        }
    }

    public int GetEngineBatchNo(string poNumber, int millNo)
    {
        lock (_stateLock)
            return GetMillSequence(millNo).EngineBatchNo;
    }

    public void SetEngineBatchNo(string poNumber, int millNo, int batchNo)
    {
        lock (_stateLock)
        {
            var millSeq = GetMillSequence(millNo);
            millSeq.EngineBatchNo = batchNo;
            if (batchNo > millSeq.BatchOffset)
                millSeq.BatchOffset = batchNo;
        }
    }

    public Dictionary<string, int> GetSizeCounts(string poNumber, int millNo)
    {
        lock (_stateLock)
            return new Dictionary<string, int>(GetPoSlot(poNumber, millNo).SizeCounts, StringComparer.OrdinalIgnoreCase);
    }

    public void SetSizeCounts(string poNumber, int millNo, IReadOnlyDictionary<string, int> counts)
    {
        lock (_stateLock)
            GetPoSlot(poNumber, millNo).SizeCounts = new Dictionary<string, int>(counts, StringComparer.OrdinalIgnoreCase);
    }

    public InputSlitRecord? GetLastRecord(string poNumber, int millNo)
    {
        lock (_stateLock)
            return GetPoSlot(poNumber, millNo).LastRecord;
    }

    public void SetLastRecord(string poNumber, int millNo, InputSlitRecord? record)
    {
        lock (_stateLock)
            GetPoSlot(poNumber, millNo).LastRecord = record;
    }

    public IReadOnlyDictionary<int, (int BatchOffset, int EngineBatchNo)> GetMillSequenceSnapshot()
    {
        lock (_stateLock)
        {
            var snapshot = new Dictionary<int, (int BatchOffset, int EngineBatchNo)>();
            foreach (var mill in Enumerable.Range(1, 4))
            {
                if (_root.MillSequences.TryGetValue(MillKey(mill), out var seq))
                    snapshot[mill] = (seq.BatchOffset, seq.EngineBatchNo);
                else
                    snapshot[mill] = (0, 0);
            }

            return snapshot;
        }
    }

    public void ResetAllStateForRebuild(IReadOnlyDictionary<int, int>? startingSequenceByMill = null)
    {
        lock (_stateLock)
        {
            _root = new PersistedRoot { Version = CurrentSchemaVersion, UpdatedUtc = DateTime.UtcNow };
            ApplyStartingSequences(startingSequenceByMill);
        }
    }

    public void PrepareForRebuild(IReadOnlyDictionary<int, int>? startingSequenceByMill = null)
    {
        lock (_stateLock)
        {
            _root = new PersistedRoot { Version = CurrentSchemaVersion, UpdatedUtc = DateTime.UtcNow };
            ApplyStartingSequences(startingSequenceByMill);
            _initialized = true;
        }
    }

    private void ApplyStartingSequences(IReadOnlyDictionary<int, int>? startingSequenceByMill)
    {
        if (startingSequenceByMill is null)
            return;

        foreach (var (mill, start) in startingSequenceByMill)
        {
            if (mill is < 1 or > 4 || start < 0)
                continue;
            _root.MillSequences[MillKey(mill)] = new PersistedMillSequence
            {
                MillNo = mill,
                BatchOffset = start,
                EngineBatchNo = start
            };
        }
    }

    public async Task<MillSequenceStatusSnapshot> GetStatusAsync(CancellationToken cancellationToken)
    {
        await EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var path = GetStateFilePath();
        var writable = false;
        if (Opt.EnableNdtBundleRuntimeStatePersistence && !string.IsNullOrWhiteSpace(path))
        {
            try
            {
                var dir = Path.GetDirectoryName(path);
                if (!string.IsNullOrEmpty(dir))
                {
                    Directory.CreateDirectory(dir);
                    var probe = Path.Combine(dir, ".ndt_state_write_probe");
                    await File.WriteAllTextAsync(probe, "ok", cancellationToken).ConfigureAwait(false);
                    File.Delete(probe);
                    writable = true;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "NDT bundle runtime state path is not writable: {Path}", path);
            }
        }

        var snapshot = GetMillSequenceSnapshot();
        return new MillSequenceStatusSnapshot
        {
            Initialized = _initialized,
            StateFilePath = path,
            StateFileLoaded = _stateFileLoaded,
            StateFileWritable = writable,
            HydrationSources = _hydrationSources.ToArray(),
            BatchOffsetByMill = snapshot.ToDictionary(kv => kv.Key, kv => kv.Value.BatchOffset),
            EngineBatchNoByMill = snapshot.ToDictionary(kv => kv.Key, kv => kv.Value.EngineBatchNo)
        };
    }

    public async Task SaveAsync(CancellationToken cancellationToken)
    {
        if (!Opt.EnableNdtBundleRuntimeStatePersistence)
            return;

        await _initLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await SaveCoreAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _initLock.Release();
        }
    }

    private async Task SaveCoreAsync(CancellationToken cancellationToken)
    {
        PersistedRoot snapshot;
        lock (_stateLock)
        {
            _root.Version = CurrentSchemaVersion;
            _root.UpdatedUtc = DateTime.UtcNow;
            snapshot = CloneRoot(_root);
        }

        var path = GetStateFilePath();
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        var temp = path + ".tmp";
        await File.WriteAllTextAsync(temp, JsonSerializer.Serialize(snapshot, JsonOptions), cancellationToken).ConfigureAwait(false);
        File.Move(temp, path, overwrite: true);
    }

    private bool TryLoadFromDisk(out PersistedRoot loaded)
    {
        loaded = new PersistedRoot();
        var path = GetStateFilePath();
        if (!File.Exists(path))
            return false;

        try
        {
            var json = File.ReadAllText(path);
            var parsed = JsonSerializer.Deserialize<PersistedRoot>(json, JsonOptions);
            if (parsed is null)
                return false;

            MigrateToV2IfNeeded(parsed);
            loaded = parsed;
            return loaded.MillSequences.Count > 0 || loaded.Mills.Count > 0;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not read NDT bundle runtime state from {Path}; will hydrate from bundles.", path);
            return false;
        }
    }

    private static void MigrateToV2IfNeeded(PersistedRoot root)
    {
        if (root.Version >= CurrentSchemaVersion && root.MillSequences.Count > 0)
            return;

        foreach (var slot in root.Mills.Values)
        {
            if (slot.MillNo is < 1 or > 4)
                continue;

            var key = MillKey(slot.MillNo);
            if (!root.MillSequences.TryGetValue(key, out var millSeq))
            {
                millSeq = new PersistedMillSequence { MillNo = slot.MillNo };
                root.MillSequences[key] = millSeq;
            }

            if (slot.BatchOffset > millSeq.BatchOffset)
                millSeq.BatchOffset = slot.BatchOffset;
            if (slot.EngineBatchNo > millSeq.EngineBatchNo)
                millSeq.EngineBatchNo = slot.EngineBatchNo;
        }

        root.Version = CurrentSchemaVersion;
    }

    private Task MergeMaxSequenceFromBundlesAsync(CancellationToken cancellationToken) =>
        MergeMaxSequenceFromAuthoritativeSourcesAsync(mergeIntoExisting: true, cancellationToken);

    private async Task HydrateFromBundlesAsync(CancellationToken cancellationToken)
    {
        _root = new PersistedRoot { Version = CurrentSchemaVersion };
        await MergeMaxSequenceFromAuthoritativeSourcesAsync(mergeIntoExisting: false, cancellationToken).ConfigureAwait(false);
    }

    private async Task MergeMaxSequenceFromAuthoritativeSourcesAsync(bool mergeIntoExisting, CancellationToken cancellationToken)
    {
        var byMill = new Dictionary<int, int>();

        try
        {
            var fromSql = await _bundleRepository.GetMaxSequenceByMillForCurrentYearAsync(cancellationToken).ConfigureAwait(false);
            foreach (var (mill, seq) in fromSql)
                byMill[mill] = seq;
            if (fromSql.Count > 0)
                _hydrationSources.Add("SqlMaxSequence");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not load max NDT bundle sequence per mill from SQL.");
        }

        var bundles = await _bundleRepository.GetBundlesAsync(cancellationToken).ConfigureAwait(false);
        foreach (var b in bundles)
        {
            if (b.MillNo is < 1 or > 4)
                continue;
            if (!NdtBundleSequence.TryParseSequenceForCurrentYear(b.BundleNo, b.MillNo, out var seq))
                continue;

            if (!byMill.TryGetValue(b.MillNo, out var max) || seq > max)
                byMill[b.MillNo] = seq;
        }

        if (bundles.Count > 0 && byMill.Count > 0 && !_hydrationSources.Contains("SqlMaxSequence"))
            _hydrationSources.Add("BundleScan");

        if (byMill.Count == 0)
        {
            if (!mergeIntoExisting)
                _logger.LogInformation("No existing NDT bundles found; runtime state starts at sequence 0 for all mills.");
            return;
        }

        var raised = ApplyMaxSequencesToState(byMill, mergeIntoExisting);

        if (mergeIntoExisting && raised > 0)
        {
            _logger.LogWarning(
                "Raised NDT bundle sequence for {Count} mill(s) to match printed bundles in SQL/CSV (state file was behind).",
                raised);
        }
        else if (!mergeIntoExisting)
        {
            _logger.LogInformation(
                "Hydrated NDT bundle runtime state from {BundleCount} bundle record(s); {MillCount} mill(s) with sequence restored.",
                bundles.Count,
                byMill.Count);
        }
    }

    private int ApplyMaxSequencesToState(IReadOnlyDictionary<int, int> byMill, bool mergeIntoExisting)
    {
        var raised = 0;
        lock (_stateLock)
        {
            foreach (var (mill, maxSeq) in byMill)
            {
                var millSeq = GetMillSequence(mill);
                if (mergeIntoExisting && maxSeq <= millSeq.BatchOffset && maxSeq <= millSeq.EngineBatchNo)
                    continue;

                if (maxSeq > millSeq.BatchOffset)
                    millSeq.BatchOffset = maxSeq;
                if (maxSeq > millSeq.EngineBatchNo)
                    millSeq.EngineBatchNo = maxSeq;
                raised++;

                _logger.LogInformation(
                    "Mill-{Mill} resumed at NDT bundle sequence {Seq} (formatted example: {Example}).",
                    mill,
                    maxSeq,
                    NdtBundleSequence.Format(maxSeq, mill));
            }
        }

        return raised;
    }

    private async Task ValidateSequenceHydrationAsync(CancellationToken cancellationToken)
    {
        if (!Opt.RequireSequenceHydration || !Opt.EnableSlitMonitoringWorker)
            return;

        IReadOnlyDictionary<int, int> authoritative;
        try
        {
            authoritative = await _bundleRepository.GetMaxSequenceByMillForCurrentYearAsync(cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            return;
        }

        if (authoritative.Count == 0)
            return;

        var snapshot = GetMillSequenceSnapshot();
        var staleMills = new List<int>();
        foreach (var (mill, maxSeq) in authoritative)
        {
            if (!snapshot.TryGetValue(mill, out var current))
            {
                staleMills.Add(mill);
                continue;
            }

            var currentMax = Math.Max(current.BatchOffset, current.EngineBatchNo);
            if (currentMax < maxSeq)
                staleMills.Add(mill);
        }

        if (staleMills.Count == 0)
            return;

        var mills = string.Join(", ", staleMills.Select(m => $"Mill-{m}"));
        var message =
            $"NDT bundle sequence hydration failed for {mills}: SQL/CSV shows current-year bundles but runtime counters are too low. " +
            "Fix NdtBundleRuntimeState.json permissions, verify SQL connectivity, or run rebuild-ndt-from-date before processing slits.";

        _logger.LogCritical(message);
        throw new InvalidOperationException(message);
    }

    private string GetStateFilePath()
    {
        var custom = (Opt.NdtBundleRuntimeStateFile ?? string.Empty).Trim();
        if (!string.IsNullOrEmpty(custom))
            return custom;

        var folder = (Opt.OutputBundleFolder ?? string.Empty).Trim();
        if (string.IsNullOrEmpty(folder))
            folder = AppContext.BaseDirectory;

        return Path.Combine(folder, "NdtBundleRuntimeState.json");
    }

    private PersistedPoMillSlot GetPoSlot(string poNumber, int millNo)
    {
        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        var key = MakePoKey(po, millNo);
        if (!_root.Mills.TryGetValue(key, out var slot))
        {
            slot = new PersistedPoMillSlot { PoNumber = po, MillNo = millNo };
            _root.Mills[key] = slot;
        }

        return slot;
    }

    private PersistedMillSequence GetMillSequence(int millNo)
    {
        var key = MillKey(millNo);
        if (!_root.MillSequences.TryGetValue(key, out var seq))
        {
            seq = new PersistedMillSequence { MillNo = millNo };
            _root.MillSequences[key] = seq;
        }

        return seq;
    }

    private static string MakePoKey(string poNumber, int millNo) =>
        $"{InputSlitCsvParsing.NormalizePo(poNumber)}|{millNo}";

    private static string MillKey(int millNo) => millNo.ToString();

    private static PersistedRoot CloneRoot(PersistedRoot source)
    {
        var clone = new PersistedRoot { Version = source.Version, UpdatedUtc = source.UpdatedUtc };
        foreach (var (k, v) in source.MillSequences)
        {
            clone.MillSequences[k] = new PersistedMillSequence
            {
                MillNo = v.MillNo,
                BatchOffset = v.BatchOffset,
                EngineBatchNo = v.EngineBatchNo
            };
        }

        foreach (var (k, v) in source.Mills)
        {
            clone.Mills[k] = new PersistedPoMillSlot
            {
                PoNumber = v.PoNumber,
                MillNo = v.MillNo,
                RunningTotal = v.RunningTotal,
                SizeCounts = new Dictionary<string, int>(v.SizeCounts, StringComparer.OrdinalIgnoreCase),
                LastRecord = v.LastRecord,
                BatchOffset = v.BatchOffset,
                EngineBatchNo = v.EngineBatchNo
            };
        }

        return clone;
    }

    private sealed class PersistedRoot
    {
        public int Version { get; set; } = CurrentSchemaVersion;
        public DateTime UpdatedUtc { get; set; }
        public Dictionary<string, PersistedMillSequence> MillSequences { get; set; } = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, PersistedPoMillSlot> Mills { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    }

    private sealed class PersistedMillSequence
    {
        public int MillNo { get; set; }
        public int BatchOffset { get; set; }
        public int EngineBatchNo { get; set; }
    }

    private sealed class PersistedPoMillSlot
    {
        public string PoNumber { get; set; } = string.Empty;
        public int MillNo { get; set; }
        public int RunningTotal { get; set; }
        public Dictionary<string, int> SizeCounts { get; set; } = new(StringComparer.OrdinalIgnoreCase);
        public InputSlitRecord? LastRecord { get; set; }

        // Legacy v1 fields — read during migration only; not written in v2 saves from PO operations.
        public int BatchOffset { get; set; }
        public int EngineBatchNo { get; set; }
    }
}
