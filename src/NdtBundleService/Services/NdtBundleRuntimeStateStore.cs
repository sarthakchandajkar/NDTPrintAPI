using System.Globalization;
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

    /// <summary>Completed bundle count for (PO, mill). Next printed/indexed bundle is <see cref="GetBatchOffset"/> + 1.</summary>
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

    Task SaveAsync(CancellationToken cancellationToken);
}

public sealed class NdtBundleRuntimeStateStore : INdtBundleRuntimeStateStore
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly INdtBundleRepository _bundleRepository;
    private readonly IActivePoPerMillService _activePoPerMill;
    private readonly ILogger<NdtBundleRuntimeStateStore> _logger;
    private readonly SemaphoreSlim _initLock = new(1, 1);
    private readonly object _stateLock = new();

    private PersistedRoot _root = new();
    private bool _initialized;

    public NdtBundleRuntimeStateStore(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        INdtBundleRepository bundleRepository,
        IActivePoPerMillService activePoPerMill,
        ILogger<NdtBundleRuntimeStateStore> logger)
    {
        _optionsMonitor = optionsMonitor;
        _bundleRepository = bundleRepository;
        _activePoPerMill = activePoPerMill;
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

            if (Opt.EnableNdtBundleRuntimeStatePersistence && TryLoadFromDisk(out var loaded) && loaded.Mills.Count > 0)
            {
                _root = loaded;
                _logger.LogInformation(
                    "Loaded NDT bundle runtime state from {Path} ({Count} PO/mill slot(s)).",
                    GetStateFilePath(),
                    loaded.Mills.Count);
                await MergeMaxSequenceFromBundlesAsync(cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await HydrateFromBundlesAsync(cancellationToken).ConfigureAwait(false);
            }

            lock (_stateLock)
            {
                RebuildMillMaxFromSlots();
                ApplyInitialMillBatchNumberSeeds();
                SyncAllSlotsToMillFloors();
                _logger.LogInformation(
                    "NDT batch sequence floors after initialization: {MillFloors}. Next bundle per mill is floor + 1 when a new bundle closes.",
                    FormatMillFloorsSnapshot());
            }

            await PruneCompletedSlotsOnStartupAsync(cancellationToken).ConfigureAwait(false);

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
            return GetSlot(poNumber, millNo).BatchOffset;
    }

    public int GetRunningTotal(string poNumber, int millNo)
    {
        lock (_stateLock)
            return GetSlot(poNumber, millNo).RunningTotal;
    }

    public void ApplySlitContribution(string poNumber, int millNo, int ndtPipes, int threshold, out int batchNumberForRow, out int totalSoFar)
    {
        lock (_stateLock)
        {
            var slot = GetSlot(poNumber, millNo);
            TouchSlotActivity(slot);

            if (ndtPipes <= 0)
            {
                totalSoFar = slot.RunningTotal;
                batchNumberForRow = NdtBundleRuntimeStateLogic.ResolveOpenBatchNumber(slot.BatchOffset);
                return;
            }

            slot.RunningTotal += ndtPipes;
            totalSoFar = slot.RunningTotal;
            batchNumberForRow = NdtBundleRuntimeStateLogic.ResolveOpenBatchNumber(slot.BatchOffset);

            if (slot.RunningTotal >= threshold)
            {
                slot.BatchOffset += 1;
                RaiseMillMaxSequence(millNo, slot.BatchOffset);
                slot.RunningTotal = 0;
            }
        }
    }

    public int CloseBundle(string poNumber, int millNo, int closedTotalPcs, int threshold)
    {
        lock (_stateLock)
        {
            var slot = GetSlot(poNumber, millNo);
            TouchSlotActivity(slot);
            if (closedTotalPcs <= 0)
                return slot.EngineBatchNo;

            slot.EngineBatchNo += 1;
            if (slot.BatchOffset < slot.EngineBatchNo)
                slot.BatchOffset = slot.EngineBatchNo;
            RaiseMillMaxSequence(millNo, slot.EngineBatchNo);

            return slot.EngineBatchNo;
        }
    }

    public void AdvanceOnPoEnd(string poNumber, int millNo, int threshold)
    {
        lock (_stateLock)
        {
            var slot = GetSlot(poNumber, millNo);
            TouchSlotActivity(slot);
            var total = slot.RunningTotal;
            if (total <= 0)
            {
                slot.RunningTotal = 0;
                return;
            }

            var offset = slot.BatchOffset;
            var sequence = Math.Max(1, ((total - 1) / threshold) + 1);
            slot.BatchOffset = offset + sequence;
            slot.RunningTotal = 0;
            slot.EngineBatchNo = slot.BatchOffset;
            RaiseMillMaxSequence(millNo, slot.BatchOffset);
        }
    }

    public int GetEngineBatchNo(string poNumber, int millNo)
    {
        lock (_stateLock)
            return GetSlot(poNumber, millNo).EngineBatchNo;
    }

    public void SetEngineBatchNo(string poNumber, int millNo, int batchNo)
    {
        lock (_stateLock)
        {
            var slot = GetSlot(poNumber, millNo);
            TouchSlotActivity(slot);
            slot.EngineBatchNo = batchNo;
            if (batchNo > slot.BatchOffset)
                slot.BatchOffset = batchNo;
            RaiseMillMaxSequence(millNo, batchNo);
        }
    }

    public Dictionary<string, int> GetSizeCounts(string poNumber, int millNo)
    {
        lock (_stateLock)
            return new Dictionary<string, int>(GetSlot(poNumber, millNo).SizeCounts, StringComparer.OrdinalIgnoreCase);
    }

    public void SetSizeCounts(string poNumber, int millNo, IReadOnlyDictionary<string, int> counts)
    {
        lock (_stateLock)
        {
            var slot = GetSlot(poNumber, millNo);
            TouchSlotActivity(slot);
            slot.SizeCounts = new Dictionary<string, int>(counts, StringComparer.OrdinalIgnoreCase);
        }
    }

    public InputSlitRecord? GetLastRecord(string poNumber, int millNo)
    {
        lock (_stateLock)
            return GetSlot(poNumber, millNo).LastRecord;
    }

    public void SetLastRecord(string poNumber, int millNo, InputSlitRecord? record)
    {
        lock (_stateLock)
        {
            var slot = GetSlot(poNumber, millNo);
            TouchSlotActivity(slot);
            slot.LastRecord = record;
        }
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

    private async Task PruneCompletedSlotsOnStartupAsync(CancellationToken cancellationToken)
    {
        var options = Opt.RuntimeStatePruning ?? new RuntimeStatePruningOptions();
        if (!options.Enabled || !options.RunOnStartup)
            return;

        IReadOnlyDictionary<int, string> activePoByMill;
        try
        {
            activePoByMill = await _activePoPerMill.GetLatestPoByMillAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Skipping NDT runtime state prune on startup: could not resolve active PO per mill.");
            return;
        }

        var utcNow = DateTime.UtcNow;
        List<string> removedKeys;
        lock (_stateLock)
        {
            var snapshots = _root.Mills.ToDictionary(
                kv => kv.Key,
                kv => ToSnapshot(kv.Value),
                StringComparer.OrdinalIgnoreCase);
            removedKeys = NdtBundleRuntimeStateLogic
                .SelectSlotsToPrune(snapshots, _root.MillMaxSequence, _root.UpdatedUtc, activePoByMill, utcNow, options)
                .ToList();

            foreach (var key in removedKeys)
                _root.Mills.Remove(key);
        }

        if (removedKeys.Count == 0)
            return;

        _logger.LogInformation(
            "Pruned {Count} idle NDT runtime state slot(s) on startup (grace {GraceDays} day(s); active PO slots retained). Mill floors: {MillFloors}.",
            removedKeys.Count,
            options.GracePeriodDays,
            FormatMillFloorsSnapshot());
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
            if (parsed is null || parsed.Mills.Count == 0)
                return false;
            loaded = parsed;
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not read NDT bundle runtime state from {Path}; will hydrate from bundles.", path);
            return false;
        }
    }

    private Task MergeMaxSequenceFromBundlesAsync(CancellationToken cancellationToken) =>
        ApplyMaxSequencesFromBundlesAsync(mergeIntoExisting: true, cancellationToken);

    private async Task HydrateFromBundlesAsync(CancellationToken cancellationToken)
    {
        _root = new PersistedRoot();
        await ApplyMaxSequencesFromBundlesAsync(mergeIntoExisting: false, cancellationToken).ConfigureAwait(false);
    }

    private async Task ApplyMaxSequencesFromBundlesAsync(bool mergeIntoExisting, CancellationToken cancellationToken)
    {
        var bundles = await _bundleRepository.GetBundlesAsync(cancellationToken).ConfigureAwait(false);
        if (bundles.Count == 0)
        {
            if (!mergeIntoExisting)
                _logger.LogInformation("No existing NDT bundles found; runtime state starts at sequence 0 for all PO/mill pairs.");
            return;
        }

        var byKey = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var byMill = new Dictionary<int, int>();
        foreach (var b in bundles)
        {
            if (b.MillNo is < 1 or > 4)
                continue;
            if (!NdtBundleSequence.TryParseSequenceForCurrentYear(b.BundleNo, b.MillNo, out var seq))
                continue;

            if (!byMill.TryGetValue(b.MillNo, out var millMax) || seq > millMax)
                byMill[b.MillNo] = seq;

            var po = InputSlitCsvParsing.NormalizePo(b.PoNumber);
            var key = MakeKey(po, b.MillNo);
            if (!byKey.TryGetValue(key, out var max) || seq > max)
                byKey[key] = seq;
        }

        var raised = 0;
        lock (_stateLock)
        {
            foreach (var (mill, maxSeq) in byMill)
                RaiseMillMaxSequence(mill, maxSeq);

            foreach (var (key, maxSeq) in byKey)
            {
                var (po, mill) = ParseKey(key);
                var slot = GetSlotWithoutMillFloorSync(po, mill);
                if (mergeIntoExisting && maxSeq <= slot.BatchOffset && maxSeq <= slot.EngineBatchNo)
                    continue;

                if (maxSeq > slot.BatchOffset)
                    slot.BatchOffset = maxSeq;
                if (maxSeq > slot.EngineBatchNo)
                    slot.EngineBatchNo = maxSeq;
                raised++;
            }
        }

        if (mergeIntoExisting && raised > 0)
        {
            _logger.LogWarning(
                "Raised NDT bundle sequence for {Count} PO/mill slot(s) to match printed bundles in SQL/CSV (state file was behind).",
                raised);
        }
        else if (!mergeIntoExisting)
        {
            _logger.LogInformation(
                "Hydrated NDT bundle runtime state from {BundleCount} bundle record(s); {SlotCount} PO/mill slot(s) with sequence restored; mill floors: {MillFloors}.",
                bundles.Count,
                byKey.Count,
                FormatMillFloorsSnapshot());
        }
    }

    private void RebuildMillMaxFromSlots()
    {
        foreach (var slot in _root.Mills.Values)
        {
            RaiseMillMaxSequence(slot.MillNo, slot.BatchOffset);
            RaiseMillMaxSequence(slot.MillNo, slot.EngineBatchNo);
        }
    }

    private void ApplyInitialMillBatchNumberSeeds()
    {
        var seeds = Opt.InitialMillBatchNumbers;
        if (seeds is null || seeds.Count == 0)
            return;

        foreach (var (millKey, batchNoRaw) in seeds)
        {
            if (!int.TryParse(millKey, NumberStyles.None, CultureInfo.InvariantCulture, out var millNo)
                || millNo is < 1 or > 4)
            {
                _logger.LogWarning("Ignoring InitialMillBatchNumbers entry with invalid mill key {Key}.", millKey);
                continue;
            }

            var batchNo = (batchNoRaw ?? string.Empty).Trim();
            if (string.IsNullOrEmpty(batchNo))
                continue;

            if (!NdtBundleSequence.TryParseSequence(batchNo, millNo, out var seq)
                && (!int.TryParse(batchNo, NumberStyles.None, CultureInfo.InvariantCulture, out seq) || seq <= 0))
            {
                _logger.LogWarning(
                    "Ignoring InitialMillBatchNumbers entry for mill {Mill}: could not parse {Value}.",
                    millNo,
                    batchNo);
                continue;
            }

            var previous = GetMillMaxSequence(millNo);
            RaiseMillMaxSequence(millNo, seq);
            if (seq > previous)
            {
                _logger.LogInformation(
                    "NDT batch sequence floor for mill {Mill} set from config to {Seq} ({BatchNo}).",
                    millNo,
                    seq,
                    NdtBundleSequence.Format(seq, millNo));
            }
        }
    }

    private void SyncAllSlotsToMillFloors()
    {
        foreach (var slot in _root.Mills.Values)
            SyncSlotToMillFloor(slot);
    }

    private void SyncSlotToMillFloor(PersistedMillSlot slot)
    {
        var floor = GetMillMaxSequence(slot.MillNo);
        var batchOffset = slot.BatchOffset;
        var engineBatchNo = slot.EngineBatchNo;
        NdtBundleRuntimeStateLogic.ApplyMillFloorIfAllowed(
            ref batchOffset,
            ref engineBatchNo,
            slot.RunningTotal,
            slot.SizeCounts,
            floor);
        slot.BatchOffset = batchOffset;
        slot.EngineBatchNo = engineBatchNo;
    }

    private int GetMillMaxSequence(int millNo)
    {
        var key = millNo.ToString(CultureInfo.InvariantCulture);
        return _root.MillMaxSequence.TryGetValue(key, out var max) ? max : 0;
    }

    private void RaiseMillMaxSequence(int millNo, int sequence) =>
        NdtBundleRuntimeStateLogic.RaiseMillMaxSequence(_root.MillMaxSequence, millNo, sequence);

    private string FormatMillFloorsSnapshot()
    {
        var parts = new List<string>();
        for (var mill = 1; mill <= 4; mill++)
        {
            var seq = GetMillMaxSequence(mill);
            if (seq <= 0)
                continue;
            parts.Add($"M{mill}={NdtBundleSequence.Format(seq, mill)}");
        }

        return parts.Count == 0 ? "(none)" : string.Join(", ", parts);
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

    private PersistedMillSlot GetSlot(string poNumber, int millNo) =>
        GetOrCreateSlot(poNumber, millNo, syncMillFloor: true);

    private PersistedMillSlot GetSlotWithoutMillFloorSync(string poNumber, int millNo) =>
        GetOrCreateSlot(poNumber, millNo, syncMillFloor: false);

    private PersistedMillSlot GetOrCreateSlot(string poNumber, int millNo, bool syncMillFloor)
    {
        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        var key = MakeKey(po, millNo);
        var isNew = !_root.Mills.TryGetValue(key, out var slot);
        if (isNew)
        {
            slot = new PersistedMillSlot { PoNumber = po, MillNo = millNo };
            _root.Mills[key] = slot;
        }

        if (syncMillFloor && (isNew || !NdtBundleRuntimeStateLogic.HasOpenPartialBundle(slot.RunningTotal, slot.SizeCounts)))
            SyncSlotToMillFloor(slot);

        return slot;
    }

    private static void TouchSlotActivity(PersistedMillSlot slot) =>
        slot.LastActivityUtc = DateTime.UtcNow;

    private static RuntimeStateSlotSnapshot ToSnapshot(PersistedMillSlot slot) =>
        new(
            slot.PoNumber,
            slot.MillNo,
            slot.BatchOffset,
            slot.RunningTotal,
            slot.EngineBatchNo,
            slot.SizeCounts,
            slot.LastActivityUtc);

    private static string MakeKey(string poNumber, int millNo) =>
        $"{InputSlitCsvParsing.NormalizePo(poNumber)}|{millNo}";

    private static (string Po, int Mill) ParseKey(string key)
    {
        var idx = key.LastIndexOf('|');
        if (idx <= 0)
            return (key, 1);
        var po = key[..idx];
        var mill = int.TryParse(key[(idx + 1)..], out var m) ? m : 1;
        return (po, mill);
    }

    private static PersistedRoot CloneRoot(PersistedRoot source)
    {
        var clone = new PersistedRoot { Version = source.Version, UpdatedUtc = source.UpdatedUtc };
        foreach (var (k, v) in source.MillMaxSequence)
            clone.MillMaxSequence[k] = v;
        foreach (var (k, v) in source.Mills)
        {
            clone.Mills[k] = new PersistedMillSlot
            {
                PoNumber = v.PoNumber,
                MillNo = v.MillNo,
                BatchOffset = v.BatchOffset,
                RunningTotal = v.RunningTotal,
                EngineBatchNo = v.EngineBatchNo,
                SizeCounts = new Dictionary<string, int>(v.SizeCounts, StringComparer.OrdinalIgnoreCase),
                LastRecord = v.LastRecord,
                LastActivityUtc = v.LastActivityUtc
            };
        }

        return clone;
    }

    private sealed class PersistedRoot
    {
        public int Version { get; set; } = 1;
        public DateTime UpdatedUtc { get; set; }
        /// <summary>Highest completed NDT batch sequence per mill (keys <c>"1"</c>–<c>"4"</c>).</summary>
        public Dictionary<string, int> MillMaxSequence { get; set; } = new(StringComparer.OrdinalIgnoreCase);
        public Dictionary<string, PersistedMillSlot> Mills { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    }

    private sealed class PersistedMillSlot
    {
        public string PoNumber { get; set; } = string.Empty;
        public int MillNo { get; set; }
        public int BatchOffset { get; set; }
        public int RunningTotal { get; set; }
        public int EngineBatchNo { get; set; }
        public Dictionary<string, int> SizeCounts { get; set; } = new(StringComparer.OrdinalIgnoreCase);
        public InputSlitRecord? LastRecord { get; set; }
        public DateTime LastActivityUtc { get; set; }
    }
}
