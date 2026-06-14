using System.Globalization;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Pure rules for NDT bundle runtime state: mill-floor sync, batch assignment, and idle-slot pruning.
/// </summary>
public static class NdtBundleRuntimeStateLogic
{
    public static bool HasOpenPartialBundle(int runningTotal, IReadOnlyDictionary<string, int>? sizeCounts)
    {
        if (runningTotal > 0)
            return true;

        if (sizeCounts is null || sizeCounts.Count == 0)
            return false;

        foreach (var count in sizeCounts.Values)
        {
            if (count > 0)
                return true;
        }

        return false;
    }

    /// <summary>Current open batch sequence for a slit row (completed count + 1).</summary>
    public static int ResolveOpenBatchNumber(int batchOffset) => batchOffset + 1;

    /// <summary>
    /// Raises <paramref name="batchOffset"/> and <paramref name="engineBatchNo"/> to the mill floor
    /// unless an open partial bundle is in progress (restart-safe).
    /// </summary>
    public static void ApplyMillFloorIfAllowed(
        ref int batchOffset,
        ref int engineBatchNo,
        int runningTotal,
        IReadOnlyDictionary<string, int>? sizeCounts,
        int millFloor)
    {
        if (millFloor <= 0)
            return;

        if (HasOpenPartialBundle(runningTotal, sizeCounts))
            return;

        if (batchOffset < millFloor)
            batchOffset = millFloor;
        if (engineBatchNo < millFloor)
            engineBatchNo = millFloor;
    }

    public static void RaiseMillMaxSequence(IDictionary<string, int> millMaxSequence, int millNo, int sequence)
    {
        if (millNo is < 1 or > 4 || sequence <= 0)
            return;

        var key = millNo.ToString(CultureInfo.InvariantCulture);
        if (!millMaxSequence.TryGetValue(key, out var current) || sequence > current)
            millMaxSequence[key] = sequence;
    }

    public static void PromoteSlotSequencesToMillMax(
        IDictionary<string, int> millMaxSequence,
        int millNo,
        int batchOffset,
        int engineBatchNo)
    {
        RaiseMillMaxSequence(millMaxSequence, millNo, batchOffset);
        RaiseMillMaxSequence(millMaxSequence, millNo, engineBatchNo);
    }

    public static bool CanPruneSlot(
        string slotPoNumber,
        int slotMillNo,
        int runningTotal,
        IReadOnlyDictionary<string, int>? sizeCounts,
        DateTime lastActivityUtc,
        DateTime rootUpdatedUtc,
        IReadOnlyDictionary<int, string> activePoByMill,
        DateTime utcNow,
        RuntimeStatePruningOptions options)
    {
        if (!options.Enabled)
            return false;

        if (HasOpenPartialBundle(runningTotal, sizeCounts))
            return false;

        if (activePoByMill.TryGetValue(slotMillNo, out var activePo)
            && !string.IsNullOrWhiteSpace(activePo)
            && string.Equals(
                InputSlitCsvParsing.NormalizePo(activePo),
                InputSlitCsvParsing.NormalizePo(slotPoNumber),
                StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (options.GracePeriodDays > 0)
        {
            var lastActivity = lastActivityUtc != default ? lastActivityUtc : rootUpdatedUtc;
            if (lastActivity == default)
                return false;

            if ((utcNow - lastActivity).TotalDays < options.GracePeriodDays)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Returns slot keys to remove. Promotes each pruned slot's sequences into <paramref name="millMaxSequence"/> first.
    /// </summary>
    public static IReadOnlyList<string> SelectSlotsToPrune(
        IReadOnlyDictionary<string, RuntimeStateSlotSnapshot> slots,
        IDictionary<string, int> millMaxSequence,
        DateTime rootUpdatedUtc,
        IReadOnlyDictionary<int, string> activePoByMill,
        DateTime utcNow,
        RuntimeStatePruningOptions options)
    {
        if (!options.Enabled)
            return Array.Empty<string>();

        var toRemove = new List<string>();
        foreach (var (key, slot) in slots)
        {
            if (!CanPruneSlot(
                    slot.PoNumber,
                    slot.MillNo,
                    slot.RunningTotal,
                    slot.SizeCounts,
                    slot.LastActivityUtc,
                    rootUpdatedUtc,
                    activePoByMill,
                    utcNow,
                    options))
            {
                continue;
            }

            PromoteSlotSequencesToMillMax(millMaxSequence, slot.MillNo, slot.BatchOffset, slot.EngineBatchNo);
            toRemove.Add(key);
        }

        return toRemove;
    }
}

/// <summary>Immutable view of a persisted PO/mill slot for logic and tests.</summary>
public sealed record RuntimeStateSlotSnapshot(
    string PoNumber,
    int MillNo,
    int BatchOffset,
    int RunningTotal,
    int EngineBatchNo,
    IReadOnlyDictionary<string, int> SizeCounts,
    DateTime LastActivityUtc);
