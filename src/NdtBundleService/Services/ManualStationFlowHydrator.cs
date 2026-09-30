namespace NdtBundleService.Services;

/// <summary>
/// Rebuilds Visual → Hydro → Revisual flow state from durable <c>Manual_Station_Run</c> rows
/// (and optionally from a consolidated NDT process CSV) after process restart.
/// </summary>
internal static class ManualStationFlowHydrator
{
    public sealed class StationCounts
    {
        public int OkPcs { get; init; }
        public int RejectedPcs { get; init; }
        public DateTime StartTime { get; init; }
        public DateTime EndTime { get; init; }
    }

    public sealed class HydrateResult
    {
        public StationCounts? Visual { get; init; }
        public StationCounts? Hydrotesting { get; init; }
        public StationCounts? Revisual { get; init; }
        public bool HydroInvalidatedByVisualReconcile { get; init; }
        public bool RevisualInvalidatedByUpstreamReconcile { get; init; }
        public string? Source { get; init; }
    }

    public static HydrateResult? TryFromManualStationRuns(IReadOnlyList<ManualStationPrintedTag> runs)
    {
        if (runs.Count == 0)
            return null;

        var ordered = runs
            .OrderBy(r => r.ImportedAtUtc)
            .ThenBy(r => r.Id)
            .ToList();

        var visuals = ordered.Where(IsVisual).ToList();
        var hydros = ordered.Where(IsHydro).ToList();
        var revisuals = ordered.Where(IsRevisual).ToList();

        // Prefer the latest completed pipeline (Revisual anchors), walking back so a later
        // orphan Visual on another physical station cannot wipe a finished Hydro/Revisual chain.
        if (revisuals.Count > 0)
        {
            var revisual = revisuals[^1];
            var hydro = LastAtOrBefore(hydros, revisual);
            var visual = hydro is null ? null : LastAtOrBefore(visuals, hydro);
            if (visual is not null && hydro is not null)
            {
                return new HydrateResult
                {
                    Visual = ToCounts(visual),
                    Hydrotesting = ToCounts(hydro),
                    Revisual = ToCounts(revisual),
                    Source = "Manual_Station_Run"
                };
            }
        }

        // Partial chain: latest Visual, then Hydro after it, then Revisual after Hydro.
        if (visuals.Count == 0)
            return null;

        var v = visuals[^1];
        ManualStationPrintedTag? h = null;
        ManualStationPrintedTag? r = null;
        var hydroInvalidated = false;
        var revisualInvalidated = false;

        var hydroAfterVisual = hydros.LastOrDefault(x => Cmp(x, v) > 0);
        if (hydroAfterVisual is not null)
        {
            h = hydroAfterVisual;
            r = revisuals.LastOrDefault(x => Cmp(x, h) > 0);
            if (r is null && revisuals.Count > 0 && Cmp(revisuals[^1], h) < 0)
                revisualInvalidated = true;
        }
        else if (hydros.Count > 0 && Cmp(v, hydros[^1]) > 0)
        {
            // Latest Visual is after the last Hydro → same as Visual reconcile cascade.
            hydroInvalidated = true;
            revisualInvalidated = true;
        }

        return new HydrateResult
        {
            Visual = ToCounts(v),
            Hydrotesting = h is null ? null : ToCounts(h),
            Revisual = r is null ? null : ToCounts(r),
            HydroInvalidatedByVisualReconcile = hydroInvalidated,
            RevisualInvalidatedByUpstreamReconcile = revisualInvalidated,
            Source = "Manual_Station_Run"
        };
    }

    /// <summary>
    /// Fallback when SQL station rows are missing but a consolidated NDT process CSV exists.
    /// </summary>
    public static HydrateResult? TryFromNdtProcessMetrics(
        int ndtPcs,
        int okPcs,
        int visualReject,
        int hydroReject,
        int revisualReject,
        DateTime? bundleStart,
        DateTime? bundleEnd)
    {
        if (ndtPcs < 0 || okPcs < 0 || visualReject < 0 || hydroReject < 0 || revisualReject < 0)
            return null;
        if (ndtPcs == 0 && okPcs == 0 && visualReject == 0 && hydroReject == 0 && revisualReject == 0)
            return null;

        var visualOk = Math.Max(0, ndtPcs - visualReject);
        var hydroOk = Math.Max(0, visualOk - hydroReject);
        // Revisual OK is the CSV OK column; reject is independent.
        var start = bundleStart ?? DateTime.Now;
        var end = bundleEnd ?? start;

        return new HydrateResult
        {
            Visual = new StationCounts
            {
                OkPcs = visualOk,
                RejectedPcs = visualReject,
                StartTime = start,
                EndTime = start
            },
            Hydrotesting = new StationCounts
            {
                OkPcs = hydroOk,
                RejectedPcs = hydroReject,
                StartTime = start,
                EndTime = end
            },
            Revisual = new StationCounts
            {
                OkPcs = okPcs,
                RejectedPcs = revisualReject,
                StartTime = end,
                EndTime = end
            },
            Source = "NDT_process_CSV"
        };
    }

    public static bool IsVisual(ManualStationPrintedTag row) =>
        (row.WorkStation ?? string.Empty).StartsWith("Visual", StringComparison.OrdinalIgnoreCase);

    public static bool IsHydro(ManualStationPrintedTag row)
    {
        var ws = row.WorkStation ?? string.Empty;
        if (ws.Equals("Hydrotesting", StringComparison.OrdinalIgnoreCase))
            return true;
        if (ws.Contains("Hydro", StringComparison.OrdinalIgnoreCase))
            return true;
        var ht = row.HydrotestingType ?? string.Empty;
        return ht.Contains("Hydro", StringComparison.OrdinalIgnoreCase);
    }

    public static bool IsRevisual(ManualStationPrintedTag row) =>
        (row.WorkStation ?? string.Empty).StartsWith("Revisual", StringComparison.OrdinalIgnoreCase);

    private static ManualStationPrintedTag? LastAtOrBefore(
        List<ManualStationPrintedTag> rows,
        ManualStationPrintedTag anchor)
    {
        ManualStationPrintedTag? best = null;
        foreach (var row in rows)
        {
            if (Cmp(row, anchor) <= 0)
                best = row;
        }

        return best;
    }

    private static int Cmp(ManualStationPrintedTag a, ManualStationPrintedTag b)
    {
        var c = a.ImportedAtUtc.CompareTo(b.ImportedAtUtc);
        return c != 0 ? c : a.Id.CompareTo(b.Id);
    }

    private static StationCounts ToCounts(ManualStationPrintedTag row) =>
        new()
        {
            OkPcs = row.OkPcs,
            RejectedPcs = row.RejectPcs,
            StartTime = row.BundleStart,
            EndTime = row.BundleEnd
        };
}
