namespace NdtBundleService.Services;

/// <summary>SAP lifecycle status of one NDT Input Slit output CSV (keyed by file basename).</summary>
public enum OutputSlitSapStatus
{
    /// <summary>In the pending folder (<c>OutputBundleFolder</c>); SAP has not pulled it yet.</summary>
    Pending = 0,

    /// <summary>SAP posted the data; file moved to the NDT Input Slit Accepted folder. Terminal — frozen locally.</summary>
    Accepted = 1,

    /// <summary>SAP rejected the file; operator edits it and resubmits to the pending folder.</summary>
    Rejected = 2
}

/// <summary>How a stored status row must react to a folder observation.</summary>
public enum OutputSlitSapStatusTransitionKind
{
    /// <summary>No row exists for the basename yet — insert with the observed status.</summary>
    InsertNew = 0,

    /// <summary>Observed status equals stored status — refresh file version metadata only, no event.</summary>
    NoChange = 1,

    /// <summary>Regular status change (e.g. Pending → Accepted, Pending → Rejected).</summary>
    Transition = 2,

    /// <summary>Rejected → Pending: the operator resubmitted the edited file (increment Resubmit_Count).</summary>
    Resubmit = 3,

    /// <summary>Stored status is Accepted and the observation disagrees — keep Accepted (frozen), audit only.</summary>
    IgnoreFrozenAccepted = 4
}

/// <summary>
/// Pure decisions for SAP status tracking of NDT Input Slit output files (F: NDT Input Slit
/// Accepted/Rejected watcher). See <c>docs/NDT_Input_Slit_SAP_Status_Design.md</c>.
/// </summary>
public static class OutputSlitSapStatusPolicy
{
    /// <summary>
    /// Derives the observed status from the folders currently containing the basename.
    /// Accepted wins over stale copies elsewhere; a file in the pending folder while a copy still
    /// sits in Rejected is a resubmit-in-flight and counts as Pending (operators may copy rather
    /// than move when resubmitting). Callers must not invoke this when the file is in no folder —
    /// absence keeps the last stored status because SAP archives Accepted files after ~6 months.
    /// </summary>
    public static OutputSlitSapStatus DeriveObservedStatus(bool inPending, bool inAccepted, bool inRejected)
    {
        if (inAccepted)
            return OutputSlitSapStatus.Accepted;
        if (inPending)
            return OutputSlitSapStatus.Pending;
        return OutputSlitSapStatus.Rejected;
    }

    /// <summary>Transition action for a stored status (<c>null</c> = no row yet) and an observation.</summary>
    public static OutputSlitSapStatusTransitionKind Decide(OutputSlitSapStatus? current, OutputSlitSapStatus observed)
    {
        if (current is null)
            return OutputSlitSapStatusTransitionKind.InsertNew;

        if (current.Value == observed)
            return OutputSlitSapStatusTransitionKind.NoChange;

        if (current.Value == OutputSlitSapStatus.Accepted)
            return OutputSlitSapStatusTransitionKind.IgnoreFrozenAccepted;

        if (current.Value == OutputSlitSapStatus.Rejected && observed == OutputSlitSapStatus.Pending)
            return OutputSlitSapStatusTransitionKind.Resubmit;

        return OutputSlitSapStatusTransitionKind.Transition;
    }

    /// <summary>Database string for a status (stored in <c>Output_Slit_Sap_Status.Status</c>).</summary>
    public static string ToDbString(OutputSlitSapStatus status) => status switch
    {
        OutputSlitSapStatus.Accepted => "Accepted",
        OutputSlitSapStatus.Rejected => "Rejected",
        _ => "Pending"
    };

    /// <summary>
    /// Strongest status across a slit's contributing files for display/gating:
    /// Accepted &gt; Rejected &gt; Pending; <c>null</c> when no file has a recorded status.
    /// A slit backed by any Accepted file is treated as Accepted (its data is posted to SAP).
    /// </summary>
    public static OutputSlitSapStatus? Strongest(IEnumerable<OutputSlitSapStatus> statuses)
    {
        OutputSlitSapStatus? strongest = null;
        foreach (var s in statuses)
        {
            if (s == OutputSlitSapStatus.Accepted)
                return OutputSlitSapStatus.Accepted;
            if (s == OutputSlitSapStatus.Rejected)
                strongest = OutputSlitSapStatus.Rejected;
            else if (strongest is null)
                strongest = OutputSlitSapStatus.Pending;
        }

        return strongest;
    }

    /// <summary>
    /// Ingest-path write gate for an output basename (Phase 2 + Phase 4). Only a plain first-pass
    /// Pending file (or an untracked one, <c>null</c> status) may be re-emitted / re-inserted by
    /// the system:
    /// <list type="bullet">
    /// <item><c>Accepted</c> — frozen, data already posted (hard requirement 3).</item>
    /// <item><c>RejectedInFlight</c> — the operator copy-edit-resubmit flow owns the basename; a
    /// system re-emit would auto-resubmit unedited data.</item>
    /// <item><c>Resubmitted</c> — <c>Resubmit_Count &gt; 0</c>: the operator-edited pending copy is
    /// authoritative and must never be clobbered (the ExactMatch bypass — treated as covered even
    /// though its content differs from what the system would regenerate).</item>
    /// </list>
    /// </summary>
    public static OutputSlitIngestGate DecideIngestGate(OutputSlitSapFileStatus? status)
    {
        if (status is null)
            return OutputSlitIngestGate.None;
        if (status.Status == OutputSlitSapStatus.Accepted)
            return OutputSlitIngestGate.Accepted;
        if (status.Status == OutputSlitSapStatus.Rejected)
            return OutputSlitIngestGate.RejectedInFlight;
        return status.ResubmitCount > 0
            ? OutputSlitIngestGate.Resubmitted
            : OutputSlitIngestGate.None;
    }

    /// <summary>Parses a database status string; unknown values return <c>null</c>.</summary>
    public static OutputSlitSapStatus? TryParse(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        return value.Trim() switch
        {
            var s when s.Equals("Pending", StringComparison.OrdinalIgnoreCase) => OutputSlitSapStatus.Pending,
            var s when s.Equals("Accepted", StringComparison.OrdinalIgnoreCase) => OutputSlitSapStatus.Accepted,
            var s when s.Equals("Rejected", StringComparison.OrdinalIgnoreCase) => OutputSlitSapStatus.Rejected,
            _ => null
        };
    }
}

/// <summary>Why the ingest path must not rewrite an output CSV / its <c>Output_Slit_Row</c> rows.</summary>
public enum OutputSlitIngestGate
{
    None = 0,
    Accepted = 1,
    RejectedInFlight = 2,
    Resubmitted = 3
}

/// <summary>Current SAP status row for one output file basename (read model for API/gating).</summary>
public sealed record OutputSlitSapFileStatus(
    OutputSlitSapStatus Status,
    DateTime StatusAtUtc,
    int ResubmitCount);

/// <summary>One folder observation for a file basename, produced by the watcher or seed-on-write.</summary>
public sealed record OutputSlitSapStatusObservation(
    string FileName,
    OutputSlitSapStatus Observed,
    string ObservedFolder,
    DateTime? FileLastWriteTimeUtc,
    string Source)
{
    public const string SourceWatcher = "Watcher";
    public const string SourceSeedOnWrite = "SeedOnWrite";
}
