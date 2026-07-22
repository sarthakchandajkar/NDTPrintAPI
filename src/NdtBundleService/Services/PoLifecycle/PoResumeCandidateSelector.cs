namespace NdtBundleService.Services.PoLifecycle;

/// <summary>
/// Chooses which Closed PO may be marked as a WIP-confirmed resume candidate from slit activity alone.
/// Excludes the just-ended PO (late trickle) and POs ended two or more transitions ago.
/// </summary>
public static class PoResumeCandidateSelector
{
    /// <summary>
    /// True when a plan-valid slit row may mark this Closed PO as a resume candidate (reopen still requires WIP).
    /// </summary>
    public static bool IsEligibleForResumeCandidate(
        int millNo,
        string poNumber,
        string? wipEndedPo,
        IReadOnlyList<PoLifecycleDrainEntry> closedEntries)
    {
        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po))
            return false;

        if (!string.IsNullOrWhiteSpace(wipEndedPo)
            && InputSlitCsvParsing.PoEquals(po, wipEndedPo))
            return false;

        var forMill = closedEntries
            .Where(e => e.MillNo == millNo && e.Phase == PoLifecyclePhase.Closed)
            .OrderByDescending(e => e.EndedAtUtc)
            .ToList();

        if (forMill.Count == 0)
            return false;

        var resumeTarget = forMill
            .FirstOrDefault(e => !string.IsNullOrWhiteSpace(e.PoNumber)
                                 && !InputSlitCsvParsing.PoEquals(e.PoNumber, wipEndedPo));

        return !string.IsNullOrWhiteSpace(resumeTarget.PoNumber)
               && InputSlitCsvParsing.PoEquals(po, resumeTarget.PoNumber);
    }
}
