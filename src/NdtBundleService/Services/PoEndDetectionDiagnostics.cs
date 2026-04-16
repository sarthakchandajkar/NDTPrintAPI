namespace NdtBundleService.Services;

/// <summary>
/// Last PO-end detection state for dashboard / troubleshooting (updated by <see cref="PlcPoEndPollHandler"/>).
/// </summary>
public sealed class PoEndDetectionDiagnostics
{
    private readonly object _sync = new();

    public PoEndDetectionDiagnostics()
    {
        Mills = new MillPoEndMillDiag[4];
        for (var i = 0; i < 4; i++)
            Mills[i] = new MillPoEndMillDiag();
    }

    public MillPoEndMillDiag[] Mills { get; }

    public string DetectionMode { get; private set; } = "CoilRisingEdge";

    public void SetDetectionMode(string mode)
    {
        lock (_sync)
            DetectionMode = mode ?? "CoilRisingEdge";
    }

    public void UpdateMill(int millNo, Action<MillPoEndMillDiag> apply)
    {
        if (millNo is < 1 or > 4 || apply is null)
            return;
        lock (_sync)
            apply(Mills[millNo - 1]);
    }

    public PoEndDetectionDiagnosticsSnapshot GetSnapshot()
    {
        lock (_sync)
        {
            var copy = new MillPoEndMillDiag[4];
            for (var i = 0; i < 4; i++)
            {
                var m = Mills[i];
                copy[i] = new MillPoEndMillDiag
                {
                    CurrentPoIdFromPlc = m.CurrentPoIdFromPlc,
                    TrackedPrevPoId = m.TrackedPrevPoId,
                    LastSlitEntryCount = m.LastSlitEntryCount,
                    LastTransitionUtc = m.LastTransitionUtc,
                    LastEndedPoNumber = m.LastEndedPoNumber,
                    LastError = m.LastError
                };
            }

            return new PoEndDetectionDiagnosticsSnapshot(DetectionMode, copy);
        }
    }
}

public sealed class MillPoEndMillDiag
{
    public int? CurrentPoIdFromPlc { get; set; }

    public int? TrackedPrevPoId { get; set; }

    public int? LastSlitEntryCount { get; set; }

    public DateTimeOffset? LastTransitionUtc { get; set; }

    public string? LastEndedPoNumber { get; set; }

    public string? LastError { get; set; }
}

public sealed record PoEndDetectionDiagnosticsSnapshot(string DetectionMode, IReadOnlyList<MillPoEndMillDiag> Mills);
