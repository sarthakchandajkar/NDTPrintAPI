namespace NdtBundleService.Models;

/// <summary>
/// Per-slit traceability row for Reconcile Bundle (aggregated by slit when multiple Output_Slit_Row rows share a slit).
/// </summary>
public sealed record ReconcileSlitDetail(
    string SlitNo,
    int NdtPipes,
    int RejectedPipes,
    DateTime? SlitStartTime,
    DateTime? SlitFinishTime,
    string? NdtShortLengthPipe,
    string? RejectedShortLengthPipe);
