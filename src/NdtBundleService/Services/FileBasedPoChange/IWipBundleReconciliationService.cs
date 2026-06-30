namespace NdtBundleService.Services.FileBasedPoChange;

/// <summary>
/// Scans WIP bundle folders for mills with <c>PoEndSource=File</c> and enqueues missed file-based PO-end events.
/// </summary>
public interface IWipBundleReconciliationService
{
    /// <summary>Returns the number of missed PO changes enqueued during this reconciliation pass.</summary>
    Task<int> ReconcileAsync(CancellationToken cancellationToken);
}
