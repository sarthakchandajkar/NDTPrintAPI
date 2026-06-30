namespace NdtBundleService.Services;

/// <summary>
/// Per-mill async lock serializing bundle-state mutations (PO end, slit threshold closes, future TCP PO end).
/// Phase 6 TCP workers should call <see cref="IPoEndWorkflowService.ExecuteAsync"/> which acquires this lock.
/// </summary>
public interface IMillBundleStateLock
{
    /// <summary>Acquires the lock for mill 1–4. Caller must dispose the returned handle to release.</summary>
    Task<IDisposable> AcquireAsync(int millNo, CancellationToken cancellationToken);
}
