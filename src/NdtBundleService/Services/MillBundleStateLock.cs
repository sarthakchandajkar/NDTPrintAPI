namespace NdtBundleService.Services;

/// <inheritdoc />
public sealed class MillBundleStateLock : IMillBundleStateLock
{
    private readonly SemaphoreSlim[] _locks =
    [
        new SemaphoreSlim(1, 1),
        new SemaphoreSlim(1, 1),
        new SemaphoreSlim(1, 1),
        new SemaphoreSlim(1, 1)
    ];

    public async Task<IDisposable> AcquireAsync(int millNo, CancellationToken cancellationToken)
    {
        if (millNo is < 1 or > 4)
            throw new ArgumentOutOfRangeException(nameof(millNo), millNo, "MillNo must be between 1 and 4.");

        await _locks[millNo - 1].WaitAsync(cancellationToken).ConfigureAwait(false);
        return new Releaser(_locks[millNo - 1]);
    }

    private sealed class Releaser(SemaphoreSlim semaphore) : IDisposable
    {
        private int _disposed;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            semaphore.Release();
        }
    }
}
