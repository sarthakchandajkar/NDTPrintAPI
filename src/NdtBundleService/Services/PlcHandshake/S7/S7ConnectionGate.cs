namespace NdtBundleService.Services.PlcHandshake.S7;

/// <summary>
/// Non-reentrant mill I/O gate. DEBUG builds throw if a nested provider call is attempted
/// (SemaphoreSlim would otherwise deadlock the mill).
/// </summary>
public sealed class S7ConnectionGate : IDisposable
{
    private readonly SemaphoreSlim _gate = new(1, 1);

#if DEBUG
    private static readonly AsyncLocal<bool> InProviderCall = new();
#endif

    public T Execute<T>(Func<T> operation)
    {
        EnterReentrancyGuard();
        _gate.Wait();
        try
        {
            return operation();
        }
        finally
        {
            _gate.Release();
            ExitReentrancyGuard();
        }
    }

    public void Execute(Action operation) =>
        Execute<object?>(() =>
        {
            operation();
            return null;
        });

    public async Task<T> ExecuteAsync<T>(Func<T> operation, CancellationToken cancellationToken)
    {
        EnterReentrancyGuard();
        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return operation();
        }
        finally
        {
            _gate.Release();
            ExitReentrancyGuard();
        }
    }

    public Task ExecuteAsync(Action operation, CancellationToken cancellationToken) =>
        ExecuteAsync<object?>(() =>
        {
            operation();
            return null;
        }, cancellationToken);

    private static void EnterReentrancyGuard()
    {
#if DEBUG
        if (InProviderCall.Value)
        {
            throw new S7ConnectionReentrancyException(
                "Nested IS7ConnectionProvider call detected. Delegates passed to Read/Write must be pure Plc " +
                "operations and must never call back into IS7ConnectionProvider (SemaphoreSlim is non-reentrant).");
        }

        InProviderCall.Value = true;
#endif
    }

    private static void ExitReentrancyGuard()
    {
#if DEBUG
        InProviderCall.Value = false;
#endif
    }

    public void Dispose() => _gate.Dispose();
}
