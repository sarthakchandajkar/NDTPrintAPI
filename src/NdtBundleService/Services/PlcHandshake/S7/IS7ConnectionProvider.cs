using S7.Net;

namespace NdtBundleService.Services.PlcHandshake.S7;

/// <summary>
/// Single shared S7 connection per mill. All PLC I/O for that mill must go through this provider.
/// Delegates passed to <see cref="Read{T}"/> / <see cref="Write"/> / async variants must be pure
/// <see cref="IS7PlcOperations"/> and must never call back into <see cref="IS7ConnectionProvider"/>
/// (the gate uses a non-reentrant <see cref="SemaphoreSlim"/>).
/// </summary>
public interface IS7ConnectionProvider : IAsyncDisposable
{
    int MillNo { get; }

    string MillName { get; }

    /// <summary>True when an open S7 session exists.</summary>
    bool IsConnected { get; }

    /// <summary>Connected and recent I/O has not marked the session unhealthy.</summary>
    bool IsHealthy { get; }

    /// <summary>Raised on failed→ok and ok→failed transitions (true = healthy).</summary>
    event Action<bool>? HealthChanged;

    /// <summary>Ensure a live connection (tries configured slots). Does not sleep on failure.</summary>
    Task<bool> EnsureConnectedAsync(CancellationToken cancellationToken);

    /// <summary>Close the session (manual disconnect / error recovery).</summary>
    void Disconnect();

    /// <summary>
    /// Synchronous read under the mill I/O gate. Prefer for the handshake poll loop
    /// (dedicated background task). Do not nest provider calls inside <paramref name="operation"/>.
    /// </summary>
    T Read<T>(Func<IS7PlcOperations, T> operation);

    /// <summary>
    /// Synchronous write under the mill I/O gate. Do not nest provider calls inside <paramref name="operation"/>.
    /// </summary>
    void Write(Action<IS7PlcOperations> operation);

    /// <summary>Async read under the mill I/O gate (e.g. NDT count reader).</summary>
    Task<T> ReadAsync<T>(Func<IS7PlcOperations, T> operation, CancellationToken cancellationToken = default);

    /// <summary>Async write under the mill I/O gate.</summary>
    Task WriteAsync(Action<IS7PlcOperations> operation, CancellationToken cancellationToken = default);

    /// <summary>Next reconnect sleep (ms) after a failed connect; advances exponential backoff.</summary>
    int TakeReconnectDelayMs();

    /// <summary>Reset reconnect backoff after a successful connect.</summary>
    void ResetReconnectBackoff();
}
