namespace NdtBundleService.Services.TcpOpenComm;

/// <summary>
/// Wire transport seam for per-mill TCP open PO-end communication.
/// Byte layout is isolated in <see cref="Mill4MessageCodec"/>.
/// </summary>
public interface IMillTcpTransport : IAsyncDisposable
{
    bool IsConnected { get; }

    Task ConnectAsync(CancellationToken cancellationToken);

    Task DisconnectAsync(CancellationToken cancellationToken);

    Task SendAckAsync(byte ackValue, CancellationToken cancellationToken);

    /// <summary>Blocks until disconnect or cancellation; parses inbound frames.</summary>
    Task RunReadLoopAsync(CancellationToken cancellationToken);
}
