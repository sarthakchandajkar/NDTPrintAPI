namespace NdtBundleService.Services.TcpOpenComm;

/// <summary>
/// Wire transport seam for per-mill TCP open PO-end communication (Phase 6).
/// Byte layout is isolated in <see cref="Mill4MessageCodec"/>; business logic uses <see cref="PoEndMessageReceived"/> only.
/// </summary>
public interface IMillTcpTransport : IAsyncDisposable
{
  bool IsConnected { get; }

  event Action<MillTcpPoEndMessage>? PoEndMessageReceived;

  Task ConnectAsync(CancellationToken cancellationToken);

  Task DisconnectAsync(CancellationToken cancellationToken);

  Task SendAckAsync(byte ackValue, CancellationToken cancellationToken);

  /// <summary>Blocks until disconnect or cancellation; parses inbound frames and raises <see cref="PoEndMessageReceived"/>.</summary>
  Task RunReadLoopAsync(CancellationToken cancellationToken);
}
