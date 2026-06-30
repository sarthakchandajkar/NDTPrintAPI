using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services.TcpOpenComm;

/// <summary>
/// Raw <see cref="TcpClient"/> transport for per-mill TCP open PO-end (not S7.Net).
/// Assumption: PLC is TCP server; MES connects outbound to <see cref="MillConfig.TcpOpenCommHost"/>:<see cref="MillConfig.TcpOpenCommPort"/>.
/// </summary>
public sealed class MillTcpOpenCommTransport : IMillTcpTransport
{
    private readonly MillConfig _mill;
    private readonly ILogger _logger;
    private TcpClient? _client;
    private NetworkStream? _stream;
    private readonly object _streamLock = new();

    public MillTcpOpenCommTransport(MillConfig mill, ILogger logger)
    {
        _mill = mill;
        _logger = logger;
    }

    public bool IsConnected => _client?.Connected == true;

    public event Action<MillTcpPoEndMessage>? PoEndMessageReceived;

    public async Task ConnectAsync(CancellationToken cancellationToken)
    {
        var host = _mill.TcpOpenCommHost!;
        var port = _mill.TcpOpenCommPort;
        var millNo = _mill.ResolveMillNo();

        _client = new TcpClient();
        await _client.ConnectAsync(host, port, cancellationToken).ConfigureAwait(false);
        _stream = _client.GetStream();

        _logger.LogInformation(
            "{MillName}: TCP open-comm connected to {Host}:{Port} (Mill {MillNo}).",
            _mill.Name,
            host,
            port,
            millNo);
    }

    public Task DisconnectAsync(CancellationToken cancellationToken)
    {
        lock (_streamLock)
        {
            try
            {
                _stream?.Close();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "{MillName}: error closing TCP stream.", _mill.Name);
            }

            try
            {
                _client?.Close();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "{MillName}: error closing TCP client.", _mill.Name);
            }

            _stream = null;
            _client = null;
        }

        return Task.CompletedTask;
    }

    public async Task SendAckAsync(byte ackValue, CancellationToken cancellationToken)
    {
        var ack = Mill4MessageCodec.BuildAck(ackValue);
        NetworkStream? stream;
        lock (_streamLock)
            stream = _stream;

        if (stream is null)
            throw new InvalidOperationException($"{_mill.Name}: TCP stream is not connected.");

        await stream.WriteAsync(ack, cancellationToken).ConfigureAwait(false);
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task RunReadLoopAsync(CancellationToken cancellationToken)
    {
        var buffer = new byte[Mill4MessageCodec.MinimumFrameLength];
        var millNo = _mill.ResolveMillNo();

        while (!cancellationToken.IsCancellationRequested)
        {
            NetworkStream? stream;
            lock (_streamLock)
                stream = _stream;

            if (stream is null)
                break;

            var totalRead = 0;
            while (totalRead < buffer.Length)
            {
                var read = await stream.ReadAsync(buffer.AsMemory(totalRead, buffer.Length - totalRead), cancellationToken)
                    .ConfigureAwait(false);
                if (read == 0)
                {
                    _logger.LogWarning(
                        "{MillName}: TCP open-comm peer closed connection (Mill {MillNo}).",
                        _mill.Name,
                        millNo);
                    return;
                }

                totalRead += read;
            }

            if (!Mill4MessageCodec.TryParsePoEndMessage(buffer, out var parsed))
            {
                _logger.LogWarning(
                    "{MillName}: TCP open-comm received unparseable frame ({Length} bytes).",
                    _mill.Name,
                    buffer.Length);
                continue;
            }

            if (!parsed.TriggerActive)
                continue;

            var message = new MillTcpPoEndMessage(
                millNo,
                parsed.PoTypeId,
                parsed.TriggerActive,
                buffer.ToArray(),
                DateTimeOffset.UtcNow);

            PoEndMessageReceived?.Invoke(message);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await DisconnectAsync(CancellationToken.None).ConfigureAwait(false);
        _client?.Dispose();
    }
}
