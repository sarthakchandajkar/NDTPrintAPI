// =============================================================================
// UNRESOLVED — TCP connection role (structural assumption, not a config toggle):
//   This transport implements MES = TCP client (TcpClient → PLC CP server port).
//   If NetPro configures the CP AG_SEND partner as ACTIVE (PLC dials out to MES),
//   swap ONLY EstablishConnectionAsync to use TcpListener — the read loop, codec,
//   and worker integration remain unchanged.
// =============================================================================

using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using NdtBundleService.Configuration;
using Serilog.Context;

namespace NdtBundleService.Services.TcpOpenComm;

/// <summary>
/// Raw <see cref="TcpClient"/> transport for per-mill TCP open PO-end (not S7.Net, not port 102).
/// </summary>
public sealed class MillTcpOpenCommTransport : IMillTcpTransport
{
    private readonly MillConfig _mill;
    private readonly ILogger _logger;
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private TcpClient? _client;
    private NetworkStream? _stream;
    private readonly object _streamLock = new();

    public MillTcpOpenCommTransport(MillConfig mill, ILogger logger)
    {
        _mill = mill;
        _logger = logger;
    }

    public bool IsConnected => _client?.Connected == true;

    public Func<MillTcpPoEndMessage, CancellationToken, Task>? OnTriggerMessageAsync { get; set; }

    public async Task ConnectAsync(CancellationToken cancellationToken)
    {
        await EstablishConnectionAsync(cancellationToken).ConfigureAwait(false);

        var millNo = _mill.ResolveMillNo();
        var endpoint = FormatEndpoint();
        _logger.LogInformation(
            "TCP connected — Mill {MillNo}, PoEndSource=TcpOpen, endpoint {Endpoint}.",
            millNo,
            endpoint);
    }

    /// <summary>
    /// MES-as-client connection establishment. Replace with <see cref="TcpListener"/> accept
    /// if the controls engineer configures the CP as the active TCP partner.
    /// </summary>
    private async Task EstablishConnectionAsync(CancellationToken cancellationToken)
    {
        var host = _mill.ResolveTcpOpenHost()
            ?? throw new InvalidOperationException($"{_mill.Name}: TCP open-comm host is not configured.");

        var port = _mill.ResolveTcpOpenPort();
        if (port <= 0)
            throw new InvalidOperationException($"{_mill.Name}: TCP open-comm port is not configured.");

        var connectTimeoutMs = Math.Max(1000, _mill.TcpOpenConnectTimeoutMs);

        _client = new TcpClient();
        using var connectCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        connectCts.CancelAfter(connectTimeoutMs);

        try
        {
            await _client.ConnectAsync(host, port, connectCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(
                $"{_mill.Name}: TCP open-comm connect to {host}:{port} timed out after {connectTimeoutMs}ms.");
        }

        var stream = _client.GetStream();
        var receiveTimeoutMs = _mill.TcpOpenReceiveTimeoutMs;
        if (receiveTimeoutMs > 0)
            stream.ReadTimeout = receiveTimeoutMs;

        lock (_streamLock)
        {
            _stream = stream;
        }
    }

    public Task DisconnectAsync(CancellationToken cancellationToken)
    {
        lock (_streamLock)
        {
            CloseClientUnsafe();
        }

        return Task.CompletedTask;
    }

    public async Task SendAckAsync(byte ackValue, CancellationToken cancellationToken)
    {
        await _sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var ack = Mill4MessageCodec.BuildAckMessage(ackValue);
            NetworkStream? stream;
            lock (_streamLock)
                stream = _stream;

            if (stream is null)
                throw new InvalidOperationException($"{_mill.Name}: TCP stream is not connected.");

            await stream.WriteAsync(ack, cancellationToken).ConfigureAwait(false);
            await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    public async Task RunReadLoopAsync(CancellationToken cancellationToken)
    {
        var frameBuffer = new byte[Mill4MessageCodec.MinimumFrameLength];
        var millNo = _mill.ResolveMillNo();

        while (!cancellationToken.IsCancellationRequested)
        {
            NetworkStream? stream;
            lock (_streamLock)
                stream = _stream;

            if (stream is null)
                break;

            int totalRead;
            try
            {
                totalRead = await ReadExactFrameAsync(stream, frameBuffer, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (IOException ex)
            {
                _logger.LogWarning(
                    ex,
                    "TCP read failed — Mill {MillNo}, PoEndSource=TcpOpen, endpoint {Endpoint}.",
                    millNo,
                    FormatEndpoint());
                return;
            }

            if (totalRead == 0)
            {
                _logger.LogWarning(
                    "TCP peer closed connection — Mill {MillNo}, PoEndSource=TcpOpen, endpoint {Endpoint}.",
                    millNo,
                    FormatEndpoint());
                return;
            }

            var parseResult = Mill4MessageCodec.ParseTriggerMessage(frameBuffer, totalRead);
            if (!parseResult.Success)
            {
                _logger.LogWarning(
                    "TCP frame parse failed — Mill {MillNo}, PoEndSource=TcpOpen, reason {Reason}, bytes {BytesRead}.",
                    millNo,
                    parseResult.FailureReason,
                    totalRead);
                continue;
            }

            var parsed = parseResult.Message!;
            if (!parsed.TriggerActive)
                continue;

            var correlationId = Guid.NewGuid();
            var payload = frameBuffer.AsSpan(0, totalRead).ToArray();
            var message = new MillTcpPoEndMessage(
                millNo,
                parsed.PoTypeId,
                parsed.TriggerActive,
                payload,
                DateTimeOffset.UtcNow,
                correlationId);

            using (LogContext.PushProperty("CorrelationId", correlationId))
            {
                _logger.LogInformation(
                    "TCP trigger received — Mill {MillNo}, PoEndSource=TcpOpen, PO_Type_ID {PoTypeId}, CorrelationId {CorrelationId}.",
                    millNo,
                    parsed.PoTypeId,
                    correlationId);

                if (OnTriggerMessageAsync is not null)
                    await OnTriggerMessageAsync(message, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static async Task<int> ReadExactFrameAsync(
        NetworkStream stream,
        byte[] buffer,
        CancellationToken cancellationToken)
    {
        var totalRead = 0;
        while (totalRead < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer.AsMemory(totalRead, buffer.Length - totalRead), cancellationToken)
                .ConfigureAwait(false);
            if (read == 0)
                return totalRead == 0 ? 0 : totalRead;

            totalRead += read;
        }

        return totalRead;
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await DisconnectAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "{MillName}: error during TCP transport dispose.", _mill.Name);
        }

        try
        {
            _client?.Dispose();
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "{MillName}: error disposing TCP client.", _mill.Name);
        }

        _sendLock.Dispose();
    }

    private void CloseClientUnsafe()
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

    private string FormatEndpoint()
    {
        var host = _mill.ResolveTcpOpenHost() ?? "?";
        var port = _mill.ResolveTcpOpenPort();
        return $"{host}:{port}";
    }
}
