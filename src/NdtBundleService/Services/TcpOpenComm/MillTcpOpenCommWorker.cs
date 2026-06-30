using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.PlcHandshake.PlcPoEnd;

namespace NdtBundleService.Services.TcpOpenComm;

/// <summary>
/// One TCP open-communication client per mill with <c>PoEndSource=TcpOpen</c>.
/// Acknowledges PO-end on the wire then enqueues via Phase 1 <see cref="PlcPoEndEdgeProcessor"/>.
/// </summary>
public sealed class MillTcpOpenCommWorker : BackgroundService
{
    private readonly IOptions<NdtBundleOptions> _options;
    private readonly PlcPoEndQueue _plcPoEndQueue;
    private readonly ILogger<MillTcpOpenCommWorker> _logger;

    public MillTcpOpenCommWorker(
        IOptions<NdtBundleOptions> options,
        PlcPoEndQueue plcPoEndQueue,
        ILogger<MillTcpOpenCommWorker> logger)
    {
        _options = options;
        _plcPoEndQueue = plcPoEndQueue;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var handshake = _options.Value.PlcHandshake ?? new PlcHandshakeOptions();
        var tcpMills = handshake.Mills
            .Where(m => m.ResolvePoEndSource(_options.Value) == MillPoEndSource.TcpOpen)
            .ToList();

        if (tcpMills.Count == 0)
        {
            _logger.LogInformation("No mill has PoEndSource=TcpOpen; MillTcpOpenCommWorker idle.");
            return;
        }

        _logger.LogInformation(
            "MillTcpOpenCommWorker started for {Count} mill(s): {Mills}.",
            tcpMills.Count,
            string.Join(", ", tcpMills.Select(m => m.Name)));

        var tasks = tcpMills.Select(mill => RunMillLoopAsync(mill, handshake, stoppingToken)).ToList();
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    private async Task RunMillLoopAsync(MillConfig mill, PlcHandshakeOptions handshake, CancellationToken stoppingToken)
    {
        if (string.IsNullOrWhiteSpace(mill.TcpOpenCommHost) || mill.TcpOpenCommPort <= 0)
        {
            _logger.LogWarning(
                "{MillName}: PoEndSource=TcpOpen but TcpOpenCommHost/TcpOpenCommPort not configured; skipping.",
                mill.Name);
            return;
        }

        var endpoint = $"{mill.TcpOpenCommHost}:{mill.TcpOpenCommPort}";
        var reconnect = new TcpOpenCommReconnect(handshake, mill.Name, endpoint, _logger);

        while (!stoppingToken.IsCancellationRequested)
        {
            await using var transport = new MillTcpOpenCommTransport(mill, _logger);
            transport.PoEndMessageReceived += message =>
                HandlePoEndMessage(message, mill, transport, _plcPoEndQueue, _logger, stoppingToken);

            try
            {
                await transport.ConnectAsync(stoppingToken).ConfigureAwait(false);
                reconnect.Reset();
                await transport.RunReadLoopAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "{MillName}: TCP open-comm connection or read loop failed ({Endpoint}).",
                    mill.Name,
                    endpoint);
            }

            try
            {
                await transport.DisconnectAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "{MillName}: disconnect after TCP failure.", mill.Name);
            }

            if (stoppingToken.IsCancellationRequested)
                break;

            try
            {
                await reconnect.DelayAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }
    }

    /// <summary>Testable handler: ack on wire then enqueue PO-end workflow (Phase 1 path).</summary>
    public static void HandlePoEndMessage(
        MillTcpPoEndMessage message,
        MillConfig mill,
        IMillTcpTransport transport,
        PlcPoEndQueue queue,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        if (!message.TriggerActive)
            return;

        var correlationId = Guid.NewGuid();
        var millNo = mill.ResolveMillNo();
        var triggerAddress = $"TcpOpen:{mill.TcpOpenCommHost}:{mill.TcpOpenCommPort}";

        PlcPoEndEdgeProcessor.ProcessDecoupledEdge(
            new PlcPoEndEdgeProcessor.EdgeProcessInput(
                millNo,
                mill.Name,
                message.PoTypeId,
                NdtCountFinal: 0,
                correlationId,
                message.ReceivedAtUtc,
                StartupRecovery: false,
                triggerAddress),
            beginAckTrue: () => _ = SendAckFireAndForgetAsync(transport, mill.Name, correlationId, logger, cancellationToken),
            tryEnqueue: queue.TryEnqueue,
            logger);
    }

    private static async Task SendAckFireAndForgetAsync(
        IMillTcpTransport transport,
        string millName,
        Guid correlationId,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        try
        {
            await transport.SendAckAsync(0x01, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(
                ex,
                "{MillName}: failed to send TCP PO-end ack — CorrelationId {CorrelationId}.",
                millName,
                correlationId);
        }
    }
}
