using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.PlcHandshake.PlcPoEnd;
using Serilog.Context;

namespace NdtBundleService.Services.TcpOpenComm;

/// <summary>
/// One TCP open-communication client per mill with <c>PoEndSource=TcpOpen</c>.
/// Ack on wire must succeed before enqueueing onto the shared <see cref="PlcPoEndQueue"/>.
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
            _logger.LogInformation("No TcpOpen mills configured — TCP transport idle.");
            return;
        }

        _logger.LogInformation(
            "MillTcpOpenCommWorker started for {Count} TcpOpen mill(s): {Mills}.",
            tcpMills.Count,
            string.Join(", ", tcpMills.Select(m => m.Name)));

        var tasks = tcpMills.Select(mill => RunMillLoopAsync(mill, handshake, stoppingToken)).ToList();
        await Task.WhenAll(tasks).ConfigureAwait(false);
    }

    private async Task RunMillLoopAsync(MillConfig mill, PlcHandshakeOptions handshake, CancellationToken stoppingToken)
    {
        var millNo = mill.ResolveMillNo();
        var host = mill.ResolveTcpOpenHost();
        var port = mill.ResolveTcpOpenPort();

        if (string.IsNullOrWhiteSpace(host) || port <= 0)
        {
            _logger.LogWarning(
                "TCP transport skipped — Mill {MillNo}, PoEndSource=TcpOpen: host/port not configured.",
                millNo);
            return;
        }

        var endpoint = $"{host}:{port}";
        var reconnect = new TcpOpenCommReconnect(handshake, millNo, endpoint, _logger);

        while (!stoppingToken.IsCancellationRequested)
        {
            await using var transport = new MillTcpOpenCommTransport(mill, _logger);
            transport.OnTriggerMessageAsync = (message, ct) =>
                HandlePoEndMessageAsync(message, mill, transport, _plcPoEndQueue, _logger, ct);

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
                    "TCP connection or read loop failed — Mill {MillNo}, PoEndSource=TcpOpen, endpoint {Endpoint}.",
                    millNo,
                    endpoint);
            }

            try
            {
                await transport.DisconnectAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogDebug(
                    ex,
                    "TCP disconnect after failure — Mill {MillNo}, PoEndSource=TcpOpen.",
                    millNo);
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

    /// <summary>
    /// Ack on wire first; enqueue only after successful ack (PLC M41.7 / AG_RECV path).
    /// PO resolution uses the same <see cref="PlcPoEndRequest.PoId"/> path as S7 Phase 1.
    /// </summary>
    public static async Task HandlePoEndMessageAsync(
        MillTcpPoEndMessage message,
        MillConfig mill,
        IMillTcpTransport transport,
        PlcPoEndQueue queue,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        if (!message.TriggerActive)
            return;

        var millNo = mill.ResolveMillNo();
        var correlationId = message.CorrelationId;
        var triggerAddress = $"TcpOpen:{mill.ResolveTcpOpenHost()}:{mill.ResolveTcpOpenPort()}";

        using (LogContext.PushProperty("CorrelationId", correlationId))
        {
            try
            {
                await transport.SendAckAsync(0x01, cancellationToken).ConfigureAwait(false);
                logger.LogInformation(
                    "TCP ack sent — Mill {MillNo}, PoEndSource=TcpOpen, PO_Type_ID {PoTypeId}, CorrelationId {CorrelationId}.",
                    millNo,
                    message.PoTypeId,
                    correlationId);
            }
            catch (Exception ex)
            {
                logger.LogError(
                    ex,
                    "TCP ack failed — Mill {MillNo}, PoEndSource=TcpOpen, PO_Type_ID {PoTypeId}, CorrelationId {CorrelationId}. PO end will not be enqueued.",
                    millNo,
                    message.PoTypeId,
                    correlationId);
                return;
            }

            var request = new PlcPoEndRequest
            {
                MillNo = millNo,
                PoId = message.PoTypeId,
                NdtCountFinal = 0,
                CorrelationId = correlationId,
                DetectedAtUtc = message.ReceivedAtUtc,
                StartupRecovery = false
            };

            if (queue.TryEnqueue(request))
            {
                logger.LogInformation(
                    "TCP PO end enqueued — Mill {MillNo}, PoEndSource=TcpOpen, trigger {Trigger}, PO_Type_ID {PoTypeId}, CorrelationId {CorrelationId}.",
                    millNo,
                    triggerAddress,
                    message.PoTypeId,
                    correlationId);
            }
            else
            {
                logger.LogWarning(
                    "TCP PO end not enqueued (mill {MillNo} already queued or processing) — PoEndSource=TcpOpen, PO_Type_ID {PoTypeId}, CorrelationId {CorrelationId}.",
                    millNo,
                    message.PoTypeId,
                    correlationId);
            }
        }
    }
}
