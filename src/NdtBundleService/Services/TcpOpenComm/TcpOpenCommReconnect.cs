using Microsoft.Extensions.Logging;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services.TcpOpenComm;

/// <summary>Exponential backoff for TCP open-communication reconnect (mirrors S7 handshake pattern).</summary>
public sealed class TcpOpenCommReconnect
{
    private readonly PlcHandshakeOptions _options;
    private readonly ILogger _logger;
    private readonly int _millNo;
    private readonly string _endpoint;
    private int _reconnectDelayMs;

    public TcpOpenCommReconnect(
        PlcHandshakeOptions options,
        int millNo,
        string endpoint,
        ILogger logger)
    {
        _options = options;
        _millNo = millNo;
        _endpoint = endpoint;
        _logger = logger;
        Reset();
    }

    public void Reset() =>
        _reconnectDelayMs = Math.Max(250, _options.InitialReconnectDelayMs);

    public async Task DelayAsync(CancellationToken cancellationToken)
    {
        _logger.LogWarning(
            "TCP reconnecting in {Delay}ms — Mill {MillNo}, PoEndSource=TcpOpen, endpoint {Endpoint}.",
            _reconnectDelayMs,
            _millNo,
            _endpoint);

        await Task.Delay(_reconnectDelayMs, cancellationToken).ConfigureAwait(false);
        _reconnectDelayMs = Math.Min(
            _reconnectDelayMs * 2,
            Math.Max(1000, _options.MaxReconnectDelayMs));
    }
}
