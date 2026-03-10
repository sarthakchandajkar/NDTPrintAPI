using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Sends raw data (e.g. ZPL) to a network printer over TCP. Supports optional local bind address for multi-NIC machines.
/// </summary>
public sealed class NetworkPrinterSender : INetworkPrinterSender
{
    private readonly NdtBundleOptions _options;
    private readonly ILogger<NetworkPrinterSender> _logger;

    public NetworkPrinterSender(IOptions<NdtBundleOptions> options, ILogger<NetworkPrinterSender> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<bool> SendAsync(string host, int port, byte[] data, CancellationToken cancellationToken = default)
    {
        var bindAddress = (_options.NdtTagPrinterLocalBindAddress ?? "").Trim();
        _logger.LogInformation("Connecting to printer at {Host}:{Port} (local bind: {Bind}), sending {Bytes} bytes.",
            host, port, string.IsNullOrEmpty(bindAddress) ? "any" : bindAddress, data.Length);

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(10));

            TcpClient client;
            if (!string.IsNullOrEmpty(bindAddress) && System.Net.IPAddress.TryParse(bindAddress, out var localIp))
            {
                client = new TcpClient(new System.Net.IPEndPoint(localIp, 0));
            }
            else
            {
                client = new TcpClient();
            }

            using (client)
            {
                await client.ConnectAsync(host, port, cts.Token).ConfigureAwait(false);
                _logger.LogDebug("TCP connected, sending {Bytes} bytes.", data.Length);
                await using var stream = client.GetStream();
                await stream.WriteAsync(data, cts.Token).ConfigureAwait(false);
                await stream.FlushAsync(cts.Token).ConfigureAwait(false);
                await Task.Delay(200, CancellationToken.None).ConfigureAwait(false);
            }
            _logger.LogInformation("Sent {Bytes} bytes to {Host}:{Port}.", data.Length, host, port);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Printer send failed to {Host}:{Port}. Error: {Message}", host, port, ex.Message);
            return false;
        }
    }
}
