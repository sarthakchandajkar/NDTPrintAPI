using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging.Abstractions;
using NdtBundleService.Configuration;
using NdtBundleService.Services.PlcHandshake.PlcPoEnd;
using NdtBundleService.Services.TcpOpenComm;
using Xunit;

namespace NdtBundleService.Tests.TcpOpenComm;

/// <summary>
/// In-process TCP simulator: PLC server sends PO-end frame; MES client acks and enqueues.
/// Assumption: PLC is TCP server, MES is client (outbound connect).
/// </summary>
public sealed class TcpOpenCommSimulatorTests
{
    [Fact]
    public async Task Transport_receives_po_end_sends_ack_and_enqueues()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var mill = new MillConfig
        {
            Name = "Mill-4",
            MillNo = 4,
            PoEndSource = "TcpOpen",
            TcpOpenCommHost = IPAddress.Loopback.ToString(),
            TcpOpenCommPort = port
        };

        // PO_Type_ID = 1001 (0x03E9), trigger active
        var poEndFrame = new byte[] { 0x03, 0xE9, 0x01 };
        var clientConnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var serverTask = Task.Run(async () =>
        {
            using var serverClient = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
            clientConnected.TrySetResult();
            var stream = serverClient.GetStream();
            await stream.WriteAsync(poEndFrame).ConfigureAwait(false);
            await stream.FlushAsync().ConfigureAwait(false);

            var ackBuffer = new byte[1];
            var read = await stream.ReadAsync(ackBuffer).ConfigureAwait(false);
            Assert.Equal(1, read);
            Assert.Equal(0x01, ackBuffer[0]);
        });

        await using var transport = new MillTcpOpenCommTransport(mill, NullLogger.Instance);
        var queue = new PlcPoEndQueue();

        transport.PoEndMessageReceived += message =>
            MillTcpOpenCommWorker.HandlePoEndMessage(
                message,
                mill,
                transport,
                queue,
                NullLogger.Instance,
                CancellationToken.None);

        await transport.ConnectAsync(CancellationToken.None);
        var readLoop = transport.RunReadLoopAsync(CancellationToken.None);

        await clientConnected.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await serverTask.WaitAsync(TimeSpan.FromSeconds(5));

        var request = await queue.Reader.ReadAsync(CancellationToken.None).AsTask()
            .WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(4, request.MillNo);
        Assert.Equal(1001, request.PoId);
        Assert.Equal(0, request.NdtCountFinal);

        listener.Stop();
        await readLoop.WaitAsync(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task TcpOpenCommReconnect_doubles_delay_up_to_max()
    {
        var options = new PlcHandshakeOptions
        {
            InitialReconnectDelayMs = 500,
            MaxReconnectDelayMs = 2000
        };
        var reconnect = new TcpOpenCommReconnect(options, "Mill-4", "127.0.0.1:2000", NullLogger.Instance);

        reconnect.Reset();
        using var cts = new CancellationTokenSource();

        var delay1 = Task.Run(async () =>
        {
            try
            {
                await reconnect.DelayAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
        });

        await Task.Delay(100, cts.Token);
        cts.Cancel();

        try
        {
            await delay1.WaitAsync(TimeSpan.FromSeconds(2));
        }
        catch (TimeoutException)
        {
        }

        reconnect.Reset();
        cts.Dispose();
        using var cts2 = new CancellationTokenSource(1500);
        var sw = System.Diagnostics.Stopwatch.StartNew();
        try
        {
            await reconnect.DelayAsync(cts2.Token);
        }
        catch (OperationCanceledException)
        {
        }

        Assert.True(sw.ElapsedMilliseconds >= 450, $"First delay should be ~500ms, was {sw.ElapsedMilliseconds}ms");
    }
}
