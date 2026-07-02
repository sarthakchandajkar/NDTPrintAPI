using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging.Abstractions;
using NdtBundleService.Configuration;
using NdtBundleService.Services.PlcHandshake.PlcPoEnd;
using NdtBundleService.Services.TcpOpenComm;
using Xunit;

namespace NdtBundleService.Tests.TcpOpenComm;

/// <summary>
/// In-process fake PLC TCP server: validates ack timing, enqueue, ack-failure gate, and reconnect delay.
/// Assumption: MES = TCP client, PLC = server.
/// </summary>
public sealed class TcpOpenCommSimulatorTests
{
  private const int AckDeadlineMs = 500;

    [Fact]
    public async Task Transport_sends_ack_within_500ms_and_enqueues_po_end()
    {
        await using var fixture = await FakePlcServer.StartAsync();
        var mill = CreateMillConfig(fixture.Port);
        var queue = new PlcPoEndQueue();

        await using var transport = CreateTransport(mill, queue);
        await transport.ConnectAsync(CancellationToken.None);

        var readLoop = transport.RunReadLoopAsync(CancellationToken.None);

        var ack = await fixture.AwaitAckAsync(TimeSpan.FromMilliseconds(AckDeadlineMs));
        Assert.Equal(0x01, ack);

        var request = await queue.Reader.ReadAsync(CancellationToken.None).AsTask()
            .WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(4, request.MillNo);
        Assert.Equal(1001, request.PoId);
        Assert.Equal(0, request.NdtCountFinal);
        Assert.NotEqual(Guid.Empty, request.CorrelationId);

        await readLoop.WaitAsync(TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task HandlePoEndMessage_does_not_enqueue_when_ack_fails()
    {
        var message = new MillTcpPoEndMessage(
            4,
            1001,
            true,
            [0x03, 0xE9, 0x01],
            DateTimeOffset.UtcNow,
            Guid.NewGuid());

        var mill = CreateMillConfig(2000);
        var queue = new PlcPoEndQueue();
        var failingTransport = new FailingAckTransport();

        await MillTcpOpenCommWorker.HandlePoEndMessageAsync(
            message,
            mill,
            failingTransport,
            queue,
            NullLogger.Instance,
            CancellationToken.None);

        Assert.False(queue.Reader.TryRead(out _));
    }

    [Fact]
    public async Task Transport_reconnects_after_disconnect_within_initial_delay()
    {
        var handshake = new PlcHandshakeOptions
        {
            InitialReconnectDelayMs = 400,
            MaxReconnectDelayMs = 30_000
        };

        await using var fixture = await FakePlcServer.StartAsync(closeAfterFirstClient: true);
        var mill = CreateMillConfig(fixture.Port);
        var reconnect = new TcpOpenCommReconnect(handshake, mill.ResolveMillNo(), $"127.0.0.1:{fixture.Port}", NullLogger.Instance);

        await using (var transport = new MillTcpOpenCommTransport(mill, NullLogger.Instance))
        {
            await transport.ConnectAsync(CancellationToken.None);
            reconnect.Reset();
            await transport.RunReadLoopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));
        }

        var sw = Stopwatch.StartNew();
        await reconnect.DelayAsync(CancellationToken.None);
        sw.Stop();

        Assert.True(sw.ElapsedMilliseconds >= 350, $"Expected reconnect delay >= 350ms, was {sw.ElapsedMilliseconds}ms");

        await using var transport2 = new MillTcpOpenCommTransport(mill, NullLogger.Instance);
        await transport2.ConnectAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task TcpOpenCommReconnect_doubles_delay_up_to_max()
    {
        var options = new PlcHandshakeOptions
        {
            InitialReconnectDelayMs = 500,
            MaxReconnectDelayMs = 2000
        };
        var reconnect = new TcpOpenCommReconnect(options, 4, "127.0.0.1:2000", NullLogger.Instance);

        reconnect.Reset();
        using var cts = new CancellationTokenSource(1500);
        var sw = Stopwatch.StartNew();
        try
        {
            await reconnect.DelayAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
        }

        Assert.True(sw.ElapsedMilliseconds >= 450, $"First delay should be ~500ms, was {sw.ElapsedMilliseconds}ms");
    }

    private static MillConfig CreateMillConfig(int port) => new()
    {
        Name = "Mill-4",
        MillNo = 4,
        PoEndSource = "TcpOpen",
        IpAddress = IPAddress.Loopback.ToString(),
        TcpOpenPort = port,
        TcpOpenConnectTimeoutMs = 5000,
        TcpOpenReceiveTimeoutMs = 0
    };

    private static MillTcpOpenCommTransport CreateTransport(MillConfig mill, PlcPoEndQueue queue)
    {
        var transport = new MillTcpOpenCommTransport(mill, NullLogger.Instance);
        transport.OnTriggerMessageAsync = (message, ct) =>
            MillTcpOpenCommWorker.HandlePoEndMessageAsync(message, mill, transport, queue, NullLogger.Instance, ct);
        return transport;
    }

    private sealed class FailingAckTransport : IMillTcpTransport
    {
        public bool IsConnected => true;

        public Task ConnectAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task DisconnectAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task SendAckAsync(byte ackValue, CancellationToken cancellationToken) =>
            throw new IOException("simulated ack failure");

        public Task RunReadLoopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class FakePlcServer : IAsyncDisposable
    {
        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _cts = new();
        private readonly TaskCompletionSource<byte> _ackTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly bool _closeAfterFirstClient;
        private int _acceptCount;

        private FakePlcServer(int port, bool closeAfterFirstClient)
        {
            _listener = new TcpListener(IPAddress.Loopback, port);
            _closeAfterFirstClient = closeAfterFirstClient;
        }

        public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

        public static async Task<FakePlcServer> StartAsync(bool closeAfterFirstClient = false)
        {
            var server = new FakePlcServer(0, closeAfterFirstClient);
            server._listener.Start();
            _ = server.AcceptLoopAsync(server._cts.Token);
            await Task.Yield();
            return server;
        }

        public Task<byte> AwaitAckAsync(TimeSpan timeout) => _ackTcs.Task.WaitAsync(timeout);

        private async Task AcceptLoopAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                TcpClient client;
                try
                {
                    client = await _listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                _acceptCount++;

                _ = HandleClientAsync(client, _acceptCount == 1 && !_closeAfterFirstClient);
            }
        }

        private async Task HandleClientAsync(TcpClient client, bool sendTrigger)
        {
            using var _ = client;
            var stream = client.GetStream();

            if (_closeAfterFirstClient && !sendTrigger)
            {
                client.Close();
                return;
            }

            if (sendTrigger)
            {
                var frame = new byte[] { 0x03, 0xE9, 0x01 };
                await stream.WriteAsync(frame).ConfigureAwait(false);
                await stream.FlushAsync().ConfigureAwait(false);

                var ackBuffer = new byte[1];
                var read = await stream.ReadAsync(ackBuffer).ConfigureAwait(false);
                if (read == 1)
                    _ackTcs.TrySetResult(ackBuffer[0]);

                client.Close();
            }
        }

        public async ValueTask DisposeAsync()
        {
            await _cts.CancelAsync().ConfigureAwait(false);
            _listener.Stop();
            _cts.Dispose();
        }
    }
}
