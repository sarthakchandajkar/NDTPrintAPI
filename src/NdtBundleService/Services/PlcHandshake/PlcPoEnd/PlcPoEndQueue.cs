using System.Threading.Channels;

namespace NdtBundleService.Services.PlcHandshake.PlcPoEnd;

/// <summary>Thread-safe queue for PLC PO-end events (one in-flight per mill).</summary>
public sealed class PlcPoEndQueue
{
    private readonly Channel<PlcPoEndRequest> _channel =
        Channel.CreateUnbounded<PlcPoEndRequest>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

    private readonly HashSet<int> _queuedOrProcessingMills = new();
    private readonly object _sync = new();

    public bool TryEnqueue(PlcPoEndRequest request)
    {
        if (request.MillNo is < 1 or > 4)
            return false;

        lock (_sync)
        {
            if (!_queuedOrProcessingMills.Add(request.MillNo))
                return false;
        }

        if (!_channel.Writer.TryWrite(request))
        {
            lock (_sync)
                _queuedOrProcessingMills.Remove(request.MillNo);
            return false;
        }

        return true;
    }

    public ChannelReader<PlcPoEndRequest> Reader => _channel.Reader;

    public void MarkCompleted(int millNo)
    {
        if (millNo is < 1 or > 4)
            return;

        lock (_sync)
            _queuedOrProcessingMills.Remove(millNo);
    }
}
