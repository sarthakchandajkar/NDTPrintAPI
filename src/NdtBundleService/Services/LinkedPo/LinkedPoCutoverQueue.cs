using System.Threading.Channels;

namespace NdtBundleService.Services.LinkedPo;

/// <summary>One in-flight linked-PO cutover per mill.</summary>
public sealed class LinkedPoCutoverQueue
{
    private readonly Channel<LinkedPoCutoverRequest> _channel =
        Channel.CreateUnbounded<LinkedPoCutoverRequest>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

    private readonly HashSet<int> _queuedOrProcessingMills = new();
    private readonly object _sync = new();

    public bool TryEnqueue(LinkedPoCutoverRequest request)
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

    public ChannelReader<LinkedPoCutoverRequest> Reader => _channel.Reader;

    public void MarkCompleted(int millNo)
    {
        if (millNo is < 1 or > 4)
            return;
        lock (_sync)
            _queuedOrProcessingMills.Remove(millNo);
    }
}
