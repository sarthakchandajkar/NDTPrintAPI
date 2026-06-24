using System.Threading.Channels;

namespace NdtBundleService.Services.FileBasedPoChange;

/// <summary>Thread-safe queue for file-based PO change events (one in-flight per mill).</summary>
public sealed class FileBasedPoChangeQueue
{
    private readonly Channel<FileBasedPoChangeRequest> _channel =
        Channel.CreateUnbounded<FileBasedPoChangeRequest>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

    private readonly HashSet<int> _queuedOrProcessingMills = new();
    private readonly object _sync = new();

    public bool TryEnqueue(FileBasedPoChangeRequest request)
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

    public ChannelReader<FileBasedPoChangeRequest> Reader => _channel.Reader;

    public void MarkCompleted(int millNo)
    {
        if (millNo is < 1 or > 4)
            return;

        lock (_sync)
            _queuedOrProcessingMills.Remove(millNo);
    }
}
