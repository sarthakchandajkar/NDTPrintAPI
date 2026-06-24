using NdtBundleService.Services.FileBasedPoChange;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class FileBasedPoChangeQueueTests
{
    [Fact]
    public void TryEnqueue_deduplicates_same_mill_until_completed()
    {
        var queue = new FileBasedPoChangeQueue();
        var first = new FileBasedPoChangeRequest
        {
            MillNo = 4,
            EndedPo = "1000057001",
            NewPo = "1000057002",
            WipFileName = "WIP_04_1000057002_1.csv"
        };
        var second = new FileBasedPoChangeRequest
        {
            MillNo = 4,
            EndedPo = "1000057003",
            NewPo = "1000057004",
            WipFileName = "WIP_04_1000057004_1.csv"
        };

        Assert.True(queue.TryEnqueue(first));
        Assert.False(queue.TryEnqueue(second));

        queue.MarkCompleted(4);
        Assert.True(queue.TryEnqueue(second));
    }

    [Fact]
    public void TryEnqueue_allows_different_mills_concurrently()
    {
        var queue = new FileBasedPoChangeQueue();

        Assert.True(queue.TryEnqueue(new FileBasedPoChangeRequest { MillNo = 1, NewPo = "1001" }));
        Assert.True(queue.TryEnqueue(new FileBasedPoChangeRequest { MillNo = 2, NewPo = "1002" }));
    }
}
