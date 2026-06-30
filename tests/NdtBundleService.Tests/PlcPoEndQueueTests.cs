using NdtBundleService.Services.PlcHandshake.PlcPoEnd;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class PlcPoEndQueueTests
{
    [Fact]
    public void TryEnqueue_deduplicates_same_mill_until_completed()
    {
        var queue = new PlcPoEndQueue();
        var first = new PlcPoEndRequest
        {
            MillNo = 2,
            PoId = 1000059168,
            NdtCountFinal = 42,
            CorrelationId = Guid.NewGuid(),
            DetectedAtUtc = DateTimeOffset.UtcNow
        };
        var second = new PlcPoEndRequest
        {
            MillNo = 2,
            PoId = 1000059169,
            NdtCountFinal = 10,
            CorrelationId = Guid.NewGuid(),
            DetectedAtUtc = DateTimeOffset.UtcNow
        };

        Assert.True(queue.TryEnqueue(first));
        Assert.False(queue.TryEnqueue(second));

        queue.MarkCompleted(2);
        Assert.True(queue.TryEnqueue(second));
    }

    [Fact]
    public void TryEnqueue_allows_different_mills_concurrently()
    {
        var queue = new PlcPoEndQueue();

        Assert.True(queue.TryEnqueue(new PlcPoEndRequest { MillNo = 1, PoId = 1001, CorrelationId = Guid.NewGuid() }));
        Assert.True(queue.TryEnqueue(new PlcPoEndRequest { MillNo = 3, PoId = 1003, CorrelationId = Guid.NewGuid() }));
    }
}
