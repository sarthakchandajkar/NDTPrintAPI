using NdtBundleService.Services.LinkedPo;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class LinkedPoCutoverQueueTests
{
    [Fact]
    public void TryEnqueue_one_in_flight_per_mill()
    {
        var q = new LinkedPoCutoverQueue();
        Assert.True(q.TryEnqueue(new LinkedPoCutoverRequest
        {
            MillNo = 1,
            EndedPo = "1000062750",
            NewPo = "1000062751"
        }));
        Assert.False(q.TryEnqueue(new LinkedPoCutoverRequest
        {
            MillNo = 1,
            EndedPo = "1000062750",
            NewPo = "1000062751"
        }));
        Assert.True(q.TryEnqueue(new LinkedPoCutoverRequest
        {
            MillNo = 2,
            EndedPo = "1000062670",
            NewPo = "1000062671"
        }));

        q.MarkCompleted(1);
        Assert.True(q.TryEnqueue(new LinkedPoCutoverRequest
        {
            MillNo = 1,
            EndedPo = "1000062750",
            NewPo = "1000062751"
        }));
    }
}
