using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class MillBundleStateLockTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(5)]
    public async Task AcquireAsync_invalid_mill_throws(int millNo)
    {
        var sut = new MillBundleStateLock();

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => sut.AcquireAsync(millNo, CancellationToken.None));
    }

    [Fact]
    public async Task AcquireAsync_same_mill_serializes_concurrent_callers()
    {
        var sut = new MillBundleStateLock();
        var firstEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseFirst = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondAcquired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var first = Task.Run(async () =>
        {
            var handle = await sut.AcquireAsync(2, CancellationToken.None);
            try
            {
                firstEntered.SetResult();
                await releaseFirst.Task;
            }
            finally
            {
                handle.Dispose();
            }
        });

        await firstEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var second = Task.Run(async () =>
        {
            var handle = await sut.AcquireAsync(2, CancellationToken.None);
            try
            {
                secondAcquired.SetResult();
            }
            finally
            {
                handle.Dispose();
            }
        });

        await Task.Delay(50);
        Assert.False(secondAcquired.Task.IsCompleted);

        releaseFirst.SetResult();
        await secondAcquired.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await first;
        await second;
    }

    [Fact]
    public async Task AcquireAsync_different_mills_do_not_block_each_other()
    {
        var sut = new MillBundleStateLock();
        var mill1Acquired = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseMill1 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var mill1 = Task.Run(async () =>
        {
            var handle = await sut.AcquireAsync(1, CancellationToken.None);
            try
            {
                mill1Acquired.SetResult();
                await releaseMill1.Task;
            }
            finally
            {
                handle.Dispose();
            }
        });

        await mill1Acquired.Task.WaitAsync(TimeSpan.FromSeconds(5));

        var mill2Handle = await sut.AcquireAsync(2, CancellationToken.None);
        try
        {
            Assert.NotNull(mill2Handle);
        }
        finally
        {
            mill2Handle.Dispose();
        }

        releaseMill1.SetResult();
        await mill1;
    }
}
