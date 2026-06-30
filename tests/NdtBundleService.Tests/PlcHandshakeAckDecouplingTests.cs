using Microsoft.Extensions.Logging.Abstractions;
using NdtBundleService.Services.PlcHandshake.PlcPoEnd;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class PlcHandshakeAckDecouplingTests
{
  private const int PollIntervalMs = 500;

    [Fact]
    public void AckTrue_fires_immediately_when_workflow_is_queued_not_awaited()
    {
        var edgeTime = DateTimeOffset.UtcNow;
        DateTimeOffset? ackTime = null;
        var workflowStarted = false;
        var correlationId = Guid.NewGuid();

        PlcPoEndEdgeProcessor.ProcessDecoupledEdge(
            new PlcPoEndEdgeProcessor.EdgeProcessInput(
                2,
                "Mill-2",
                1000059168,
                42,
                correlationId,
                edgeTime,
                StartupRecovery: false,
                "M40.6"),
            beginAckTrue: () => ackTime = DateTimeOffset.UtcNow,
            tryEnqueue: _ =>
            {
                workflowStarted = true;
                return true;
            },
            NullLogger.Instance);

        Assert.NotNull(ackTime);
        Assert.True(workflowStarted);
        Assert.True(
            (ackTime!.Value - edgeTime).TotalMilliseconds < PollIntervalMs,
            $"Ack TRUE took {(ackTime.Value - edgeTime).TotalMilliseconds}ms; expected < {PollIntervalMs}ms.");
    }

    [Fact]
    public async Task Slow_queued_workflow_does_not_delay_ack_true()
    {
        var queue = new PlcPoEndQueue();
        var workflowEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseWorkflow = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ackTime = DateTimeOffset.MinValue;
        var edgeTime = DateTimeOffset.UtcNow;
        var correlationId = Guid.NewGuid();

        var worker = Task.Run(async () =>
        {
            await foreach (var request in queue.Reader.ReadAllAsync())
            {
                workflowEntered.TrySetResult();
                await releaseWorkflow.Task.ConfigureAwait(false);
                queue.MarkCompleted(request.MillNo);
                break;
            }
        });

        PlcPoEndEdgeProcessor.ProcessDecoupledEdge(
            new PlcPoEndEdgeProcessor.EdgeProcessInput(
                2,
                "Mill-2",
                1000059168,
                42,
                correlationId,
                edgeTime,
                StartupRecovery: false,
                "M40.6"),
            beginAckTrue: () => ackTime = DateTimeOffset.UtcNow,
            tryEnqueue: queue.TryEnqueue,
            NullLogger.Instance);

        await workflowEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(
            (ackTime - edgeTime).TotalMilliseconds < PollIntervalMs,
            $"Ack TRUE took {(ackTime - edgeTime).TotalMilliseconds}ms while workflow was blocked.");

        releaseWorkflow.TrySetResult();
        await worker.WaitAsync(TimeSpan.FromSeconds(5));
    }
}
