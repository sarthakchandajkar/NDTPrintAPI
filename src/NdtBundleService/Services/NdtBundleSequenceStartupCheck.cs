using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace NdtBundleService.Services;

/// <summary>Initializes mill bundle sequence counters at startup and logs the resumed sequence per mill.</summary>
public sealed class NdtBundleSequenceStartupCheck : IHostedService
{
    private readonly INdtBundleRuntimeStateStore _runtimeState;
    private readonly ILogger<NdtBundleSequenceStartupCheck> _logger;

    public NdtBundleSequenceStartupCheck(
        INdtBundleRuntimeStateStore runtimeState,
        ILogger<NdtBundleSequenceStartupCheck> logger)
    {
        _runtimeState = runtimeState;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var status = await _runtimeState.GetStatusAsync(cancellationToken).ConfigureAwait(false);

        if (!status.StateFileWritable && status.StateFilePath is not null)
        {
            _logger.LogWarning(
                "NDT bundle runtime state file may not be writable: {Path}. Sequence counters may reset after restart.",
                status.StateFilePath);
        }

        foreach (var mill in Enumerable.Range(1, 4))
        {
            status.BatchOffsetByMill.TryGetValue(mill, out var offset);
            status.EngineBatchNoByMill.TryGetValue(mill, out var engine);
            var seq = Math.Max(offset, engine);
            _logger.LogInformation(
                "Mill-{Mill} NDT bundle sequence at startup: {Seq} (next label {Next}, sources: {Sources}).",
                mill,
                seq,
                seq > 0 ? NdtBundleSequence.Format(seq + 1, mill) : NdtBundleSequence.Format(1, mill),
                status.HydrationSources.Count > 0 ? string.Join(", ", status.HydrationSources) : "none");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
