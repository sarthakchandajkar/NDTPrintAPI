using Microsoft.Extensions.Logging;

namespace NdtBundleService.Services;

/// <summary>
/// PLC client used when Modbus is disabled or for tests: all mills read PO-end false.
/// </summary>
public sealed class StubPlcClient : IPlcClient
{
    private static readonly IReadOnlyDictionary<int, bool> AllFalse =
        new Dictionary<int, bool> { [1] = false, [2] = false, [3] = false, [4] = false };

    private readonly ILogger<StubPlcClient> _logger;

    public StubPlcClient(ILogger<StubPlcClient> logger)
    {
        _logger = logger;
    }

    public Task<IReadOnlyDictionary<int, bool>> GetPoEndSignalsByMillAsync(CancellationToken cancellationToken)
    {
        _logger.LogDebug("StubPlcClient: PO-end signals all false.");
        return Task.FromResult(AllFalse);
    }

    public Task<bool> GetPoEndAsync(CancellationToken cancellationToken) => Task.FromResult(false);

    public Task<IReadOnlyDictionary<int, MillPoPlcSnapshot>?> ReadMillPoSnapshotsAsync(CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyDictionary<int, MillPoPlcSnapshot>?>(null);

    public Task AcknowledgeMesPoChangeAsync(int millNo, CancellationToken cancellationToken) => Task.CompletedTask;
}
