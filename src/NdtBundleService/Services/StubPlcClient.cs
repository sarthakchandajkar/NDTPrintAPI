using Microsoft.Extensions.Logging;

namespace NdtBundleService.Services;

/// <summary>
/// Stub PLC client that always reports PO-end as false.
/// Replace with a real S7 implementation when PLC details are available.
/// </summary>
public sealed class StubPlcClient : IPlcClient
{
    private readonly ILogger<StubPlcClient> _logger;

    public StubPlcClient(ILogger<StubPlcClient> logger)
    {
        _logger = logger;
    }

    public Task<bool> GetPoEndAsync(CancellationToken cancellationToken)
    {
        // TODO: Implement actual S7 communication and return PO end bit.
        _logger.LogDebug("StubPlcClient.GetPoEndAsync called; always returning false.");
        return Task.FromResult(false);
    }
}

