namespace NdtBundleService.Services.PlcHandshake;

/// <summary>Registry of running per-mill handshake loops (for settings tests without a second S7 client).</summary>
public sealed class PlcHandshakeCoordinator
{
    private readonly object _sync = new();
    private readonly Dictionary<int, PlcHandshakeService> _byMill = new();

    public void Register(PlcHandshakeService service, int millNo)
    {
        if (millNo is < 1 or > 4)
            return;
        lock (_sync)
            _byMill[millNo] = service;
    }

    public bool IsMillRegistered(int millNo)
    {
        lock (_sync)
            return _byMill.ContainsKey(millNo);
    }

    public async Task<PlcPoChangeTestResult> RunSettingsTestAsync(int millNo, CancellationToken cancellationToken)
    {
        PlcHandshakeService? service;
        lock (_sync)
        {
            if (!_byMill.TryGetValue(millNo, out service))
            {
                return new PlcPoChangeTestResult
                {
                    Success = false,
                    MillNo = millNo,
                    Message = "Handshake loop for this mill is not running. Ensure PlcHandshake.Enabled is true and restart NdtBundleService."
                };
            }
        }

        return await service.RunSettingsTestAsync(cancellationToken).ConfigureAwait(false);
    }
}
