namespace NdtBundleService.Services.PlcHandshake;

/// <summary>
/// Exposes handshake trigger-bit state for dashboard status without opening extra S7 connections.
/// </summary>
public sealed class PlcHandshakeMirrorPlcClient : IPlcClient
{
    private readonly PlcHandshakeStatusRegistry _registry;

    public PlcHandshakeMirrorPlcClient(PlcHandshakeStatusRegistry registry)
    {
        _registry = registry;
    }

    public Task<IReadOnlyDictionary<int, bool>> GetPoEndSignalsByMillAsync(CancellationToken cancellationToken) =>
        Task.FromResult(_registry.GetPoEndByMill());

    public Task<bool> GetPoEndAsync(CancellationToken cancellationToken)
    {
        var map = _registry.GetPoEndByMill();
        return Task.FromResult(map.Values.Any(v => v));
    }

    public Task<IReadOnlyDictionary<int, MillPoPlcSnapshot>?> ReadMillPoSnapshotsAsync(CancellationToken cancellationToken) =>
        Task.FromResult<IReadOnlyDictionary<int, MillPoPlcSnapshot>?>(null);

    public Task AcknowledgeMesPoChangeAsync(int millNo, CancellationToken cancellationToken) =>
        Task.CompletedTask;
}
