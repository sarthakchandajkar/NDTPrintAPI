using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services.PlcHandshake.S7;

/// <summary>
/// Creates at most one <see cref="IS7ConnectionProvider"/> per mill.
/// Pre-registers mills with <c>PlcHandshakeEnabled</c> so NDT count reads can share the handshake connection.
/// </summary>
public sealed class S7ConnectionProviderRegistry : IS7ConnectionProviderRegistry, IAsyncDisposable
{
    private readonly IOptions<NdtBundleOptions> _options;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ConcurrentDictionary<int, IS7ConnectionProvider> _byMill = new();
    private readonly object _createLock = new();

    public S7ConnectionProviderRegistry(
        IOptions<NdtBundleOptions> options,
        ILoggerFactory loggerFactory)
    {
        _options = options;
        _loggerFactory = loggerFactory;
        PreRegisterFromConfig();
    }

    public IS7ConnectionProvider? TryGet(int millNo)
    {
        if (millNo is < 1 or > 4)
            return null;
        return _byMill.TryGetValue(millNo, out var provider) ? provider : null;
    }

    public IS7ConnectionProvider GetOrCreate(MillConfig mill, PlcHandshakeOptions options)
    {
        var millNo = mill.ResolveMillNo();
        if (millNo is < 1 or > 4)
            throw new ArgumentOutOfRangeException(nameof(mill), millNo, "MillNo must be 1..4.");

        if (_byMill.TryGetValue(millNo, out var existing))
            return existing;

        lock (_createLock)
        {
            if (_byMill.TryGetValue(millNo, out existing))
                return existing;

            var provider = new S7ConnectionProvider(
                mill,
                options,
                _loggerFactory.CreateLogger<S7ConnectionProvider>());
            _byMill[millNo] = provider;
            return provider;
        }
    }

    private void PreRegisterFromConfig()
    {
        var handshake = _options.Value.PlcHandshake ?? new PlcHandshakeOptions();
        if (!handshake.Enabled)
            return;

        foreach (var mill in handshake.Mills)
        {
            if (!mill.PlcHandshakeEnabled || string.IsNullOrWhiteSpace(mill.IpAddress))
                continue;
            if (mill.ResolveMillNo() is < 1 or > 4)
                continue;
            GetOrCreate(mill, handshake);
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var provider in _byMill.Values)
            await provider.DisposeAsync().ConfigureAwait(false);
        _byMill.Clear();
    }
}
