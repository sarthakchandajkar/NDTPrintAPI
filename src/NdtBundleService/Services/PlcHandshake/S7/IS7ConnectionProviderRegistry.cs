using NdtBundleService.Configuration;

namespace NdtBundleService.Services.PlcHandshake.S7;

/// <summary>Mill-keyed shared S7 providers (one connection per mill).</summary>
public interface IS7ConnectionProviderRegistry
{
    /// <summary>Existing provider for the mill, or null if none was registered.</summary>
    IS7ConnectionProvider? TryGet(int millNo);

    /// <summary>Get or create the shared provider for this mill config.</summary>
    IS7ConnectionProvider GetOrCreate(MillConfig mill, PlcHandshakeOptions options);
}
