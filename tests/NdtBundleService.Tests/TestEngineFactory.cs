using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.PlcHandshake.S7;

namespace NdtBundleService.Tests;

internal static class TestEngineFactory
{
    public static NdtBundleService.Services.NdtBundleEngine Create(
        NdtBundleService.Services.IFormationChartProvider formation,
        NdtBundleService.Services.IPipeSizeProvider pipeSize,
        NdtBundleService.Services.INdtBundleRuntimeStateStore runtime,
        string closeTrigger = "File",
        IS7ConnectionProviderRegistry? s7Registry = null)
    {
        var options = Options.Create(new NdtBundleOptions { CloseTrigger = closeTrigger });
        return new NdtBundleService.Services.NdtBundleEngine(
            formation,
            pipeSize,
            runtime,
            options,
            s7Registry ?? new EmptyS7Registry(),
            Microsoft.Extensions.Logging.Abstractions.NullLogger<NdtBundleService.Services.NdtBundleEngine>.Instance);
    }

    private sealed class EmptyS7Registry : IS7ConnectionProviderRegistry
    {
        public IS7ConnectionProvider GetOrCreate(MillConfig mill, PlcHandshakeOptions options) =>
            throw new NotSupportedException();

        public IS7ConnectionProvider? TryGet(int millNo) => null;
    }
}
