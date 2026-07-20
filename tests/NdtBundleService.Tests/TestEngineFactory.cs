using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using NdtBundleService.Services.PlcHandshake.S7;

namespace NdtBundleService.Tests;

internal static class TestEngineFactory
{
    public static NdtBundleEngine Create(
        IFormationChartProvider formation,
        IPipeSizeProvider pipeSize,
        INdtBundleRuntimeStateStore runtime,
        string closeTrigger = "File",
        IS7ConnectionProviderRegistry? s7Registry = null,
        int plcCloseGraceSeconds = 60,
        TimeProvider? timeProvider = null,
        ILogger<NdtBundleEngine>? logger = null,
        IBundleProvisionalStampCorrector? stampCorrector = null)
    {
        var options = Options.Create(new NdtBundleOptions
        {
            CloseTrigger = closeTrigger,
            PlcCloseGraceSeconds = plcCloseGraceSeconds
        });
        return new NdtBundleEngine(
            formation,
            pipeSize,
            runtime,
            stampCorrector ?? NoOpStampCorrector.Instance,
            options,
            s7Registry ?? new EmptyS7Registry(),
            logger ?? Microsoft.Extensions.Logging.Abstractions.NullLogger<NdtBundleEngine>.Instance,
            timeProvider);
    }

    private sealed class NoOpStampCorrector : IBundleProvisionalStampCorrector
    {
        public static readonly NoOpStampCorrector Instance = new();
        public Task CorrectAsync(
            string poNumber,
            int millNo,
            int provisionalSequence,
            int finalSequence,
            CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class EmptyS7Registry : IS7ConnectionProviderRegistry
    {
        public IS7ConnectionProvider GetOrCreate(MillConfig mill, PlcHandshakeOptions options) =>
            throw new NotSupportedException();

        public IS7ConnectionProvider? TryGet(int millNo) => null;
    }
}
