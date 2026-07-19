using NdtBundleService.Configuration;

namespace NdtBundleService.Services.PlcHandshake;

/// <summary>Resolves handshake poll interval (keeps 500 ms default behavior testable).</summary>
public static class PlcHandshakePollTiming
{
    public static int ResolvePollIntervalMs(MillConfig mill, PlcHandshakeOptions options)
    {
        if (mill.PollIntervalMs > 0)
            return mill.PollIntervalMs;
        return Math.Max(100, options.PollIntervalMs);
    }
}
