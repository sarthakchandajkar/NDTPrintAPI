// =============================================================================
// TBD — PLACEHOLDER wire layout pending controls engineer confirmation of
// AG_SEND/AG_RECV ladder byte layout. Change ONLY this file when finalized.
// =============================================================================

namespace NdtBundleService.Services.TcpOpenComm;

/// <summary>
/// Single place for Mill-4 TCP open-communication message encoding/decoding.
/// Assumption: PLC is server; MES connects as client. Fixed minimum frame length.
/// </summary>
public static class Mill4MessageCodec
{
    public const int MinimumFrameLength = 3;

    public sealed record MillTcpPoEndParsed(ushort PoTypeId, bool TriggerActive);

    public static bool TryParsePoEndMessage(ReadOnlySpan<byte> buffer, out MillTcpPoEndParsed parsed)
    {
        parsed = default!;
        if (buffer.Length < MinimumFrameLength)
            return false;

        var poTypeId = (ushort)((buffer[0] << 8) | buffer[1]);
        var triggerActive = buffer[2] != 0;
        parsed = new MillTcpPoEndParsed(poTypeId, triggerActive);
        return true;
    }

    public static byte[] BuildAck(byte ackValue) => [ackValue];
}
