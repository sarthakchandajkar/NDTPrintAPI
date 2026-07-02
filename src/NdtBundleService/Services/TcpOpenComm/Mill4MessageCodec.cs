// =============================================================================
// Mill-4 AG_SEND / AG_RECV wire layout — CHANGE ONLY THIS FILE when the controls
// engineer finalizes the protocol.
//
// TODO (controls engineer confirmation required):
//   - Byte 0-1: PO_Type_ID from DB50.DBW10 (UInt16, S7 big-endian)
//   - Byte 2:   Trigger flag 0x01 = active (M41.6 latch via AG_SEND)
//   - Ack byte: 0x01 = MES processed PO change (AG_RECV drives M41.7)
//   - Ack byte: 0x00 = nack/clear (reserved)
//   - Additional trigger bytes (NDT count, Slit_No, etc.) — extend frame when confirmed
// =============================================================================

namespace NdtBundleService.Services.TcpOpenComm;

/// <summary>
/// Single place for Mill-4 TCP open-communication message encoding/decoding.
/// </summary>
public static class Mill4MessageCodec
{
    /// <summary>Minimum bytes required before <see cref="ParseTriggerMessage"/> can succeed.</summary>
    public const int MinimumFrameLength = 3;

    public sealed record Mill4TriggerMessage(ushort PoTypeId, bool TriggerActive);

    public sealed record Mill4TriggerParseResult(bool Success, Mill4TriggerMessage? Message, string? FailureReason)
    {
        public static Mill4TriggerParseResult Ok(Mill4TriggerMessage message) =>
            new(true, message, null);

        public static Mill4TriggerParseResult Fail(string reason) =>
            new(false, null, reason);
    }

    /// <summary>
    /// Parses an inbound trigger frame. Returns a failure result (never throws) when
    /// <paramref name="bytesRead"/> is below <see cref="MinimumFrameLength"/>.
    /// </summary>
    public static Mill4TriggerParseResult ParseTriggerMessage(byte[] buffer, int bytesRead)
    {
        if (buffer is null)
            return Mill4TriggerParseResult.Fail("buffer is null");

        if (bytesRead < MinimumFrameLength)
            return Mill4TriggerParseResult.Fail($"frame too short ({bytesRead} < {MinimumFrameLength})");

        // TODO: confirm PO_Type_ID maps to DB50.DBW10 (Slit2) with controls engineer.
        var poTypeId = (ushort)((buffer[0] << 8) | buffer[1]);

        // TODO: confirm trigger byte semantics (0x01 = active) with controls engineer.
        var triggerActive = buffer[2] != 0;

        return Mill4TriggerParseResult.Ok(new Mill4TriggerMessage(poTypeId, triggerActive));
    }

    /// <summary>Builds the outbound ack frame (MES → PLC via AG_RECV).</summary>
    public static byte[] BuildAckMessage(byte ackValue) => [ackValue];

    /// <summary>Backward-compatible parse for callers using <see cref="ReadOnlySpan{T}"/>.</summary>
    public static bool TryParsePoEndMessage(ReadOnlySpan<byte> buffer, out MillTcpPoEndParsed parsed)
    {
        parsed = default!;
        var array = buffer.ToArray();
        var result = ParseTriggerMessage(array, array.Length);
        if (!result.Success || result.Message is null)
            return false;

        parsed = new MillTcpPoEndParsed(result.Message.PoTypeId, result.Message.TriggerActive);
        return true;
    }

    /// <summary>Backward-compatible alias.</summary>
    public static byte[] BuildAck(byte ackValue) => BuildAckMessage(ackValue);

    public sealed record MillTcpPoEndParsed(ushort PoTypeId, bool TriggerActive);
}
