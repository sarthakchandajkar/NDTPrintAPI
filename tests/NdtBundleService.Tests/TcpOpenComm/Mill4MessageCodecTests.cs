using NdtBundleService.Services.TcpOpenComm;
using Xunit;

namespace NdtBundleService.Tests.TcpOpenComm;

public sealed class Mill4MessageCodecTests
{
    [Fact]
    public void TryParsePoEndMessage_parses_big_endian_po_id_and_trigger()
    {
        // PO_Type_ID = 1001 (0x03E9), trigger active
        ReadOnlySpan<byte> frame = [0x03, 0xE9, 0x01];

        Assert.True(Mill4MessageCodec.TryParsePoEndMessage(frame, out var parsed));
        Assert.Equal((ushort)1001, parsed.PoTypeId);
        Assert.True(parsed.TriggerActive);
    }

    [Fact]
    public void TryParsePoEndMessage_returns_false_for_short_buffer()
    {
        ReadOnlySpan<byte> frame = [0x03, 0xE9];

        Assert.False(Mill4MessageCodec.TryParsePoEndMessage(frame, out _));
    }

    [Fact]
    public void TryParsePoEndMessage_trigger_inactive_when_zero()
    {
        ReadOnlySpan<byte> frame = [0x00, 0x01, 0x00];

        Assert.True(Mill4MessageCodec.TryParsePoEndMessage(frame, out var parsed));
        Assert.False(parsed.TriggerActive);
    }

    [Fact]
    public void BuildAck_returns_single_byte()
    {
        var ack = Mill4MessageCodec.BuildAck(0x01);

        Assert.Single(ack);
        Assert.Equal(0x01, ack[0]);
    }
}
