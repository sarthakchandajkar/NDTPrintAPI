using NdtBundleService.Services.TcpOpenComm;
using Xunit;

namespace NdtBundleService.Tests.TcpOpenComm;

public sealed class Mill4MessageCodecTests
{
    [Fact]
    public void ParseTriggerMessage_parses_big_endian_po_id_and_trigger()
    {
        var buffer = new byte[] { 0x03, 0xE9, 0x01 };

        var result = Mill4MessageCodec.ParseTriggerMessage(buffer, buffer.Length);

        Assert.True(result.Success);
        Assert.NotNull(result.Message);
        Assert.Equal((ushort)1001, result.Message!.PoTypeId);
        Assert.True(result.Message.TriggerActive);
    }

    [Fact]
    public void ParseTriggerMessage_returns_failure_for_short_buffer()
    {
        var buffer = new byte[] { 0x03, 0xE9 };

        var result = Mill4MessageCodec.ParseTriggerMessage(buffer, buffer.Length);

        Assert.False(result.Success);
        Assert.Null(result.Message);
        Assert.Contains("too short", result.FailureReason, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ParseTriggerMessage_trigger_inactive_when_zero()
    {
        var buffer = new byte[] { 0x00, 0x01, 0x00 };

        var result = Mill4MessageCodec.ParseTriggerMessage(buffer, buffer.Length);

        Assert.True(result.Success);
        Assert.False(result.Message!.TriggerActive);
    }

    [Fact]
    public void BuildAckMessage_returns_single_byte()
    {
        var ack = Mill4MessageCodec.BuildAckMessage(0x01);

        Assert.Single(ack);
        Assert.Equal(0x01, ack[0]);
    }

    [Fact]
    public void MinimumFrameLength_is_single_source_of_truth()
    {
        Assert.Equal(3, Mill4MessageCodec.MinimumFrameLength);
    }
}
