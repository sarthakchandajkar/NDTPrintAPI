using NdtBundleService.Services.PlcHandshake;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class PoChangeLadderDiagnosticsTrackerTests
{
    [Fact]
    public void First_sample_always_logs_as_initial()
    {
        var t = new PoChangeLadderDiagnosticsTracker(30_000f);
        var snap = Sample(slit1: 9, slit2: 9, md232: 0f);
        Assert.True(t.Observe(snap, out var reason));
        Assert.Equal("initial", reason);
    }

    [Fact]
    public void Md232_chatter_below_threshold_does_not_log()
    {
        var t = new PoChangeLadderDiagnosticsTracker(30_000f);
        Assert.True(t.Observe(Sample(md232: 100f), out _));
        Assert.False(t.Observe(Sample(md232: 5000f), out _));
        Assert.False(t.Observe(Sample(md232: 29_999f), out _));
    }

    [Fact]
    public void Md232_crossing_threshold_up_and_down_logs()
    {
        var t = new PoChangeLadderDiagnosticsTracker(30_000f);
        Assert.True(t.Observe(Sample(md232: 29_000f), out _));

        Assert.True(t.Observe(Sample(md232: 30_001f), out var up));
        Assert.Contains("MD232-cross-up", up);

        Assert.True(t.Observe(Sample(md232: 29_000f), out var down));
        Assert.Contains("MD232-cross-down", down);
    }

    [Fact]
    public void Md232_reset_to_zero_logs()
    {
        var t = new PoChangeLadderDiagnosticsTracker(30_000f);
        Assert.True(t.Observe(Sample(md232: 12_345f), out _));
        Assert.True(t.Observe(Sample(md232: 0f), out var reason));
        Assert.Contains("MD232->0", reason);
    }

    [Fact]
    public void Other_signal_change_logs_full_snapshot_even_if_md232_only_chattered()
    {
        var t = new PoChangeLadderDiagnosticsTracker(30_000f);
        Assert.True(t.Observe(Sample(slit1: 9, slit2: 9, md232: 100f, m406: false), out _));
        Assert.True(t.Observe(Sample(slit1: 9, slit2: 10, md232: 200f, m406: false), out var reason));
        Assert.Contains("Slit2.PO_ID", reason);
        Assert.DoesNotContain("MD232", reason);
    }

    [Fact]
    public void M406_change_logs()
    {
        var t = new PoChangeLadderDiagnosticsTracker(30_000f);
        Assert.True(t.Observe(Sample(m406: false), out _));
        Assert.True(t.Observe(Sample(m406: true), out var reason));
        Assert.Contains("M40.6", reason);
    }

    [Theory]
    [InlineData(29_999f, 30_001f, true)]
    [InlineData(30_001f, 30_000f, true)]
    [InlineData(30_000f, 30_000f, false)]
    [InlineData(100f, 0f, true)]
    [InlineData(0f, 0f, false)]
    [InlineData(100f, 200f, false)]
    public void IsMd232LogEvent_matches_plc_greater_than_semantics(float prev, float curr, bool expected)
    {
        Assert.Equal(expected, PoChangeLadderDiagnosticsTracker.IsMd232LogEvent(prev, curr, 30_000f));
    }

    private static PoChangeLadderSnapshot Sample(
        int slit1 = 9,
        int slit2 = 9,
        float md232 = 0f,
        bool reconOn = false,
        bool manualButtEnd = false,
        bool m406 = false,
        bool m407 = false,
        int mw314 = 0,
        int slitId = 1,
        int ndt = 0) =>
        new(slit1, slit2, md232, reconOn, manualButtEnd, m406, m407, mw314, slitId, ndt);
}
