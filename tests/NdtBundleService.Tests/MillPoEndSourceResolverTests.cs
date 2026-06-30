using NdtBundleService.Configuration;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class MillPoEndSourceResolverTests
{
    [Theory]
    [InlineData(null, MillPoEndSource.File)]
    [InlineData("", MillPoEndSource.File)]
    [InlineData("Plc", MillPoEndSource.Plc)]
    [InlineData("plc", MillPoEndSource.Plc)]
    [InlineData("File", MillPoEndSource.File)]
    [InlineData("TcpOpen", MillPoEndSource.TcpOpen)]
    [InlineData("tcpopen", MillPoEndSource.TcpOpen)]
    [InlineData("unknown", MillPoEndSource.File)]
    public void Parse_returns_expected_source(string? value, MillPoEndSource expected)
    {
        Assert.Equal(expected, MillPoEndSourceResolver.Parse(value));
    }

    [Fact]
    public void MillConfig_defaults_to_File_when_po_end_source_missing()
    {
        var mill = new MillConfig { Name = "Mill-2", MillNo = 2 };
        Assert.Equal(MillPoEndSource.File, mill.ResolvePoEndSource());
        Assert.False(mill.UsesPlcHandshakeForPoEnd());
        Assert.True(mill.UsesFileBasedPoEnd());
        Assert.True(mill.UsesS7TelemetryOnlyForPoEnd());
    }

    [Fact]
    public void MillConfig_Plc_uses_handshake_not_telemetry_only()
    {
        var mill = new MillConfig { Name = "Mill-2", MillNo = 2, PoEndSource = "Plc" };
        Assert.True(mill.UsesPlcHandshakeForPoEnd());
        Assert.False(mill.UsesS7TelemetryOnlyForPoEnd());
    }

    [Fact]
    public void MillConfig_TcpOpen_uses_s7_telemetry_only()
    {
        var mill = new MillConfig { Name = "Mill-4", MillNo = 4, PoEndSource = "TcpOpen" };
        Assert.False(mill.UsesPlcHandshakeForPoEnd());
        Assert.True(mill.UsesS7TelemetryOnlyForPoEnd());
    }

    [Fact]
    public void Deprecated_FileBasedPoEnd_Enabled_fallback_when_po_end_source_absent()
    {
        var mill = new MillConfig { Name = "Mill-1", MillNo = 1 };
        var options = new NdtBundleOptions
        {
#pragma warning disable CS0618
            FileBasedPoEnd = new FileBasedPoEndOptions { Enabled = true }
#pragma warning restore CS0618
        };
        Assert.Equal(MillPoEndSource.File, mill.ResolvePoEndSource(options));
    }

    [Fact]
    public void ForMill_resolves_from_plc_handshake_mills()
    {
        var options = new NdtBundleOptions
        {
            PlcHandshake = new PlcHandshakeOptions
            {
                Mills =
                [
                    new MillConfig { MillNo = 1, PoEndSource = "Plc" },
                    new MillConfig { MillNo = 4, PoEndSource = "File" }
                ]
            }
        };

        Assert.Equal(MillPoEndSource.Plc, MillPoEndSourceResolver.ForMill(1, options));
        Assert.Equal(MillPoEndSource.File, MillPoEndSourceResolver.ForMill(4, options));
        Assert.True(MillPoEndSourceResolver.AnyMillUsesPlcPoEnd(options));
        Assert.True(MillPoEndSourceResolver.AnyMillUsesFilePoEnd(options));
    }

    [Fact]
    public void AnyMillUsesTcpOpenPoEnd_detects_tcp_mill()
    {
        var options = new NdtBundleOptions
        {
            PlcHandshake = new PlcHandshakeOptions
            {
                Mills =
                [
                    new MillConfig { MillNo = 1, PoEndSource = "Plc" },
                    new MillConfig { MillNo = 4, PoEndSource = "TcpOpen" }
                ]
            }
        };

        Assert.True(MillPoEndSourceResolver.AnyMillUsesTcpOpenPoEnd(options));
        Assert.False(MillPoEndSourceResolver.AnyMillUsesTcpOpenPoEnd(new NdtBundleOptions
        {
            PlcHandshake = new PlcHandshakeOptions
            {
                Mills = [new MillConfig { MillNo = 4, PoEndSource = "File" }]
            }
        }));
    }
}
