namespace NdtBundleService.Configuration;

/// <summary>Resolves per-mill <see cref="MillPoEndSource"/> from <see cref="MillConfig"/> and optional bundle options.</summary>
public static class MillPoEndSourceResolver
{
    public static MillPoEndSource ForMill(int millNo, NdtBundleOptions options)
    {
        var handshake = options.PlcHandshake ?? new PlcHandshakeOptions();
        var mill = handshake.Mills.FirstOrDefault(m => m.ResolveMillNo() == millNo);
        if (mill is null)
            return MillPoEndSource.File;

        return mill.ResolvePoEndSource(options);
    }

    public static bool AnyMillUsesPlcPoEnd(NdtBundleOptions options)
    {
        var handshake = options.PlcHandshake ?? new PlcHandshakeOptions();
        return handshake.Mills.Any(m => m.ResolvePoEndSource(options) == MillPoEndSource.Plc);
    }

    public static bool AnyMillUsesFilePoEnd(NdtBundleOptions options)
    {
        var handshake = options.PlcHandshake ?? new PlcHandshakeOptions();
        return handshake.Mills.Any(m => m.ResolvePoEndSource(options) == MillPoEndSource.File);
    }

    public static bool AnyMillUsesTcpOpenPoEnd(NdtBundleOptions options)
    {
        var handshake = options.PlcHandshake ?? new PlcHandshakeOptions();
        return handshake.Mills.Any(m => m.ResolvePoEndSource(options) == MillPoEndSource.TcpOpen);
    }

    public static MillPoEndSource Parse(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return MillPoEndSource.File;

        if (string.Equals(value, "Plc", StringComparison.OrdinalIgnoreCase))
            return MillPoEndSource.Plc;

        if (string.Equals(value, "File", StringComparison.OrdinalIgnoreCase))
            return MillPoEndSource.File;

        if (string.Equals(value, "TcpOpen", StringComparison.OrdinalIgnoreCase))
            return MillPoEndSource.TcpOpen;

        return MillPoEndSource.File;
    }

    public static string ToConfigValue(MillPoEndSource source) => source switch
    {
        MillPoEndSource.Plc => "Plc",
        MillPoEndSource.TcpOpen => "TcpOpen",
        _ => "File"
    };

    public static string Describe(MillPoEndSource source) => source switch
    {
        MillPoEndSource.Plc => "PLC PO-change trigger (S7 handshake)",
        MillPoEndSource.TcpOpen => "TCP open PO-change trigger (AG_SEND/AG_RECV — codec TBD)",
        _ => "TM Bundle WIP filename"
    };
}

public static class MillConfigPoEndSourceExtensions
{
    public static MillPoEndSource ResolvePoEndSource(this MillConfig mill, NdtBundleOptions? bundle = null)
    {
        if (!string.IsNullOrWhiteSpace(mill.PoEndSource))
            return MillPoEndSourceResolver.Parse(mill.PoEndSource);

#pragma warning disable CS0618
        if (bundle?.FileBasedPoEnd?.Enabled == true)
            return MillPoEndSource.File;
#pragma warning restore CS0618

        return MillPoEndSource.File;
    }

    public static bool UsesPlcHandshakeForPoEnd(this MillConfig mill, NdtBundleOptions? bundle = null) =>
        mill.ResolvePoEndSource(bundle) == MillPoEndSource.Plc;

    public static bool UsesFileBasedPoEnd(this MillConfig mill, NdtBundleOptions? bundle = null) =>
        mill.ResolvePoEndSource(bundle) == MillPoEndSource.File;

    public static bool UsesS7TelemetryOnlyForPoEnd(this MillConfig mill, NdtBundleOptions? bundle = null)
    {
        var source = mill.ResolvePoEndSource(bundle);
        return source is MillPoEndSource.File or MillPoEndSource.TcpOpen;
    }
}
