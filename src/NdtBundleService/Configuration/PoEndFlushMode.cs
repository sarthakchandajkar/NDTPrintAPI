namespace NdtBundleService.Configuration;

/// <summary>When to flush partial bundles on PO end for <see cref="MillPoEndSource.Plc"/> mills.</summary>
public enum PoEndFlushMode
{
    /// <summary>Flush partials immediately at the PO-end trigger (historical behavior), then sweep again when the drain window expires.</summary>
    Immediate = 0,

    /// <summary>Defer the partial flush until the drain window expires (or an operator forces it).</summary>
    AfterDrain = 1
}

public static class PoEndFlushModeParser
{
    public static PoEndFlushMode Parse(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return PoEndFlushMode.Immediate;

        if (string.Equals(value, "Immediate", StringComparison.OrdinalIgnoreCase))
            return PoEndFlushMode.Immediate;

        if (string.Equals(value, "AfterDrain", StringComparison.OrdinalIgnoreCase))
            return PoEndFlushMode.AfterDrain;

        return PoEndFlushMode.Immediate;
    }

    public static string ToConfigValue(PoEndFlushMode mode) => mode switch
    {
        PoEndFlushMode.Immediate => "Immediate",
        _ => "AfterDrain"
    };
}
