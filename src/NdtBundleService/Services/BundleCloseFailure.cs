using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>Honest close-failure copy — never format sequence 0 as a real batch number.</summary>
public static class BundleCloseFailure
{
    public const string AllocateUnavailable =
        "bundle close failed: could not allocate sequence — SQL unavailable";
}

/// <summary>
/// <c>UseSqlServerForBundles=false</c> is CSV/dev-only. A configured printer would send
/// <c>12YYM00000</c> (sequence 0). Refuse to start that combination.
/// </summary>
internal static class SqlOffPrintGuard
{
    public const string RefuseMessage =
        "NdtBundle:UseSqlServerForBundles=false cannot start with a configured NDT tag printer. "
        + "Bundle close would format sequence 0 as 12YYM00000 and print a fabricated tag. "
        + "Enable SQL bundles (Mill_Sequence) or disable EnableNdtTagZplAndPrint.";

    public static bool PrinterWouldPrint(NdtBundleOptions options)
    {
        if (!options.EnableNdtTagZplAndPrint)
            return false;

        var name = (options.NdtTagPrinterName ?? string.Empty).Trim();
        if (name.Length > 0)
            return true;

        var addr = (options.NdtTagPrinterAddress ?? string.Empty).Trim();
        return addr.Length > 0 && !string.Equals(addr, "0.0.0.0", StringComparison.Ordinal);
    }

    public static bool MustRefuseStart(NdtBundleOptions options) =>
        !SqlTraceabilityConnection.IsSqlEnabled(options) && PrinterWouldPrint(options);
}
