using System.Globalization;

namespace NdtBundleService.Services;

/// <summary>
/// Helpers for the <c>yyMMdd_HHmmss</c> production timestamp embedded in WIP bundle filenames
/// (e.g. <c>WIP_01_1000060546_2601020510_260727_054023.csv</c> → <c>260727_054023</c>).
/// Unlike file write stamps — which backlog copies refresh, interleaving POs out of production
/// order — the embedded key is copy-invariant. Fixed-width digit strings compare ordinally in
/// chronological order, so comparisons never need to parse.
/// </summary>
public static class WipSortKey
{
    public const string Format = "yyMMdd_HHmmss";

    /// <summary>Embedded key from a WIP bundle filename, or null when the name matches no WIP pattern.</summary>
    public static string? TryGetFromFileName(string? fileName)
    {
        if (string.IsNullOrWhiteSpace(fileName))
            return null;

        return WipBundleFileName.TryParse(Path.GetFileName(fileName))?.SortKey;
    }

    /// <summary>True when the key parses as a real <c>yyMMdd_HHmmss</c> timestamp (telemetry only; ordering stays ordinal).</summary>
    public static bool IsValid(string? sortKey) =>
        !string.IsNullOrWhiteSpace(sortKey)
        && DateTime.TryParseExact(sortKey, Format, CultureInfo.InvariantCulture, DateTimeStyles.None, out _);

    /// <summary>
    /// Formats a UTC instant as a plant-local sort key for floor fallbacks. Assumes the host timezone
    /// equals the plant timezone (production log offsets and WIP filename stamps are both UTC+4).
    /// </summary>
    public static string FromUtc(DateTime utc) =>
        utc.ToLocalTime().ToString(Format, CultureInfo.InvariantCulture);
}
