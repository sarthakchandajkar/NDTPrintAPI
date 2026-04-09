using System.Globalization;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Filters source CSV files by last write time (UTC). Used for slit inbox, WIP/PO folders, and summary APIs.
/// </summary>
public static class SourceFileEligibility
{
    /// <summary>Parses <see cref="NdtBundleOptions.MinSourceFileLastWriteUtc"/> (ISO-8601, e.g. <c>2026-04-05T00:00:00Z</c>).</summary>
    public static DateTime? ParseMinUtc(NdtBundleOptions options)
    {
        var raw = options.MinSourceFileLastWriteUtc?.Trim();
        if (string.IsNullOrEmpty(raw))
            return null;

        if (DateTime.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var u))
            return DateTime.SpecifyKind(u, DateTimeKind.Utc);

        return null;
    }

    public static bool IncludeFileUtc(DateTime lastWriteUtc, DateTime? minUtc)
    {
        if (minUtc is null)
            return true;
        var fileUtc = lastWriteUtc.Kind == DateTimeKind.Utc
            ? lastWriteUtc
            : lastWriteUtc.ToUniversalTime();
        return fileUtc >= minUtc.Value;
    }
}
