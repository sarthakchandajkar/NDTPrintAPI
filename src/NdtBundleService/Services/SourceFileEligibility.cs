using System.Globalization;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Filters source CSV files by last write time (UTC). Used for slit inbox, WIP/PO folders, and summary APIs.
/// PO plan folder scans use <see cref="GetPoPlanFolderEffectiveMinUtc"/> (rolling window + optional fixed min).
/// </summary>
public static class SourceFileEligibility
{
    /// <summary>Parses <see cref="NdtBundleOptions.MinSourceFileLastWriteUtc"/> (ISO-8601, e.g. <c>2026-04-05T00:00:00Z</c>).</summary>
    public static DateTime? ParseMinUtc(NdtBundleOptions options) =>
        ParseMinUtcFromRaw(options.MinSourceFileLastWriteUtc);

    public static DateTime? ParseMinUtcFromRaw(string? rawValue)
    {
        var raw = rawValue?.Trim();
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

    /// <summary>
    /// Effective minimum <c>LastWriteTimeUtc</c> for files under <see cref="NdtBundleOptions.PoPlanFolder"/>.
    /// Uses the later of <see cref="ParseMinUtc"/> and <c>UtcNow - PoPlanFolderRollingDays</c> when rolling days &gt; 0.
    /// </summary>
    public static DateTime? GetPoPlanFolderEffectiveMinUtc(NdtBundleOptions options, DateTime utcNow)
    {
        var configMin = ParseMinUtc(options);
        DateTime? rollingMin = null;
        var days = options.PoPlanFolderRollingDays;
        if (days > 0)
            rollingMin = utcNow.AddDays(-days);

        if (configMin is null && rollingMin is null)
            return null;
        if (configMin is null)
            return rollingMin;
        if (rollingMin is null)
            return configMin;

        return configMin.Value >= rollingMin.Value ? configMin : rollingMin;
    }

    /// <inheritdoc cref="GetPoPlanFolderEffectiveMinUtc"/>
    public static bool IncludePoPlanFolderFileUtc(DateTime lastWriteUtc, NdtBundleOptions options, DateTime? utcNow = null)
    {
        var now = utcNow ?? DateTime.UtcNow;
        var effectiveMin = GetPoPlanFolderEffectiveMinUtc(options, now);
        return IncludeFileUtc(lastWriteUtc, effectiveMin);
    }
}
