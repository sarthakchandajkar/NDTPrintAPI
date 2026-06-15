using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

internal static class FormationChartPathResolver
{
    internal static string? Resolve(NdtBundleOptions options)
    {
        var configured = (options.FormationChartCsvPath ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(configured))
            return null;

        return Path.IsPathRooted(configured)
            ? configured
            : Path.Combine(AppContext.BaseDirectory, configured);
    }
}
