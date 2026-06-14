using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

internal static class PoPlanWipImportSettings
{
    public static bool IsEnabled(NdtBundleOptions options) =>
        options.ImportPoPlanWipFromFolder && SqlTraceabilityConnection.IsSqlEnabled(options);

    public static DateTime? GetImportMinUtc(NdtBundleOptions options) =>
        SourceFileEligibility.ParseMinUtcFromRaw(options.PoPlanImportMinLastWriteUtc)
        ?? SourceFileEligibility.ParseMinUtc(options);
}
