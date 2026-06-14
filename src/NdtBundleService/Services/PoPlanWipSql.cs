using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>Helpers for reading PO plan WIP data from <c>dbo.PO_Plan_WIP</c> instead of scanning PO plan CSV folders.</summary>
internal static class PoPlanWipSql
{
    public static bool IsEnabled(NdtBundleOptions options) =>
        options.PreferSqlForPoPlanWip && SqlTraceabilityConnection.IsSqlEnabled(options);
}
