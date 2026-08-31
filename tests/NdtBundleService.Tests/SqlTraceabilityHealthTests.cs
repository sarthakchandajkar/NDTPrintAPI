using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class SqlTraceabilityHealthTests
{
    [Fact]
    public void Required_schema_includes_Station_Printer_and_Manual_Station_Run_print_columns()
    {
        Assert.Contains("Station_Printer", SqlTraceabilityHealth.RequiredTables, StringComparer.Ordinal);
        Assert.Contains(
            ("Manual_Station_Run", "Print_Status"),
            SqlTraceabilityHealth.RequiredColumns);
        Assert.Contains(
            ("Manual_Station_Run", "Print_Error"),
            SqlTraceabilityHealth.RequiredColumns);
    }
}
