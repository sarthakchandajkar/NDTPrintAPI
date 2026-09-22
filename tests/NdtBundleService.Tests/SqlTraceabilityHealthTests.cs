using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class SqlTraceabilityHealthTests
{
    [Fact]
    public void Required_schema_includes_Printer_and_Manual_Station_Run_print_columns()
    {
        Assert.Contains("Printer", SqlTraceabilityHealth.RequiredTables, StringComparer.Ordinal);
        Assert.Contains("Mill_Instance_Status", SqlTraceabilityHealth.RequiredTables, StringComparer.Ordinal);
        Assert.DoesNotContain("Mill_Printer", SqlTraceabilityHealth.RequiredTables, StringComparer.Ordinal);
        Assert.DoesNotContain("Station_Printer", SqlTraceabilityHealth.RequiredTables, StringComparer.Ordinal);
        Assert.Contains(
            ("Manual_Station_Run", "Print_Status"),
            SqlTraceabilityHealth.RequiredColumns);
        Assert.Contains(
            ("Manual_Station_Run", "Print_Error"),
            SqlTraceabilityHealth.RequiredColumns);
    }
}
