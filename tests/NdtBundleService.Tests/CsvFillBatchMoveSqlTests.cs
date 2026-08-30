using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// Pins Output_Slit_Row batch-move SQL to <c>Source_File</c> (live §5.3 used the wrong column name).
/// </summary>
public sealed class CsvFillBatchMoveSqlTests
{
    [Fact]
    public void OutputSlitRowBatchMoveSql_matches_Source_File_not_Source_File_Name()
    {
        var sql = ICsvFillService.OutputSlitRowBatchMoveSql;
        Assert.Contains("Source_File", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("Source_File_Name", sql, StringComparison.Ordinal);
        Assert.Contains("Source_File LIKE @LikeWin", sql, StringComparison.Ordinal);
        Assert.Contains("Source_File LIKE @LikeUnix", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Incomplete_and_stamp_sql_exclude_voided_in_sql_not_only_IsIncomplete()
    {
        Assert.True(CsvFillState.IsIncomplete(CsvFillState.PlcClosed));
        Assert.True(CsvFillState.IsIncomplete(CsvFillState.CsvFilling));
        Assert.False(CsvFillState.IsIncomplete(CsvFillState.Voided));

        foreach (var sql in new[]
                 {
                     ICsvFillService.OldestIncompleteSelectSql,
                     ICsvFillService.StampFindIncompleteSql
                 })
        {
            Assert.Contains("ISNULL(Voided, 0) = 0", sql, StringComparison.Ordinal);
            Assert.Contains("Csv_Fill_State IN (N'PlcClosed', N'CsvFilling')", sql, StringComparison.Ordinal);
        }
    }
}
