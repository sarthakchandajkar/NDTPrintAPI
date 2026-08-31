using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// lastRecord is persisted print context, not reconstructed from Input_Slit_Row.
/// SQL newest-row reconstruction is not equivalent (restart-mid-PO / backfill / PLC-only remainder).
/// </summary>
public sealed class LastRecordPrintContextTests
{
    [Fact]
    public void Identical_lastRecord_produces_identical_csv_and_zpl()
    {
        var record = Sample("SL-10", new DateTime(2026, 8, 15, 10, 0, 0, DateTimeKind.Utc));
        var reconstructed = Sample("SL-10", new DateTime(2026, 8, 15, 10, 0, 0, DateTimeKind.Utc));

        Assert.Equal(Csv(record), Csv(reconstructed));
        Assert.Equal(Zpl(record), Zpl(reconstructed));
    }

    [Fact]
    public void Sql_newest_Input_Slit_Row_can_diverge_from_in_memory_lastRecord()
    {
        // Process held slit SL-10 as lastRecord. After a crash, SQL's newest imported row
        // is a later backfill (SL-99) — ImportedAtUtc DESC is not "the slit the mill held".
        var inMemory = Sample("SL-10", new DateTime(2026, 8, 15, 10, 0, 0, DateTimeKind.Utc));
        var sqlNewest = Sample("SL-99", new DateTime(2026, 8, 16, 18, 0, 0, DateTimeKind.Utc));

        Assert.NotEqual(Csv(inMemory), Csv(sqlNewest));
        Assert.NotEqual(Zpl(inMemory), Zpl(sqlNewest));
    }

    private static InputSlitRecord Sample(string slitNo, DateTime start) => new()
    {
        PoNumber = "1000060288",
        MillNo = 1,
        SlitNo = slitNo,
        NdtPipes = 12,
        RejectedPipes = 1,
        SlitStartTime = start,
        SlitFinishTime = start.AddMinutes(8),
        NdtShortLengthPipe = "2",
        RejectedShortLengthPipe = "0"
    };

    private static string Csv(InputSlitRecord record) =>
        BundleCloseCsv.FormatLine(record, totalNdtPcs: 80, ndtBatchNoFormatted: "1226100042");

    private static byte[] Zpl(InputSlitRecord record)
    {
        var date = record.SlitStartTime ?? DateTime.Now;
        return ZplNdtLabelBuilder.BuildNdtTagZpl(
            "1226100042",
            record.MillNo,
            record.PoNumber,
            "X42",
            "6",
            "2.0",
            "6.0",
            "120",
            "ERW",
            date,
            pcsInBundle: 80,
            isReprint: false);
    }
}
