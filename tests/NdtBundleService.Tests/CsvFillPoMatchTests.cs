using Microsoft.Extensions.Logging.Abstractions;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// Pins fill-stamp PO matching: <c>PO_Number = @Po OR PO_Number = @PoNormalized</c>
/// where <c>@Po</c> is trimmed input and <c>@PoNormalized</c> is <see cref="InputSlitCsvParsing.NormalizePo"/>.
/// A mismatch between close-time storage and this predicate orphans files.
/// </summary>
public sealed class CsvFillPoMatchTests
{
    [Fact]
    public void Stamp_sql_matches_trimmed_or_normalized_PO()
    {
        foreach (var sql in new[]
                 {
                     ICsvFillService.OldestIncompleteSelectSql,
                     ICsvFillService.StampFindIncompleteSql,
                     ICsvFillService.HasTerminalFillRowSql
                 })
        {
            Assert.Contains("PO_Number = @Po OR PO_Number = @PoNormalized", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void NormalizePo_trims_and_strips_excel_decimal_and_leading_zeros()
    {
        // Whitespace
        Assert.Equal("1000057028", InputSlitCsvParsing.NormalizePo("  1000057028  "));

        // Excel float form (common SAP/CSV export)
        Assert.Equal("1000057028", InputSlitCsvParsing.NormalizePo("1000057028.0"));

        // Leading zeros → integer string (decimal parse)
        Assert.Equal("1000057028", InputSlitCsvParsing.NormalizePo("01000057028"));

        // Empty / whitespace-only
        Assert.Equal(string.Empty, InputSlitCsvParsing.NormalizePo("   "));
        Assert.Equal(string.Empty, InputSlitCsvParsing.NormalizePo(null));

        // Non-numeric: trim only (casing preserved by NormalizePo; SQL = is typically CI)
        Assert.Equal("PO-ABC", InputSlitCsvParsing.NormalizePo(" PO-ABC "));
    }

    [Fact]
    public void Stamp_predicate_matches_normalized_variants_of_stored_PO()
    {
        const string stored = "1000057028"; // typical close-time / WIP form

        Assert.True(InMemoryTransactionalCsvFillService.MatchesPo(stored, "1000057028"));
        Assert.True(InMemoryTransactionalCsvFillService.MatchesPo(stored, " 1000057028 "));
        Assert.True(InMemoryTransactionalCsvFillService.MatchesPo(stored, "1000057028.0"));
        Assert.True(InMemoryTransactionalCsvFillService.MatchesPo(stored, "01000057028"));

        // Genuinely different PO must not match
        Assert.False(InMemoryTransactionalCsvFillService.MatchesPo(stored, "1000057029"));
        Assert.False(InMemoryTransactionalCsvFillService.MatchesPo(stored, "1000060363"));
    }

    [Fact]
    public void Stamp_predicate_orphans_legacy_rows_stored_as_excel_float()
    {
        // Pre-fix rows may still have Excel ".0" in NDT_Bundle.PO_Number.
        // New closes normalize via AddBundleUpsertParameters (see Close_with_excel_float_PO_…).
        const string storedExcelFloat = "1000057028.0";
        Assert.False(InMemoryTransactionalCsvFillService.MatchesPo(storedExcelFloat, "1000057028"));
        Assert.True(InMemoryTransactionalCsvFillService.MatchesPo(storedExcelFloat, "1000057028.0"));
    }

    [Fact]
    public async Task Close_with_excel_float_PO_stores_plain_digits_then_plain_digit_file_stamps()
    {
        // Bundle close write contract: same as NdtBundleRepository.AddBundleUpsertParameters.
        const string excelFloatFromContext = "1000057028.0";
        var storedAtClose = InputSlitCsvParsing.NormalizePo(excelFloatFromContext);
        Assert.Equal("1000057028", storedAtClose);

        var fill = new InMemoryTransactionalCsvFillService();
        fill.Seed(
            "1226100001",
            target: 22,
            filled: 0,
            CsvFillState.PlcClosed,
            printedAt: DateTime.UtcNow,
            poNumber: storedAtClose);

        var assigner = new SlitCsvFillAssigner(fill, NullLogger<SlitCsvFillAssigner>.Instance);
        var stamped = await assigner.AssignAsync(
            @"C:\inbox\plain_po.csv",
            "1000057028",
            millNo: 1,
            pipeSize: null,
            fileNdtPipes: 7,
            holdWhenNoOpenBundle: true,
            CancellationToken.None);

        Assert.Equal("1226100001", stamped.BatchNo);
        Assert.False(stamped.Held);
        Assert.Equal(7, fill.GetFilled("1226100001"));
        Assert.True(InMemoryTransactionalCsvFillService.MatchesPo(storedAtClose, "1000057028"));
    }

    [Fact]
    public async Task TryStamp_normalized_file_PO_hits_bundle_stored_as_plain_digits()
    {
        var fill = new InMemoryTransactionalCsvFillService();
        fill.Seed(
            "1226100001",
            target: 22,
            filled: 0,
            CsvFillState.PlcClosed,
            printedAt: DateTime.UtcNow,
            poNumber: "1000057028");

        var assigner = new SlitCsvFillAssigner(fill, NullLogger<SlitCsvFillAssigner>.Instance);

        var stamped = await assigner.AssignAsync(
            @"C:\inbox\excel_po.csv",
            "1000057028.0",
            millNo: 1,
            pipeSize: null,
            fileNdtPipes: 5,
            holdWhenNoOpenBundle: true,
            CancellationToken.None);

        Assert.Equal("1226100001", stamped.BatchNo);
        Assert.Equal(5, fill.GetFilled("1226100001"));
    }

    [Fact]
    public async Task TryStamp_genuinely_different_PO_does_not_match_and_awaits_retry()
    {
        var fill = new InMemoryTransactionalCsvFillService();
        fill.Seed(
            "1226100001",
            target: 22,
            filled: 0,
            CsvFillState.PlcClosed,
            printedAt: DateTime.UtcNow,
            poNumber: "1000057028");

        var assigner = new SlitCsvFillAssigner(fill, NullLogger<SlitCsvFillAssigner>.Instance);

        var result = await assigner.AssignAsync(
            @"C:\inbox\other_po.csv",
            "1000060999",
            millNo: 1,
            pipeSize: null,
            fileNdtPipes: 5,
            holdWhenNoOpenBundle: true,
            CancellationToken.None);

        Assert.True(result.AwaitingTarget);
        Assert.True(result.Held);
        Assert.Null(result.BatchNo);
        Assert.Equal(0, fill.GetFilled("1226100001"));
        Assert.Equal(CsvFillState.PlcClosed, fill.GetState("1226100001"));
        Assert.Empty(fill.HeldFiles);
    }
}
