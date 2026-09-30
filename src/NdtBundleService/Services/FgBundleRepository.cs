using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

public interface IFgBundleRepository
{
    Task<bool> EnsureImportReadyAsync(CancellationToken cancellationToken);
    Task<bool> IsImportSourceFilePresentAsync(string sourceFileKey, CancellationToken cancellationToken);
    Task<int> InsertImportRowsAsync(string sourceFileKey, IReadOnlyList<FgBundleRow> rows, CancellationToken cancellationToken);
    Task<int> InsertAuditRowsAsync(string sourceFileKey, IReadOnlyList<CsvFolderAuditRow> rows, CancellationToken cancellationToken);
    Task<string?> TryGetLatestPipeGradeAsync(string poNumber, int? millNo, CancellationToken cancellationToken);
}

public sealed class FgBundleRepository : IFgBundleRepository
{
    private readonly NdtBundleOptions _options;
    private readonly ILogger<FgBundleRepository> _logger;
    private bool? _tableExists;

    public FgBundleRepository(IOptions<NdtBundleOptions> options, ILogger<FgBundleRepository> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<bool> EnsureImportReadyAsync(CancellationToken cancellationToken)
    {
        if (!SqlTraceabilityConnection.IsSqlEnabled(_options))
            return false;
        return await EnsureTableExistsCachedAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> IsImportSourceFilePresentAsync(string sourceFileKey, CancellationToken cancellationToken)
    {
        if (!await EnsureImportReadyAsync(cancellationToken).ConfigureAwait(false))
            return false;

        await using var conn = SqlTraceabilityConnection.Create(_options);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "Fg_Bundle_Row source check", cancellationToken)
            .ConfigureAwait(false);
        await using var cmd = new SqlCommand(
            @"SELECT TOP 1 1
FROM (
    SELECT Source_File FROM dbo.Fg_Bundle_Audit_Row WHERE Source_File = @SourceFile
    UNION ALL
    SELECT Source_File FROM dbo.Fg_Bundle_Row WHERE Source_File = @SourceFile
) x;",
            conn);
        cmd.Parameters.AddWithValue("@SourceFile", sourceFileKey);
        var scalar = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return scalar is not null and not DBNull;
    }

    public async Task<int> InsertImportRowsAsync(
        string sourceFileKey,
        IReadOnlyList<FgBundleRow> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0 || !await EnsureImportReadyAsync(cancellationToken).ConfigureAwait(false))
            return 0;

        await using var conn = SqlTraceabilityConnection.Create(_options);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "Fg_Bundle_Row insert", cancellationToken)
            .ConfigureAwait(false);

        const string sql = @"
INSERT INTO dbo.Fg_Bundle_Row
    (PO_Number, Mill_No, Pipe_Grade, Pipe_Size, Pipe_Thickness, Pipe_Length, Pipe_Weight_Per_Meter, Pipe_Type,
     Act_Pcs_In_Bundle, Bundle_No, Bundle_Status, Slit_1_Num, Slit_1_Ok_Pcs, Slit_2_Num, Slit_2_Ok_Pcs,
     Slit_3_Num, Slit_3_Ok_Pcs, Slit_4_Num, Slit_4_Ok_Pcs, Bundle_Wt, Bundle_Start, Bundle_End,
     Operator_Done, Non_Standard_Slit, PO_Specification, Source_File)
VALUES
    (@Po, @Mill, @Grade, @Size, @Thick, @Len, @WtM, @Type,
     @ActPcs, @BundleNo, @Status, @S1, @S1Ok, @S2, @S2Ok,
     @S3, @S3Ok, @S4, @S4Ok, @BundleWt, @Start, @End,
     @Op, @Nss, @Spec, @SourceFile);";

        var inserted = 0;
        foreach (var row in rows)
        {
            if (string.IsNullOrWhiteSpace(row.PoNumber))
                continue;
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@Po", InputSlitCsvParsing.NormalizePo(row.PoNumber));
            cmd.Parameters.AddWithValue("@Mill", row.MillNo.HasValue ? row.MillNo.Value : DBNull.Value);
            cmd.Parameters.AddWithValue("@Grade", NullIfEmpty(row.PipeGrade));
            cmd.Parameters.AddWithValue("@Size", NullIfEmpty(row.PipeSize));
            cmd.Parameters.AddWithValue("@Thick", NullIfEmpty(row.PipeThickness));
            cmd.Parameters.AddWithValue("@Len", NullIfEmpty(row.PipeLength));
            cmd.Parameters.AddWithValue("@WtM", NullIfEmpty(row.PipeWeightPerMeter));
            cmd.Parameters.AddWithValue("@Type", NullIfEmpty(row.PipeType));
            cmd.Parameters.AddWithValue("@ActPcs", NullIfEmpty(row.ActPcsInBundle));
            cmd.Parameters.AddWithValue("@BundleNo", NullIfEmpty(row.BundleNo));
            cmd.Parameters.AddWithValue("@Status", NullIfEmpty(row.BundleStatus));
            cmd.Parameters.AddWithValue("@S1", NullIfEmpty(row.Slit1Num));
            cmd.Parameters.AddWithValue("@S1Ok", NullIfEmpty(row.Slit1OkPcs));
            cmd.Parameters.AddWithValue("@S2", NullIfEmpty(row.Slit2Num));
            cmd.Parameters.AddWithValue("@S2Ok", NullIfEmpty(row.Slit2OkPcs));
            cmd.Parameters.AddWithValue("@S3", NullIfEmpty(row.Slit3Num));
            cmd.Parameters.AddWithValue("@S3Ok", NullIfEmpty(row.Slit3OkPcs));
            cmd.Parameters.AddWithValue("@S4", NullIfEmpty(row.Slit4Num));
            cmd.Parameters.AddWithValue("@S4Ok", NullIfEmpty(row.Slit4OkPcs));
            cmd.Parameters.AddWithValue("@BundleWt", NullIfEmpty(row.BundleWt));
            cmd.Parameters.AddWithValue("@Start", NullIfEmpty(row.BundleStart));
            cmd.Parameters.AddWithValue("@End", NullIfEmpty(row.BundleEnd));
            cmd.Parameters.AddWithValue("@Op", NullIfEmpty(row.OperatorDone));
            cmd.Parameters.AddWithValue("@Nss", NullIfEmpty(row.NonStandardSlit));
            cmd.Parameters.AddWithValue("@Spec", NullIfEmpty(row.PoSpecification));
            cmd.Parameters.AddWithValue("@SourceFile", sourceFileKey);
            inserted += await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        return inserted;
    }

    public async Task<int> InsertAuditRowsAsync(
        string sourceFileKey,
        IReadOnlyList<CsvFolderAuditRow> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0 || !await EnsureImportReadyAsync(cancellationToken).ConfigureAwait(false))
            return 0;

        await using var conn = SqlTraceabilityConnection.Create(_options);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "Fg_Bundle_Audit_Row insert", cancellationToken)
            .ConfigureAwait(false);

        const string sql = @"
INSERT INTO dbo.Fg_Bundle_Audit_Row
    (Source_File, Source_Row_Number, Header_Line, Row_Line, Columns_Json)
VALUES
    (@SourceFile, @RowNo, @Header, @RowLine, @ColumnsJson);";

        var inserted = 0;
        foreach (var row in rows)
        {
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@SourceFile", sourceFileKey);
            cmd.Parameters.AddWithValue("@RowNo", row.SourceRowNumber);
            cmd.Parameters.AddWithValue("@Header", row.HeaderLine ?? string.Empty);
            cmd.Parameters.AddWithValue("@RowLine", row.RowLine ?? string.Empty);
            cmd.Parameters.AddWithValue("@ColumnsJson", row.ColumnsJson ?? "{}");
            inserted += await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        return inserted;
    }

    public async Task<string?> TryGetLatestPipeGradeAsync(
        string poNumber,
        int? millNo,
        CancellationToken cancellationToken)
    {
        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po) || !await EnsureImportReadyAsync(cancellationToken).ConfigureAwait(false))
            return null;

        await using var conn = SqlTraceabilityConnection.Create(_options);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "Fg_Bundle_Row grade lookup", cancellationToken)
            .ConfigureAwait(false);

        var sql = millNo is >= 1 and <= 4
            ? @"
SELECT TOP 1 Pipe_Grade
FROM dbo.Fg_Bundle_Row
WHERE PO_Number = @Po
  AND Mill_No = @Mill
  AND Pipe_Grade IS NOT NULL
  AND LTRIM(RTRIM(Pipe_Grade)) <> N''
ORDER BY ImportedAtUtc DESC, Fg_Bundle_Row_ID DESC;"
            : @"
SELECT TOP 1 Pipe_Grade
FROM dbo.Fg_Bundle_Row
WHERE PO_Number = @Po
  AND Pipe_Grade IS NOT NULL
  AND LTRIM(RTRIM(Pipe_Grade)) <> N''
ORDER BY ImportedAtUtc DESC, Fg_Bundle_Row_ID DESC;";

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@Po", po);
        if (millNo is >= 1 and <= 4)
            cmd.Parameters.AddWithValue("@Mill", millNo.Value);

        var scalar = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return scalar is string s && !string.IsNullOrWhiteSpace(s) ? s.Trim() : null;
    }

    private async Task<bool> EnsureTableExistsCachedAsync(CancellationToken cancellationToken)
    {
        if (_tableExists == false)
            return false;
        try
        {
            await using var conn = SqlTraceabilityConnection.Create(_options);
            await SqlTraceabilityConnection.OpenAsync(conn, _logger, "Fg_Bundle_Row availability", cancellationToken)
                .ConfigureAwait(false);
            await using var cmd = new SqlCommand(
                @"SELECT
    CASE WHEN EXISTS (
        SELECT 1 FROM sys.tables WHERE name = N'Fg_Bundle_Row' AND schema_id = SCHEMA_ID(N'dbo'))
         AND EXISTS (
        SELECT 1 FROM sys.tables WHERE name = N'Fg_Bundle_Audit_Row' AND schema_id = SCHEMA_ID(N'dbo'))
    THEN 1 ELSE 0 END;",
                conn);
            var exists = Convert.ToInt32(await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)) == 1;
            _tableExists = exists;
            if (!exists)
                _logger.LogWarning(
                    "dbo.Fg_Bundle_Row and/or dbo.Fg_Bundle_Audit_Row missing; FG bundle SQL import/lookup skipped.");
            return exists;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not verify dbo.Fg_Bundle_Row.");
            return false;
        }
    }

    private static object NullIfEmpty(string? value) =>
        string.IsNullOrWhiteSpace(value) ? DBNull.Value : value.Trim();
}
