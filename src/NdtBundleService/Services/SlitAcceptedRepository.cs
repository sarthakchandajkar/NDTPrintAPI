using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

public interface ISlitAcceptedRepository
{
    Task<bool> EnsureImportReadyAsync(CancellationToken cancellationToken);
    Task<bool> IsImportSourceFilePresentAsync(string sourceFileKey, CancellationToken cancellationToken);
    Task<int> InsertImportRowsAsync(string sourceFileKey, IReadOnlyList<SlitAcceptedRow> rows, CancellationToken cancellationToken);
    Task<int> InsertAuditRowsAsync(string sourceFileKey, IReadOnlyList<CsvFolderAuditRow> rows, CancellationToken cancellationToken);
    Task<string?> TryGetLatestSlitWidthAsync(string slitNo, CancellationToken cancellationToken);
}

public sealed class SlitAcceptedRepository : ISlitAcceptedRepository
{
    private readonly NdtBundleOptions _options;
    private readonly ILogger<SlitAcceptedRepository> _logger;
    private bool? _tableExists;

    public SlitAcceptedRepository(IOptions<NdtBundleOptions> options, ILogger<SlitAcceptedRepository> logger)
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
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "Slit_Accepted_Row source check", cancellationToken)
            .ConfigureAwait(false);
        await using var cmd = new SqlCommand(
            @"SELECT TOP 1 1
FROM (
    SELECT Source_File FROM dbo.Slit_Accepted_Audit_Row WHERE Source_File = @SourceFile
    UNION ALL
    SELECT Source_File FROM dbo.Slit_Accepted_Row WHERE Source_File = @SourceFile
) x;",
            conn);
        cmd.Parameters.AddWithValue("@SourceFile", sourceFileKey);
        var scalar = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return scalar is not null and not DBNull;
    }

    public async Task<int> InsertImportRowsAsync(
        string sourceFileKey,
        IReadOnlyList<SlitAcceptedRow> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0 || !await EnsureImportReadyAsync(cancellationToken).ConfigureAwait(false))
            return 0;

        await using var conn = SqlTraceabilityConnection.Create(_options);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "Slit_Accepted_Row insert", cancellationToken)
            .ConfigureAwait(false);

        const string sql = @"
INSERT INTO dbo.Slit_Accepted_Row
    (Slit_No, Slit_Width, HRC_Number, Slit_Thick, PO_Number, Mill_No, NSS, Source_File, Source_Row_Number)
VALUES
    (@SlitNo, @Width, @Hrc, @Thick, @Po, @Mill, @Nss, @SourceFile, @RowNo);";

        var inserted = 0;
        foreach (var row in rows)
        {
            if (string.IsNullOrWhiteSpace(row.SlitNo))
                continue;
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@SlitNo", row.SlitNo.Trim());
            cmd.Parameters.AddWithValue("@Width", NullIfEmpty(row.SlitWidth));
            cmd.Parameters.AddWithValue("@Hrc", NullIfEmpty(row.HrcNumber));
            cmd.Parameters.AddWithValue("@Thick", NullIfEmpty(row.SlitThick));
            cmd.Parameters.AddWithValue("@Po", NullIfEmpty(row.PoNumber));
            cmd.Parameters.AddWithValue("@Mill", row.MillNo.HasValue ? row.MillNo.Value : DBNull.Value);
            cmd.Parameters.AddWithValue("@Nss", NullIfEmpty(row.Nss));
            cmd.Parameters.AddWithValue("@SourceFile", sourceFileKey);
            cmd.Parameters.AddWithValue("@RowNo", row.SourceRowNumber);
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
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "Slit_Accepted_Audit_Row insert", cancellationToken)
            .ConfigureAwait(false);

        const string sql = @"
INSERT INTO dbo.Slit_Accepted_Audit_Row
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

    public async Task<string?> TryGetLatestSlitWidthAsync(string slitNo, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(slitNo) || !await EnsureImportReadyAsync(cancellationToken).ConfigureAwait(false))
            return null;

        await using var conn = SqlTraceabilityConnection.Create(_options);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "Slit_Accepted_Row width lookup", cancellationToken)
            .ConfigureAwait(false);

        const string sql = @"
SELECT TOP 1 Slit_Width
FROM dbo.Slit_Accepted_Row
WHERE Slit_No = @SlitNo
  AND Slit_Width IS NOT NULL
  AND LTRIM(RTRIM(Slit_Width)) <> N''
ORDER BY ImportedAtUtc DESC, Slit_Accepted_Row_ID DESC;";

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@SlitNo", slitNo.Trim());
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
            await SqlTraceabilityConnection.OpenAsync(conn, _logger, "Slit_Accepted_Row availability", cancellationToken)
                .ConfigureAwait(false);
            await using var cmd = new SqlCommand(
                @"SELECT
    CASE WHEN EXISTS (
        SELECT 1 FROM sys.tables WHERE name = N'Slit_Accepted_Row' AND schema_id = SCHEMA_ID(N'dbo'))
         AND EXISTS (
        SELECT 1 FROM sys.tables WHERE name = N'Slit_Accepted_Audit_Row' AND schema_id = SCHEMA_ID(N'dbo'))
    THEN 1 ELSE 0 END;",
                conn);
            var exists = Convert.ToInt32(await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)) == 1;
            _tableExists = exists;
            if (!exists)
                _logger.LogWarning(
                    "dbo.Slit_Accepted_Row and/or dbo.Slit_Accepted_Audit_Row missing; Slit Accepted SQL import/lookup skipped.");
            return exists;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not verify dbo.Slit_Accepted_Row.");
            return false;
        }
    }

    private static object NullIfEmpty(string? value) =>
        string.IsNullOrWhiteSpace(value) ? DBNull.Value : value.Trim();
}
