using System.Globalization;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

public sealed class PoPlanWipRepository : IPoPlanWipRepository
{
    private static string LatestByPoSql => $@"
SELECT {PoPlanWipRowMapper.SelectColumns}
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY PO_Number
               ORDER BY ImportedAtUtc DESC, PO_Plan_WIP_ID DESC) AS rn
    FROM dbo.PO_Plan_WIP
    WHERE PO_Number IS NOT NULL AND LTRIM(RTRIM(PO_Number)) <> ''
) ranked
WHERE rn = 1;";

    private static string LatestByMillSql => $@"
SELECT {PoPlanWipRowMapper.SelectColumns}
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY Mill_No
               ORDER BY ImportedAtUtc DESC, PO_Plan_WIP_ID DESC) AS rn
    FROM dbo.PO_Plan_WIP
    WHERE Mill_No BETWEEN 1 AND 4
      AND PO_Number IS NOT NULL AND LTRIM(RTRIM(PO_Number)) <> ''
) ranked
WHERE rn = 1;";

    private static string LatestBySinglePoSql => $@"
SELECT TOP 1 {PoPlanWipRowMapper.SelectColumns}
FROM dbo.PO_Plan_WIP
WHERE PO_Number = @PoNumber
ORDER BY ImportedAtUtc DESC, PO_Plan_WIP_ID DESC;";

    private const string SignatureSql = @"
SELECT COUNT_BIG(1) AS TotalRows, MAX(ImportedAtUtc) AS MaxImportedUtc, MAX(PO_Plan_WIP_ID) AS MaxId
FROM dbo.PO_Plan_WIP;";

    private const string ImportSourcePresentSql = @"
SELECT TOP 1 1
FROM dbo.PO_Plan_WIP
WHERE Source_File = @SourceFile;";

    private const string InsertImportRowSql = @"
INSERT INTO dbo.PO_Plan_WIP
    (PO_Number, Mill_No, Planned_Month, Pipe_Grade, Pipe_Size, Pipe_Thickness, Pipe_Length,
     Pipe_Weight_Per_Meter, Pipe_Type, Output_Itemcode, Item_Description, Product_Type, PO_Specification,
     Input_WIP_Itemcode, Pieces_Per_Bundle, NDTPcsPerBundle, Total_Pieces, Source_File)
VALUES
    (@PoNumber, @MillNo, @PlannedMonth, @PipeGrade, @PipeSize, @PipeThickness, @PipeLength,
     @PipeWeightPerMeter, @PipeType, @OutputItemcode, @ItemDescription, @ProductType, @PoSpecification,
     @InputWipItemcode, @PiecesPerBundle, @NdtPcsPerBundle, @TotalPieces, @SourceFile);";

    private readonly NdtBundleOptions _options;
    private readonly ILogger<PoPlanWipRepository> _logger;
    private bool? _tableExists;

    public PoPlanWipRepository(IOptions<NdtBundleOptions> options, ILogger<PoPlanWipRepository> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<bool> IsAvailableAsync(CancellationToken cancellationToken)
    {
        if (!PoPlanWipSql.IsEnabled(_options))
            return false;

        return await EnsureTableExistsCachedAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> EnsureTableExistsCachedAsync(CancellationToken cancellationToken)
    {
        if (_tableExists == false)
            return false;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(_options);
            await SqlTraceabilityConnection.OpenAsync(conn, _logger, "PO_Plan_WIP availability", cancellationToken)
                .ConfigureAwait(false);
            if (!await TableExistsAsync(conn, cancellationToken).ConfigureAwait(false))
            {
                _tableExists = false;
                _logger.LogWarning(
                    "PO_Plan_WIP table was not found in {Database}; PO plan SQL reads/import will be skipped.",
                    conn.Database);
                return false;
            }

            _tableExists = true;
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not verify dbo.PO_Plan_WIP; PO plan SQL reads/import will be skipped.");
            return false;
        }
    }

    public async Task<string?> TryGetDataSignatureAsync(CancellationToken cancellationToken)
    {
        if (!await IsAvailableAsync(cancellationToken).ConfigureAwait(false))
            return null;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(_options);
            await SqlTraceabilityConnection.OpenAsync(conn, _logger, "PO_Plan_WIP signature", cancellationToken)
                .ConfigureAwait(false);

            await using var cmd = new SqlCommand(SignatureSql, conn);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                return "sql:empty";

            var count = reader.IsDBNull(0) ? 0L : reader.GetInt64(0);
            var maxImported = reader.IsDBNull(1) ? DateTime.MinValue : reader.GetDateTime(1);
            var maxId = reader.IsDBNull(2) ? 0L : reader.GetInt64(2);
            return FormattableString.Invariant($"sql:{count}|{maxImported.Ticks}|{maxId}");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "PO_Plan_WIP signature query failed; SQL pipe-size reads will fall back to CSV folders.");
            return null;
        }
    }

    public async Task<IReadOnlyDictionary<string, string>> GetLatestPipeSizeByPoAsync(CancellationToken cancellationToken)
    {
        var snapshot = await LoadLatestByPoAsync(cancellationToken).ConfigureAwait(false);
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (po, row) in snapshot)
        {
            if (string.IsNullOrWhiteSpace(row.PipeSize))
                continue;
            map[po] = row.PipeSize.Trim();
        }

        return map;
    }

    public async Task<PoPlanWipSqlSnapshot> GetLatestEnrichmentAsync(CancellationToken cancellationToken)
    {
        if (!await IsAvailableAsync(cancellationToken).ConfigureAwait(false))
        {
            return new PoPlanWipSqlSnapshot
            {
                SourceDescription = "JazeeraMES_Prod.dbo.PO_Plan_WIP (SQL unavailable)"
            };
        }

        var byPo = await LoadLatestByPoAsync(cancellationToken).ConfigureAwait(false);
        var byMill = await LoadLatestByMillAsync(cancellationToken).ConfigureAwait(false);
        var database = SqlTraceabilityConnection.DescribeConnectionString(SqlTraceabilityConnection.ResolveConnectionString(_options)).Database
            ?? SqlTraceabilityHealth.ExpectedDatabaseName;

        _logger.LogInformation(
            "Loaded PO plan WIP enrichment for {PoCount} PO(s) and {MillCount} mill row(s) from {Database}.dbo.PO_Plan_WIP.",
            byPo.Count,
            byMill.Count,
            database);

        return new PoPlanWipSqlSnapshot
        {
            ByPo = byPo,
            ByMill = byMill,
            SourceDescription = $"{database}.dbo.PO_Plan_WIP ({byPo.Count} PO(s))"
        };
    }

    public async Task<bool> EnsureImportReadyAsync(CancellationToken cancellationToken)
    {
        if (!SqlTraceabilityConnection.IsSqlEnabled(_options))
            return false;

        return await EnsureTableExistsCachedAsync(cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> IsImportSourceFilePresentAsync(string sourceFileKey, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(sourceFileKey))
            return false;

        if (!await EnsureImportReadyAsync(cancellationToken).ConfigureAwait(false))
            return false;

        await using var conn = SqlTraceabilityConnection.Create(_options);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "PO_Plan_WIP import lookup", cancellationToken)
            .ConfigureAwait(false);

        await using var cmd = new SqlCommand(ImportSourcePresentSql, conn);
        cmd.Parameters.AddWithValue("@SourceFile", sourceFileKey.Trim());
        var scalar = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return scalar is not null and not DBNull;
    }

    public async Task<int> InsertImportRowsAsync(
        string sourceFileKey,
        IEnumerable<PoPlanWipRow> rows,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(sourceFileKey))
            return 0;

        var rowList = rows.Where(r => !string.IsNullOrWhiteSpace(r.PoNumber)).ToList();
        if (rowList.Count == 0)
            return 0;

        if (!await EnsureImportReadyAsync(cancellationToken).ConfigureAwait(false))
            return 0;

        await using var conn = SqlTraceabilityConnection.Create(_options);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "PO_Plan_WIP import insert", cancellationToken)
            .ConfigureAwait(false);

        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        var inserted = 0;
        try
        {
            foreach (var row in rowList)
            {
                await using var cmd = new SqlCommand(InsertImportRowSql, conn, tx);
                cmd.Parameters.AddWithValue("@PoNumber", InputSlitCsvParsing.NormalizePo(row.PoNumber));
                cmd.Parameters.AddWithValue("@MillNo", row.MillNo is >= 1 and <= 4 ? row.MillNo : (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@PlannedMonth", NullIfEmpty(row.PlannedMonth));
                cmd.Parameters.AddWithValue("@PipeGrade", NullIfEmpty(row.PipeGrade));
                cmd.Parameters.AddWithValue("@PipeSize", NullIfEmpty(row.PipeSize));
                cmd.Parameters.AddWithValue("@PipeThickness", NullIfEmpty(row.PipeThickness));
                cmd.Parameters.AddWithValue("@PipeLength", NullIfEmpty(row.PipeLength));
                cmd.Parameters.AddWithValue("@PipeWeightPerMeter", NullIfEmpty(row.PipeWeightPerMeter));
                cmd.Parameters.AddWithValue("@PipeType", NullIfEmpty(row.PipeType));
                cmd.Parameters.AddWithValue("@OutputItemcode", NullIfEmpty(row.OutputItemcode));
                cmd.Parameters.AddWithValue("@ItemDescription", NullIfEmpty(row.ItemDescription));
                cmd.Parameters.AddWithValue("@ProductType", NullIfEmpty(row.ProductType));
                cmd.Parameters.AddWithValue("@PoSpecification", NullIfEmpty(row.PoSpecification));
                cmd.Parameters.AddWithValue("@InputWipItemcode", NullIfEmpty(row.InputWipItemcode));
                cmd.Parameters.AddWithValue("@PiecesPerBundle", TryParseNullableInt(row.PiecesPerBundle) ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@NdtPcsPerBundle", TryParseNullableInt(row.NdtPcsPerBundle) ?? (object)DBNull.Value);
                cmd.Parameters.AddWithValue("@TotalPieces", NullIfEmpty(row.TotalPieces));
                cmd.Parameters.AddWithValue("@SourceFile", sourceFileKey.Trim());
                inserted += await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            return inserted;
        }
        catch
        {
            await tx.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
            throw;
        }
    }

    public async Task<PoPlanWipRow?> TryGetLatestByPoAsync(string poNumber, CancellationToken cancellationToken)
    {
        if (!await IsAvailableAsync(cancellationToken).ConfigureAwait(false))
            return null;

        var normalized = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(normalized))
            return null;

        await using var conn = SqlTraceabilityConnection.Create(_options);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "PO_Plan_WIP by PO", cancellationToken)
            .ConfigureAwait(false);

        await using var cmd = new SqlCommand(LatestBySinglePoSql, conn);
        cmd.Parameters.AddWithValue("@PoNumber", normalized);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? MapRow(reader)
            : null;
    }

    private async Task<Dictionary<string, PoPlanWipRow>> LoadLatestByPoAsync(CancellationToken cancellationToken)
    {
        var byPo = new Dictionary<string, PoPlanWipRow>(StringComparer.OrdinalIgnoreCase);
        await using var conn = SqlTraceabilityConnection.Create(_options);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "PO_Plan_WIP latest by PO", cancellationToken)
            .ConfigureAwait(false);

        await using var cmd = new SqlCommand(LatestByPoSql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var row = MapRow(reader);
            if (string.IsNullOrWhiteSpace(row.PoNumber))
                continue;
            byPo[InputSlitCsvParsing.NormalizePo(row.PoNumber)] = row;
        }

        return byPo;
    }

    private async Task<Dictionary<int, PoPlanWipRow>> LoadLatestByMillAsync(CancellationToken cancellationToken)
    {
        var byMill = new Dictionary<int, PoPlanWipRow>();
        await using var conn = SqlTraceabilityConnection.Create(_options);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "PO_Plan_WIP latest by mill", cancellationToken)
            .ConfigureAwait(false);

        await using var cmd = new SqlCommand(LatestByMillSql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var row = MapRow(reader);
            if (row.MillNo is < 1 or > 4)
                continue;
            byMill[row.MillNo] = row;
        }

        return byMill;
    }

    private static async Task<bool> TableExistsAsync(SqlConnection conn, CancellationToken cancellationToken)
    {
        const string sql = "SELECT 1 FROM sys.tables WHERE name = N'PO_Plan_WIP' AND schema_id = SCHEMA_ID(N'dbo');";
        await using var cmd = new SqlCommand(sql, conn);
        var scalar = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return scalar is not null and not DBNull;
    }

    private static object NullIfEmpty(string? value) =>
        string.IsNullOrWhiteSpace(value) ? DBNull.Value : value.Trim();

    private static int? TryParseNullableInt(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        return int.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;
    }

    private static PoPlanWipRow MapRow(SqlDataReader reader) => PoPlanWipRowMapper.Read(reader);
}
