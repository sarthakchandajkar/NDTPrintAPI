using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

public interface ISqlTraceabilityHealth
{
    Task<SqlTraceabilityHealthReport> GetReportAsync(CancellationToken cancellationToken);
}

public sealed class SqlTraceabilityHealthReport
{
    public bool Enabled { get; init; }
    public bool Connected { get; init; }
    public string? Database { get; init; }
    public string? DataSource { get; init; }
    public bool IsExpectedDatabase { get; init; }
    public IReadOnlyList<string> MissingTables { get; init; } = Array.Empty<string>();
    public IReadOnlyDictionary<string, long> RowCounts { get; init; } = new Dictionary<string, long>();
    public IReadOnlyList<SqlTraceabilityBundleSample> RecentBundles { get; init; } = Array.Empty<SqlTraceabilityBundleSample>();
    public string? Error { get; init; }
}

public sealed class SqlTraceabilityBundleSample
{
    public string BundleNo { get; init; } = string.Empty;
    public string PoNumber { get; init; } = string.Empty;
    public int MillNo { get; init; }
    public int TotalNdtPcs { get; init; }
    public DateTime? PrintedAt { get; init; }
}

public sealed class SqlTraceabilityHealth : ISqlTraceabilityHealth
{
    public const string ExpectedDatabaseName = "JazeeraMES_Prod";

    private static readonly string[] RequiredTables =
    {
        "NDT_Bundle",
        "Input_Slit_Row",
        "Output_Slit_Row",
        "Bundle_Label",
        "Manual_Station_Run",
        "NDT_Process_Consolidated"
    };

    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly ILogger<SqlTraceabilityHealth> _logger;

    public SqlTraceabilityHealth(IOptionsMonitor<NdtBundleOptions> optionsMonitor, ILogger<SqlTraceabilityHealth> logger)
    {
        _optionsMonitor = optionsMonitor;
        _logger = logger;
    }

    public async Task<SqlTraceabilityHealthReport> GetReportAsync(CancellationToken cancellationToken)
    {
        var opt = _optionsMonitor.CurrentValue;
        if (!opt.UseSqlServerForBundles)
        {
            return new SqlTraceabilityHealthReport
            {
                Enabled = false,
                Error = "NdtBundle:UseSqlServerForBundles is false."
            };
        }

        if (string.IsNullOrWhiteSpace(opt.ConnectionString))
        {
            return new SqlTraceabilityHealthReport
            {
                Enabled = true,
                Error = "NdtBundle:ConnectionString is empty. Set NdtBundle__ConnectionString on the server."
            };
        }

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(opt.ConnectionString);
            await SqlTraceabilityConnection.OpenAsync(conn, _logger, "health check", cancellationToken).ConfigureAwait(false);

            var database = conn.Database;
            var isExpectedDb = string.Equals(database, ExpectedDatabaseName, StringComparison.OrdinalIgnoreCase);

            var missingTables = new List<string>();
            foreach (var table in RequiredTables)
            {
                if (!await TableExistsAsync(conn, table, cancellationToken).ConfigureAwait(false))
                    missingTables.Add(table);
            }

            var rowCounts = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
            foreach (var table in RequiredTables)
            {
                if (missingTables.Contains(table, StringComparer.OrdinalIgnoreCase))
                    continue;

                rowCounts[table] = await CountRowsAsync(conn, table, cancellationToken).ConfigureAwait(false);
            }

            var recentBundles = missingTables.Contains("NDT_Bundle", StringComparer.OrdinalIgnoreCase)
                ? Array.Empty<SqlTraceabilityBundleSample>()
                : await LoadRecentBundlesAsync(conn, cancellationToken).ConfigureAwait(false);

            return new SqlTraceabilityHealthReport
            {
                Enabled = true,
                Connected = true,
                Database = database,
                DataSource = conn.DataSource,
                IsExpectedDatabase = isExpectedDb,
                MissingTables = missingTables,
                RowCounts = rowCounts,
                RecentBundles = recentBundles
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SQL traceability health check failed.");
            return new SqlTraceabilityHealthReport
            {
                Enabled = true,
                Connected = false,
                Error = ex.Message
            };
        }
    }

    private static async Task<bool> TableExistsAsync(SqlConnection conn, string tableName, CancellationToken cancellationToken)
    {
        const string sql = "SELECT 1 FROM sys.tables WHERE name = @Name AND schema_id = SCHEMA_ID('dbo');";
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@Name", tableName);
        var scalar = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return scalar is not null and not DBNull;
    }

    private static async Task<long> CountRowsAsync(SqlConnection conn, string tableName, CancellationToken cancellationToken)
    {
        var sql = $"SELECT COUNT_BIG(1) FROM dbo.[{tableName.Replace("]", "]]", StringComparison.Ordinal)}];";
        await using var cmd = new SqlCommand(sql, conn);
        var scalar = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return scalar is null or DBNull ? 0L : Convert.ToInt64(scalar, System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<IReadOnlyList<SqlTraceabilityBundleSample>> LoadRecentBundlesAsync(
        SqlConnection conn,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT TOP 5 Bundle_No, PO_Number, Mill_No, Total_NDT_Pcs, PrintedAt
FROM dbo.NDT_Bundle
ORDER BY PrintedAt DESC;";

        var list = new List<SqlTraceabilityBundleSample>();
        await using var cmd = new SqlCommand(sql, conn);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            list.Add(new SqlTraceabilityBundleSample
            {
                BundleNo = reader.GetString(0),
                PoNumber = reader.GetString(1),
                MillNo = reader.GetInt32(2),
                TotalNdtPcs = reader.GetInt32(3),
                PrintedAt = reader.IsDBNull(4) ? null : reader.GetDateTime(4)
            });
        }

        return list;
    }
}
