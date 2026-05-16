using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>Opens SQL connections for traceability with a normalized connection string and clear logging.</summary>
internal static class SqlTraceabilityConnection
{
    private static readonly Regex ServerDot = new(
        @"(?i)\bServer\s*=\s*\.",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public static bool IsSqlEnabled(NdtBundleOptions options) =>
        options.UseSqlServerForBundles && !string.IsNullOrWhiteSpace(ResolveConnectionString(options));

    public static string? ResolveConnectionString(NdtBundleOptions options)
    {
        if (!options.UseSqlServerForBundles)
            return null;

        var configured = (options.ConnectionString ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(configured))
            return NormalizeConnectionString(configured);

        var server = (options.SqlServer ?? string.Empty).Trim();
        var database = (options.SqlDatabase ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(server) || string.IsNullOrWhiteSpace(database))
            return null;

        return NormalizeConnectionString(
            $"Server={server};Database={database};Trusted_Connection=True;TrustServerCertificate=True;Connect Timeout=15;");
    }

    public static (string? Server, string? Database) DescribeConnectionString(string? connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            return (null, null);

        try
        {
            var builder = new SqlConnectionStringBuilder(NormalizeConnectionString(connectionString));
            return (builder.DataSource, builder.InitialCatalog);
        }
        catch
        {
            return (null, null);
        }
    }

    public static string NormalizeConnectionString(string? connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            return string.Empty;

        var trimmed = connectionString.Trim();
        if (ServerDot.IsMatch(trimmed))
            trimmed = ServerDot.Replace(trimmed, "Server=localhost\\SQLEXPRESS");

        var builder = new SqlConnectionStringBuilder(trimmed);
        if (builder.ConnectTimeout < 15)
            builder.ConnectTimeout = 15;

        return builder.ConnectionString;
    }

    public static SqlConnection Create(NdtBundleOptions options)
    {
        var cs = ResolveConnectionString(options)
            ?? throw new InvalidOperationException("SQL traceability is not configured (NdtBundle:ConnectionString or SqlServer+SqlDatabase).");
        return new SqlConnection(cs);
    }

    public static SqlConnection Create(string? connectionString) =>
        new(NormalizeConnectionString(connectionString));

    public static async Task OpenAsync(
        SqlConnection connection,
        ILogger logger,
        string operation,
        CancellationToken cancellationToken)
    {
        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (SqlException ex)
        {
            logger.LogError(
                ex,
                "SQL traceability {Operation} failed for data source {DataSource}, database {Database}. Check NdtBundle:ConnectionString (or NdtBundle__ConnectionString) and that the Windows service account can log in to SQL Server.",
                operation,
                connection.DataSource,
                connection.Database);
            throw;
        }
    }
}
