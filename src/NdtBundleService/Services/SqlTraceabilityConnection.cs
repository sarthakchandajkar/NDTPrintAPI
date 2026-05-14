using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace NdtBundleService.Services;

/// <summary>Opens SQL connections for traceability with a normalized connection string and clear logging.</summary>
internal static class SqlTraceabilityConnection
{
    private static readonly Regex ServerDot = new(
        @"(?i)\bServer\s*=\s*\.",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public static string NormalizeConnectionString(string? connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            return string.Empty;

        var trimmed = connectionString.Trim();
        // Some service accounts cannot reach SQL via "." / named pipes; localhost often works on the same machine.
        return ServerDot.IsMatch(trimmed)
            ? ServerDot.Replace(trimmed, "Server=localhost")
            : trimmed;
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
                "SQL traceability {Operation} failed for data source {DataSource}. Check NdtBundle:ConnectionString (or NdtBundle__ConnectionString) and that SQL Server is reachable from this service account.",
                operation,
                connection.DataSource);
            throw;
        }
    }
}
