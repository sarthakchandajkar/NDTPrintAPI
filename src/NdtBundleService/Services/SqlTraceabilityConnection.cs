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
        builder.DataSource = PreferLocalhostWhenOnSameMachine(builder.DataSource);
        if (builder.ConnectTimeout < 15)
            builder.ConnectTimeout = 15;

        return builder.ConnectionString;
    }

    /// <summary>
    /// SSMS can resolve the VM hostname + named instance; the Windows service often cannot (SQL Browser / error 26).
    /// When the app runs on the SQL host, use localhost so SQLEXPRESS is located reliably.
    /// </summary>
    private static string PreferLocalhostWhenOnSameMachine(string dataSource)
    {
        if (string.IsNullOrWhiteSpace(dataSource))
            return dataSource;

        var machine = Environment.MachineName;
        if (string.IsNullOrWhiteSpace(machine))
            return dataSource;

        var slash = dataSource.IndexOf('\\', StringComparison.Ordinal);
        var comma = dataSource.IndexOf(',', StringComparison.Ordinal);
        var splitAt = slash >= 0 && comma >= 0 ? Math.Min(slash, comma)
            : slash >= 0 ? slash
            : comma >= 0 ? comma
            : -1;
        var hostPart = splitAt >= 0 ? dataSource[..splitAt] : dataSource;
        var suffix = splitAt >= 0 ? dataSource[splitAt..] : string.Empty;

        if (!IsLocalMachineHost(hostPart, machine))
            return dataSource;

        return "localhost" + suffix;
    }

    private static bool IsLocalMachineHost(string hostPart, string machine)
    {
        if (hostPart.Equals(machine, StringComparison.OrdinalIgnoreCase))
            return true;

        if (hostPart.Equals("127.0.0.1", StringComparison.OrdinalIgnoreCase)
            || hostPart.Equals("localhost", StringComparison.OrdinalIgnoreCase)
            || hostPart.Equals("(local)", StringComparison.OrdinalIgnoreCase))
            return false;

        // NetBIOS name is 15 chars; SSMS/login may show AJS-SOH-VM-PAS while MachineName is AJS-SOH-VM-PAS-DEV.
        if (machine.Length > 15
            && hostPart.Equals(machine[..15], StringComparison.OrdinalIgnoreCase))
            return true;

        var computerName = Environment.GetEnvironmentVariable("COMPUTERNAME");
        return !string.IsNullOrWhiteSpace(computerName)
            && hostPart.Equals(computerName, StringComparison.OrdinalIgnoreCase);
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
