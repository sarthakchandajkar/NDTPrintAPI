using System.Security.Principal;
using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>Opens SQL connections for traceability with a normalized connection string and clear logging.</summary>
internal static class SqlTraceabilityConnection
{
    /// <summary>Shared prefix when NetBIOS truncates the VM name (15 chars), e.g. <c>AJS-SOH-VM-PAS-</c>.</summary>
    private const string PasVmHostPrefix = "AJS-SOH-VM-PAS";

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

        var machine = GetEffectiveComputerName();
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

    /// <summary><see cref="Environment.MachineName"/> or <c>COMPUTERNAME</c> (NetBIOS, often 15 chars with trailing dash).</summary>
    public static string GetEffectiveComputerName() =>
        (Environment.GetEnvironmentVariable("COMPUTERNAME") ?? Environment.MachineName ?? string.Empty).Trim();

    private static bool IsLocalMachineHost(string hostPart, string machine)
    {
        if (HostsReferToSameMachine(hostPart, machine))
            return true;

        if (hostPart.Equals("127.0.0.1", StringComparison.OrdinalIgnoreCase)
            || hostPart.Equals("localhost", StringComparison.OrdinalIgnoreCase)
            || hostPart.Equals("(local)", StringComparison.OrdinalIgnoreCase))
            return false;

        return false;
    }

    /// <summary>
    /// Treats <c>AJS-SOH-VM-PAS</c>, <c>AJS-SOH-VM-PAS-</c>, and mistaken <c>AJS-SOH-VM-PAS-DEV</c> as the same PAS VM.
    /// </summary>
    internal static bool HostsReferToSameMachine(string? configuredHost, string? actualHost)
    {
        if (string.IsNullOrWhiteSpace(configuredHost) || string.IsNullOrWhiteSpace(actualHost))
            return false;

        if (configuredHost.Equals(actualHost, StringComparison.OrdinalIgnoreCase))
            return true;

        var a = NormalizePasHostKey(configuredHost);
        var b = NormalizePasHostKey(actualHost);
        return a.Length > 0 && a.Equals(b, StringComparison.OrdinalIgnoreCase);
    }

    internal static string NormalizePasHostKey(string host)
    {
        var h = host.Trim().TrimEnd('-');
        if (h.StartsWith(PasVmHostPrefix, StringComparison.OrdinalIgnoreCase))
            return PasVmHostPrefix;

        if (h.Length > 15)
            h = h[..15].TrimEnd('-');

        return h;
    }

    /// <summary>Windows logins to use in SSMS <c>CREATE LOGIN</c> (COMPUTERNAME + account name from the process token).</summary>
    public static SqlWindowsLoginHint GetWindowsLoginHint()
    {
        var machine = GetEffectiveComputerName();
        var current = WindowsIdentity.GetCurrent()?.Name ?? $"{Environment.UserDomainName}\\{Environment.UserName}";
        var account = GetAccountNameFromWindowsIdentity() ?? Environment.UserName ?? string.Empty;

        var candidates = new List<string>();
        if (!string.IsNullOrWhiteSpace(machine) && !string.IsNullOrWhiteSpace(account))
            candidates.Add($"{machine}\\{account}");

        if (!string.IsNullOrWhiteSpace(account))
            candidates.Add($@".\{account}");

        var trimmedMachine = machine.TrimEnd('-');
        if (!string.IsNullOrWhiteSpace(trimmedMachine)
            && !trimmedMachine.Equals(machine, StringComparison.OrdinalIgnoreCase)
            && !string.IsNullOrWhiteSpace(account))
        {
            candidates.Add($"{trimmedMachine}\\{account}");
        }

        if (!string.IsNullOrWhiteSpace(account)
            && !account.Equals(Environment.UserName, StringComparison.Ordinal)
            && !string.IsNullOrWhiteSpace(machine))
        {
            candidates.Add($"{machine}\\{Environment.UserName}");
        }

        return new SqlWindowsLoginHint(
            machine,
            current,
            candidates.Distinct(StringComparer.OrdinalIgnoreCase).ToList());
    }

    private static string? GetAccountNameFromWindowsIdentity()
    {
        var name = WindowsIdentity.GetCurrent()?.Name;
        if (string.IsNullOrWhiteSpace(name))
            return null;

        var idx = name.IndexOf('\\');
        return idx >= 0 ? name[(idx + 1)..] : name;
    }

    public sealed record SqlWindowsLoginHint(
        string MachineName,
        string CurrentProcessLogin,
        IReadOnlyList<string> SuggestedCreateLoginNames);

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
            var hint = GetWindowsLoginHint();
            logger.LogError(
                ex,
                "SQL traceability {Operation} failed for data source {DataSource}, database {Database}. COMPUTERNAME={MachineName}, process login={ProcessLogin}. Grant SQL using suggested login (e.g. {PreferredLogin}). Check NdtBundle:ConnectionString.",
                operation,
                connection.DataSource,
                connection.Database,
                hint.MachineName,
                hint.CurrentProcessLogin,
                hint.SuggestedCreateLoginNames.FirstOrDefault() ?? hint.CurrentProcessLogin);
            throw;
        }
    }
}
