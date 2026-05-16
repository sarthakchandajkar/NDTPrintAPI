using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>Logs once at startup whether JazeeraMES_Prod traceability SQL is reachable and tables exist.</summary>
public sealed class SqlTraceabilityStartupCheck : IHostedService
{
    private readonly ISqlTraceabilityHealth _health;
    private readonly ILogger<SqlTraceabilityStartupCheck> _logger;

    public SqlTraceabilityStartupCheck(ISqlTraceabilityHealth health, ILogger<SqlTraceabilityStartupCheck> logger)
    {
        _health = health;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var report = await _health.GetReportAsync(cancellationToken).ConfigureAwait(false);

        if (!report.Enabled)
        {
            _logger.LogInformation("SQL traceability is disabled (NdtBundle:UseSqlServerForBundles=false or no connection string).");
            return;
        }

        _logger.LogInformation(
            "SQL traceability configured for Server={Server} Database={Database} (expected {Expected}).",
            report.ConfiguredServer ?? "(unknown)",
            report.ConfiguredDatabase ?? "(unknown)",
            SqlTraceabilityHealth.ExpectedDatabaseName);

        if (!string.IsNullOrWhiteSpace(report.Error) && !report.Connected)
        {
            _logger.LogError(
                "SQL traceability is enabled but not reachable: {Error}. Set NdtBundle:ConnectionString (or NdtBundle__ConnectionString) to a working Server with Database={Database}.",
                report.Error,
                SqlTraceabilityHealth.ExpectedDatabaseName);
            return;
        }

        if (!report.Connected)
            return;

        _logger.LogInformation(
            "SQL traceability connected to {DataSource}, database {Database} (expected {Expected}).",
            report.DataSource,
            report.Database,
            SqlTraceabilityHealth.ExpectedDatabaseName);

        if (!report.IsExpectedDatabase)
        {
            _logger.LogWarning(
                "Connection is open but database is {Actual}, not {Expected}. Traceability writes may go to the wrong database.",
                report.Database,
                SqlTraceabilityHealth.ExpectedDatabaseName);
        }

        if (report.MissingTables.Count > 0)
        {
            _logger.LogError(
                "SQL traceability tables missing in {Database}: {Tables}. Run docs/NDT_Traceability_Schema.sql against JazeeraMES_Prod.",
                report.Database,
                string.Join(", ", report.MissingTables));
            return;
        }

        foreach (var kv in report.RowCounts)
            _logger.LogInformation("SQL traceability table {Table}: {Count} row(s).", kv.Key, kv.Value);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
