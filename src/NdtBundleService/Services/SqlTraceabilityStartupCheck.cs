using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>Logs once at startup whether JazeeraMES traceability SQL is reachable.</summary>
public sealed class SqlTraceabilityStartupCheck : IHostedService
{
    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly ILogger<SqlTraceabilityStartupCheck> _logger;

    public SqlTraceabilityStartupCheck(IOptionsMonitor<NdtBundleOptions> optionsMonitor, ILogger<SqlTraceabilityStartupCheck> logger)
    {
        _optionsMonitor = optionsMonitor;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var opt = _optionsMonitor.CurrentValue;
        if (!opt.UseSqlServerForBundles)
        {
            _logger.LogInformation("SQL traceability is disabled (NdtBundle:UseSqlServerForBundles=false).");
            return;
        }

        if (string.IsNullOrWhiteSpace(opt.ConnectionString))
        {
            _logger.LogWarning("SQL traceability is enabled but NdtBundle:ConnectionString is empty.");
            return;
        }

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(opt.ConnectionString);
            await SqlTraceabilityConnection.OpenAsync(conn, _logger, "startup connectivity check", cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("SQL traceability connected to {DataSource} (database {Database}).", conn.DataSource, conn.Database);
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "SQL traceability is enabled but the database is not reachable at startup. Input_Slit_Row, Bundle_Label, and Revisual SQL writes will fail until NdtBundle:ConnectionString (or NdtBundle__ConnectionString) is corrected.");
        }
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
