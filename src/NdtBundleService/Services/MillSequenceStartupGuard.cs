using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Seeds missing <c>Mill_Sequence</c> rows, then refuses to start when live bundles exceed the table
/// (same refuse-by-default pattern as <see cref="FillCutoverStartupCheck"/>).
/// </summary>
public sealed class MillSequenceStartupGuard : IHostedService
{
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly IMillSequenceService _millSequence;
    private readonly IMillOwnership _ownership;
    private readonly ILogger<MillSequenceStartupGuard> _logger;

    public MillSequenceStartupGuard(
        IOptionsMonitor<NdtBundleOptions> options,
        IMillSequenceService millSequence,
        IMillOwnership ownership,
        ILogger<MillSequenceStartupGuard> logger)
    {
        _options = options;
        _millSequence = millSequence;
        _ownership = ownership;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (!_millSequence.IsEnabled)
        {
            if (SqlOffPrintGuard.MustRefuseStart(_options.CurrentValue))
            {
                _logger.LogError("{Message}", SqlOffPrintGuard.RefuseMessage);
                throw new InvalidOperationException(SqlOffPrintGuard.RefuseMessage);
            }

            _logger.LogInformation("Mill_Sequence guard skipped (SQL bundles disabled).");
            return;
        }

        await _millSequence.SeedMissingRowsAsync(cancellationToken).ConfigureAwait(false);

        if (!_options.CurrentValue.RequireMillSequenceMatchesBundles)
        {
            _logger.LogWarning(
                "RequireMillSequenceMatchesBundles=false — skipping live-bundle vs Mill_Sequence check.");
            return;
        }

        var millNo = _ownership.SingleOwnedMill;
        var mills = millNo.HasValue
            ? new[] { millNo.Value }
            : new[] { 1, 2, 3, 4 };

        foreach (var mill in mills)
        {
            if (!_ownership.Owns(mill))
                continue;

            try
            {
                await _millSequence.EnsureScanDoesNotExceedTableAsync(mill, cancellationToken).ConfigureAwait(false);
            }
            catch (InvalidOperationException ex)
            {
                _logger.LogError(ex, "Mill_Sequence startup guard blocked for mill {Mill}.", mill);
                throw;
            }
        }

        _logger.LogInformation(
            "Mill_Sequence startup guard passed{Scope}.",
            millNo.HasValue ? $" (mill {millNo.Value})" : "");
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
