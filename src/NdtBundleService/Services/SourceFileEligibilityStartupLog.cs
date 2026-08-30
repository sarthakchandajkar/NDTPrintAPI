using System.Globalization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>Logs the effective <see cref="NdtBundleOptions.MinSourceFileLastWriteUtc"/> floor once at startup.</summary>
public sealed class SourceFileEligibilityStartupLog : IHostedService
{
    private readonly IOptions<NdtBundleOptions> _options;
    private readonly ILogger<SourceFileEligibilityStartupLog> _logger;

    public SourceFileEligibilityStartupLog(
        IOptions<NdtBundleOptions> options,
        ILogger<SourceFileEligibilityStartupLog> logger)
    {
        _options = options;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        SourceFileEligibility.TryParseMinUtcFromRaw(
            _options.Value.MinSourceFileLastWriteUtc,
            out var minUtc);

        _logger.LogInformation(
            "NdtBundle:MinSourceFileLastWriteUtc floor is {Floor}.",
            minUtc is DateTime utc
                ? utc.ToString("o", CultureInfo.InvariantCulture)
                : "none");

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
