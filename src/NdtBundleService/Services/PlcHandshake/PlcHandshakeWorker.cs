using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services.PlcHandshake;

/// <summary>
/// Starts one <see cref="PlcHandshakeService"/> per configured mill (each on its own task).
/// </summary>
public sealed class PlcHandshakeWorker : BackgroundService
{
    private readonly IOptions<NdtBundleOptions> _options;
    private readonly IPoChangeHandler _poChangeHandler;
    private readonly PlcHandshakeStatusRegistry _statusRegistry;
    private readonly PlcConnectionHealth _connectionHealth;
    private readonly PlcHandshakeCoordinator _coordinator;
    private readonly IActivePoPerMillService _activePoPerMill;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<PlcHandshakeWorker> _logger;

    private readonly List<PlcHandshakeService> _services = new();
    private readonly List<Task> _tasks = new();

    public PlcHandshakeWorker(
        IOptions<NdtBundleOptions> options,
        IPoChangeHandler poChangeHandler,
        PlcHandshakeStatusRegistry statusRegistry,
        PlcConnectionHealth connectionHealth,
        PlcHandshakeCoordinator coordinator,
        IActivePoPerMillService activePoPerMill,
        ILoggerFactory loggerFactory,
        ILogger<PlcHandshakeWorker> logger)
    {
        _options = options;
        _poChangeHandler = poChangeHandler;
        _statusRegistry = statusRegistry;
        _connectionHealth = connectionHealth;
        _coordinator = coordinator;
        _activePoPerMill = activePoPerMill;
        _loggerFactory = loggerFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var handshake = _options.Value.PlcHandshake ?? new PlcHandshakeOptions();
        if (!handshake.Enabled)
        {
            _logger.LogInformation("PlcHandshake is disabled; PlcHandshakeWorker will not start mill loops.");
            return;
        }

        var mills = handshake.Mills
            .Where(m => !string.IsNullOrWhiteSpace(m.IpAddress))
            .ToList();

        if (mills.Count == 0)
        {
            _logger.LogWarning("PlcHandshake.Enabled is true but no mills with IpAddress are configured.");
            return;
        }

        _logger.LogInformation(
            "PlcHandshakeWorker starting {Count} mill handshake loop(s) (default poll {Poll}ms).",
            mills.Count,
            handshake.PollIntervalMs);

        foreach (var mill in mills)
        {
            var service = new PlcHandshakeService(
                mill,
                handshake,
                _poChangeHandler,
                _statusRegistry,
                _connectionHealth,
                _activePoPerMill,
                _loggerFactory.CreateLogger<PlcHandshakeService>());

            var millNo = mill.ResolveMillNo();
            if (millNo is >= 1 and <= 4)
                _coordinator.Register(service, millNo);

            _services.Add(service);
            _tasks.Add(service.RunAsync(stoppingToken));
        }

        try
        {
            await Task.WhenAll(_tasks).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            /* expected on shutdown */
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("PlcHandshakeWorker stopping {Count} mill loop(s).", _tasks.Count);
        await base.StopAsync(cancellationToken).ConfigureAwait(false);
    }
}
