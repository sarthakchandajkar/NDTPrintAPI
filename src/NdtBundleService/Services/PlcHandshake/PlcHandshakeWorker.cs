using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.PlcHandshake.PlcPoEnd;

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
    private readonly IMillHooterPlcValuesService _hooterValues;
    private readonly IWipBundleRunningPoProvider _wipRunningPo;
    private readonly PlcPoEndQueue _plcPoEndQueue;
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
        IMillHooterPlcValuesService hooterValues,
        IWipBundleRunningPoProvider wipRunningPo,
        PlcPoEndQueue plcPoEndQueue,
        ILoggerFactory loggerFactory,
        ILogger<PlcHandshakeWorker> logger)
    {
        _options = options;
        _poChangeHandler = poChangeHandler;
        _statusRegistry = statusRegistry;
        _connectionHealth = connectionHealth;
        _coordinator = coordinator;
        _activePoPerMill = activePoPerMill;
        _hooterValues = hooterValues;
        _wipRunningPo = wipRunningPo;
        _plcPoEndQueue = plcPoEndQueue;
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

        var allMills = handshake.Mills
            .Where(m => !string.IsNullOrWhiteSpace(m.IpAddress))
            .ToList();

        foreach (var excluded in allMills.Where(m => !m.PlcHandshakeEnabled))
        {
            _logger.LogInformation(
                "PlcHandshake skipped for {MillName} (Mill {MillNo}): PlcHandshakeEnabled=false — no S7 connection (PoEndSource={PoEndSource}).",
                excluded.Name,
                excluded.ResolveMillNo(),
                MillPoEndSourceResolver.ToConfigValue(excluded.ResolvePoEndSource(_options.Value)));
        }

        var mills = allMills.Where(m => m.PlcHandshakeEnabled).ToList();

        if (mills.Count == 0)
        {
            _logger.LogWarning(
                "PlcHandshake.Enabled is true but no mills have PlcHandshakeEnabled=true with IpAddress configured.");
            return;
        }

        if (handshake.TelemetryOnly)
        {
            _logger.LogInformation(
                "PlcHandshakeWorker starting {Count} mill loop(s) (default poll {Poll}ms) — global TelemetryOnly: all mills S7 read-only.",
                mills.Count,
                handshake.PollIntervalMs);
        }
        else
        {
            _logger.LogInformation(
                "PlcHandshakeWorker starting {Count} mill loop(s) (default poll {Poll}ms).",
                mills.Count,
                handshake.PollIntervalMs);
        }

        foreach (var mill in mills)
        {
            var millNo = mill.ResolveMillNo();
            if (millNo is < 1 or > 4)
                continue;

            var source = mill.ResolvePoEndSource(_options.Value);
            _logger.LogInformation(
                "{MillName} (Mill {MillNo}): PoEndSource={PoEndSource} — {Description}.",
                mill.Name,
                millNo,
                MillPoEndSourceResolver.ToConfigValue(source),
                MillPoEndSourceResolver.Describe(source));
        }

        foreach (var mill in mills)
        {
            var service = new PlcHandshakeService(
                mill,
                handshake,
                _options.Value,
                _poChangeHandler,
                _plcPoEndQueue,
                _statusRegistry,
                _connectionHealth,
                _activePoPerMill,
                _hooterValues,
                _wipRunningPo,
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
