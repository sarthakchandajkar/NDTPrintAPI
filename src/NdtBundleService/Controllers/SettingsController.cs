using System.Net.Sockets;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.MillInstanceProxy;
using NdtBundleService.Services.MillInstanceStatus;
using NdtBundleService.Services.PlcHandshake;

namespace NdtBundleService.Controllers;

/// <summary>
/// Password-protected dashboard settings: formation chart thresholds, PLC checks, per-mill printer IPs.
/// </summary>
[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
[InstanceRole(InstanceRoleModes.Monolith, InstanceRoleModes.Shared, InstanceRoleModes.Mill)]
public sealed class SettingsController : ControllerBase
{
    private readonly SettingsAuthService _auth;
    private readonly IFormationChartSettingsService _formationChart;
    private readonly IMillPrinterSettingsService _millPrinters;
    private readonly IStationPrinterSettingsService? _stationPrinters;
    private readonly IPlcClient _plcClient;
    private readonly PlcConnectionHealth _plcHealth;
    private readonly PoEndDetectionDiagnostics _poEndDiagnostics;
    private readonly PlcHandshakeStatusRegistry _handshakeStatus;
    private readonly PlcHandshakeCoordinator _handshakeCoordinator;
    private readonly IMillHooterPlcValuesService _hooterValues;
    private readonly OpenAccumulationOverrideService _openAccumulationOverride;
    private readonly INetworkPrinterSender _networkPrinterSender;
    private readonly IZplGenerationToggle _zplToggle;
    private readonly IMillSequenceService _millSequence;
    private readonly IMillSettingsPlcProxy _millPlcProxy;
    private readonly IPlcLiveSnapshotService _plcLive;
    private readonly IOptionsMonitor<InstanceRoleOptions> _role;
    private readonly NdtBundleOptions _options;
    private readonly ILogger<SettingsController> _logger;

    private static readonly JsonSerializerOptions ProxyJsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    public SettingsController(
        SettingsAuthService auth,
        IFormationChartSettingsService formationChart,
        IMillPrinterSettingsService millPrinters,
        IPlcClient plcClient,
        PlcConnectionHealth plcHealth,
        PoEndDetectionDiagnostics poEndDiagnostics,
        PlcHandshakeStatusRegistry handshakeStatus,
        PlcHandshakeCoordinator handshakeCoordinator,
        IMillHooterPlcValuesService hooterValues,
        OpenAccumulationOverrideService openAccumulationOverride,
        INetworkPrinterSender networkPrinterSender,
        IZplGenerationToggle zplToggle,
        IMillSequenceService millSequence,
        IMillSettingsPlcProxy millPlcProxy,
        IPlcLiveSnapshotService plcLive,
        IOptionsMonitor<InstanceRoleOptions> role,
        IOptions<NdtBundleOptions> options,
        ILogger<SettingsController> logger,
        IStationPrinterSettingsService? stationPrinters = null)
    {
        _auth = auth;
        _formationChart = formationChart;
        _millPrinters = millPrinters;
        _stationPrinters = stationPrinters;
        _plcClient = plcClient;
        _plcHealth = plcHealth;
        _poEndDiagnostics = poEndDiagnostics;
        _handshakeStatus = handshakeStatus;
        _handshakeCoordinator = handshakeCoordinator;
        _hooterValues = hooterValues;
        _openAccumulationOverride = openAccumulationOverride;
        _networkPrinterSender = networkPrinterSender;
        _zplToggle = zplToggle;
        _millSequence = millSequence;
        _millPlcProxy = millPlcProxy;
        _plcLive = plcLive;
        _role = role;
        _options = options.Value;
        _logger = logger;
    }

    [HttpGet("status")]
    public IActionResult GetStatus()
    {
        var token = GetToken();
        return Ok(new
        {
            configured = _auth.IsConfigured,
            authenticated = _auth.ValidateToken(token),
            message = !_auth.IsConfigured
                ? "Set NdtBundle:DashboardSettings:AdminPassword in appsettings.Production.json (or env) to enable Settings."
                : null
        });
    }

    [HttpPost("login")]
    public IActionResult Login([FromBody] SettingsLoginRequest request)
    {
        if (!_auth.IsConfigured)
            return StatusCode(503, new { Message = "Settings admin password is not configured on the server." });

        if (request is null || string.IsNullOrWhiteSpace(request.Password))
            return BadRequest(new { Message = "Password is required." });

        if (!_auth.TryLogin(request.Password, out var token, out var expiresUtc))
            return Unauthorized(new { Message = "Invalid password." });

        return Ok(new { Token = token, ExpiresUtc = expiresUtc });
    }

    [HttpPost("logout")]
    public IActionResult Logout()
    {
        _auth.Revoke(GetToken());
        return Ok(new { Message = "Logged out." });
    }

    [HttpGet("formation-chart")]
    public async Task<IActionResult> GetFormationChart(CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        var entries = await _formationChart.GetEntriesAsync(cancellationToken).ConfigureAwait(false);
        var resolvedPath = FormationChartPathResolver.Resolve(_options);
        return Ok(new
        {
            Entries = entries.Select(e => new { pipeSize = e.PipeSize, requiredNdtPcs = e.RequiredNdtPcs }),
            SourcePath = _options.FormationChartCsvPath,
            ResolvedPath = resolvedPath
        });
    }

    [HttpPut("formation-chart")]
    public async Task<IActionResult> PutFormationChart(
        [FromBody] SaveFormationChartRequest request,
        CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        if (request?.Entries is null || request.Entries.Count == 0)
            return BadRequest(new { Message = "At least one pipe size row is required." });

        try
        {
            var entries = request.Entries
                .Where(e => !string.IsNullOrWhiteSpace(e.PipeSize))
                .Select(e => new FormationChartEntry
                {
                    PipeSize = e.PipeSize!.Trim(),
                    RequiredNdtPcs = e.RequiredNdtPcs
                })
                .ToList();

            await _formationChart.SaveEntriesAsync(entries, cancellationToken).ConfigureAwait(false);
            return Ok(new { Message = "Formation chart saved.", Count = entries.Count });
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Formation chart save failed.");
            return BadRequest(new { Message = ex.Message });
        }
    }

    [HttpGet("plc")]
    public async Task<IActionResult> GetPlcDiagnostics(
        [FromQuery] bool live,
        CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        // Dashboard polls while PLC tab is open; do not abort on client disconnect.
        var loadToken = CancellationToken.None;

        try
        {
            if (live)
                return await GetPlcDiagnosticsCoreAsync(loadToken, liveOnly: true).ConfigureAwait(false);

            await FullPlcDiagnosticsGate.WaitAsync(loadToken).ConfigureAwait(false);
            try
            {
                return await GetPlcDiagnosticsCoreAsync(loadToken, liveOnly: false).ConfigureAwait(false);
            }
            finally
            {
                FullPlcDiagnosticsGate.Release();
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GetPlcDiagnostics failed.");
            return StatusCode(500, new { Message = "Failed to load PLC diagnostics.", Error = ex.Message });
        }
    }

    private async Task<IActionResult> GetPlcDiagnosticsCoreAsync(CancellationToken cancellationToken, bool liveOnly)
    {
        // Shared has no S7 loop — Settings UI is filled from Mill_Instance_Status + PlcHandshake.Mills (display/hooter config).
        if (_role.CurrentValue.IsShared)
            return await GetSharedPlcDiagnosticsAsync(cancellationToken, liveOnly).ConfigureAwait(false);

        var handshakeCfg = _options.PlcHandshake ?? new PlcHandshakeOptions();
        if (handshakeCfg.Enabled)
        {
            var snapshot = _handshakeStatus.GetSnapshot();
            var poEndByMill = _handshakeStatus.GetPoEndByMill();
            var mills = new List<object>();
            var millsCfg = handshakeCfg.Mills ?? new List<MillConfig>();

            foreach (var cfg in millsCfg.OrderBy(m => m.ResolveMillNo()))
            {
                var millNo = cfg.ResolveMillNo();
                if (millNo is < 1 or > 4)
                    continue;

                var live = snapshot.FirstOrDefault(s => s.MillNo == millNo);
                var host = (cfg.IpAddress ?? string.Empty).Trim();
                var handshakeConnected = live?.Connected ?? false;
                bool reachable;
                if (!cfg.PlcHandshakeEnabled)
                {
                    reachable = false;
                }
                else if (liveOnly)
                {
                    // Auto-refresh: use handshake connection state only (no extra TCP clients).
                    reachable = handshakeConnected;
                }
                else if (handshakeConnected)
                {
                    reachable = true;
                }
                else
                {
                    reachable = !string.IsNullOrEmpty(host) &&
                                await TcpProbeCachedAsync(host, 102, cancellationToken).ConfigureAwait(false);
                }

                object? mesHooter = null;
                if (!liveOnly && cfg.Hooter?.Enabled == true && millNo is >= 1 and <= 4)
                {
                    try
                    {
                        var resolved = await _hooterValues.ResolveAsync(millNo, cancellationToken).ConfigureAwait(false);
                        mesHooter = new
                        {
                            poNumber = resolved.PoNumber,
                            pipeSize = resolved.PipeSize,
                            threshold = resolved.Threshold,
                            accumulated = resolved.Accumulated,
                            bundleNear = resolved.Threshold > 0 &&
                                         resolved.Accumulated > resolved.Threshold
                        };
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "MES hooter values resolve failed for mill {Mill} during PLC diagnostics.", millNo);
                    }
                }

                var poEndSource = cfg.ResolvePoEndSource(_options);
                mills.Add(new
                {
                    millNo,
                    name = cfg.Name,
                    poEndSource = MillPoEndSourceResolver.ToConfigValue(poEndSource),
                    poEndSourceDescription = MillPoEndSourceResolver.Describe(poEndSource),
                    tcpOpenCommHost = poEndSource == MillPoEndSource.TcpOpen ? cfg.ResolveTcpOpenHost() : null,
                    tcpOpenPort = poEndSource == MillPoEndSource.TcpOpen ? cfg.ResolveTcpOpenPort() : (int?)null,
                    tcpOpenConnectTimeoutMs = poEndSource == MillPoEndSource.TcpOpen ? cfg.TcpOpenConnectTimeoutMs : (int?)null,
                    tcpOpenReceiveTimeoutMs = poEndSource == MillPoEndSource.TcpOpen ? cfg.TcpOpenReceiveTimeoutMs : (int?)null,
                    driver = poEndSource == MillPoEndSource.TcpOpen ? "TcpOpen" : "S7-Handshake",
                    host,
                    port = 102,
                    reachable,
                    poEndAddress = cfg.TriggerAddress,
                    mesAckAddress = cfg.AckAddress,
                    handshakeConnected,
                    plcConnectionEnabled = live?.PlcConnectionEnabled ?? cfg.PlcHandshakeEnabled,
                    triggerActive = live?.TriggerActive ?? false,
                    ackActive = live?.AckActive ?? false,
                    handshakeState = live?.HandshakeState
                        ?? (cfg.PlcHandshakeEnabled ? "Unknown" : "Excluded (S7 disabled)"),
                    lastPoChangeUtc = live?.LastPoChangeUtc,
                    lastError = live?.LastError,
                    testAvailable = _handshakeCoordinator.IsMillRegistered(millNo),
                    lineRunning = live?.LineRunning,
                    lineRunningAddress = FormatLineRunningAddress(handshakeCfg),
                    accumulatedValue = live?.AccumulatedValue,
                    thresholdValue = live?.ThresholdValue,
                    hooterActive = live?.HooterActive ?? false,
                    okCount = live?.OkCount,
                    nokCount = live?.NokCount,
                    ndtCount = live?.NdtCount,
                    poId = live?.PoId,
                    countsUpdatedUtc = live?.CountsUpdatedUtc,
                    hooterEnabled = cfg.Hooter?.Enabled ?? false,
                    hooterAccumAddress = cfg.Hooter?.Enabled == true
                        ? $"MW{cfg.Hooter.AccumulatedWordOffset}"
                        : null,
                    hooterThresholdAddress = cfg.Hooter?.Enabled == true
                        ? $"MW{cfg.Hooter.ThresholdWordOffset}"
                        : null,
                    hooterOutputAddress = cfg.Hooter?.Enabled == true
                        ? $"Q{cfg.Hooter.OutputByte}.{cfg.Hooter.OutputBit}"
                        : null,
                    hooterPasEnableAddress = cfg.Hooter?.Enabled == true
                        ? $"DB{cfg.Hooter.PasEnableDbNumber}.DBX{cfg.Hooter.PasEnableByteOffset}.{cfg.Hooter.PasEnableBit}"
                        : null,
                    hooterDurationMs = cfg.Hooter?.DurationMs,
                    mesHooter
                });
            }

            var poEndSourceByMill = Enumerable.Range(1, 4).ToDictionary(
                m => m.ToString(),
                m =>
                {
                    var src = MillPoEndSourceResolver.ForMill(m, _options);
                    return new
                    {
                        source = MillPoEndSourceResolver.ToConfigValue(src),
                        description = MillPoEndSourceResolver.Describe(src)
                    };
                });

            return Ok(new
            {
                plcPoEndEnabled = true,
                plcHandshakeEnabled = true,
                plcHandshakeTelemetryOnly = handshakeCfg.TelemetryOnly,
                poEndSourceByMill,
                driver = "S7-Handshake",
                lastReadOk = snapshot.Count > 0 && snapshot.All(m => m.Connected),
                lastPlcError = _handshakeStatus.FirstError() ?? _plcHealth.LastError,
                lastPlcCheckUtc = snapshot.Count > 0
                    ? snapshot.Max(m => m.LastUpdateUtc)
                    : _plcHealth.LastUpdateUtc,
                readLineRunning = handshakeCfg.ReadLineRunning,
                recoverLatchedTriggerAtStartup = handshakeCfg.RecoverLatchedTriggerAtStartup,
                runPoEndWorkflowOnStartupRecovery = handshakeCfg.RunPoEndWorkflowOnStartupRecovery,
                lineRunningSignal = new
                {
                    address = FormatLineRunningAddress(handshakeCfg),
                    dbNumber = handshakeCfg.LineRunningDbNumber,
                    byteOffset = handshakeCfg.LineRunningByteOffset,
                    bit = handshakeCfg.LineRunningBit
                },
                poEndByMill = Enumerable.Range(1, 4).ToDictionary(
                    m => m.ToString(),
                    m => poEndByMill.TryGetValue(m, out var v) && v),
                mills
            });
        }

        return await GetLegacyPlcDiagnosticsAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Runs a PO-change test for one mill: read trigger from PLC, execute PO end workflow, pulse ack bit.
    /// Uses the mill's persistent handshake S7 connection (no second client).
    /// On Shared, forwards to the mill instance HTTP API.
    /// </summary>
    [HttpPost("plc/test-po-change")]
    public async Task<IActionResult> TestPoChange(
        [FromBody] TestPoChangeRequest request,
        CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        if (request?.MillNo is < 1 or > 4)
            return BadRequest(new { Message = "millNo must be 1–4." });

        if (_millPlcProxy.ShouldProxy(request.MillNo))
        {
            _logger.LogInformation(
                "Settings PO change test for Mill {Mill} proxied to mill instance.",
                request.MillNo);
            return await ProxyToMillAsync(
                    request.MillNo,
                    HttpMethod.Post,
                    "api/Settings/plc/test-po-change",
                    request,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        var handshakeCfg = _options.PlcHandshake ?? new PlcHandshakeOptions();
        if (!handshakeCfg.Enabled)
        {
            return BadRequest(new
            {
                Message = "PlcHandshake is disabled. Enable NdtBundle:PlcHandshake:Enabled or use POST /api/Test/po-end for workflow-only simulation."
            });
        }

        if (handshakeCfg.TelemetryOnly)
        {
            return BadRequest(new
            {
                Message =
                    "PO-change handshake is disabled (PlcHandshake.TelemetryOnly). " +
                    "Use POST /api/Test/po-end for workflow-only simulation."
            });
        }

        _logger.LogInformation("Settings PO change test requested for Mill {Mill}.", request.MillNo);

        var result = await _handshakeCoordinator
            .RunSettingsTestAsync(request.MillNo, cancellationToken)
            .ConfigureAwait(false);

        return Ok(new
        {
            result.Success,
            result.Message,
            result.MillNo,
            result.MillName,
            result.PlcConnected,
            result.TriggerBefore,
            result.TriggerAfter,
            result.AckPulsed,
            result.WorkflowInvoked,
            result.PoNumber,
            steps = result.Steps,
            logHint = "Filter logs for [Settings test] or PO end workflow for this mill."
        });
    }

    /// <summary>
    /// Disconnect one mill's S7 handshake (releases the PLC connection slot). Slit CSV processing and other mills are unaffected.
    /// On Shared, forwards to the mill instance HTTP API.
    /// </summary>
    [HttpPost("plc/mill/{millNo:int}/disconnect")]
    public async Task<IActionResult> DisconnectMillPlc(int millNo, CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        return await SetMillPlcConnectionAsync(millNo, enabled: false, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// MES-computed hooter values (formation threshold + open accumulation / sizeCounts) for one mill.
    /// Mill instances serve the authoritative sizeCounts; Shared Settings proxies here so the UI
    /// does not show stale Shared-local accumulation.
    /// </summary>
    [HttpGet("plc/mill/{millNo:int}/mes-hooter")]
    public async Task<IActionResult> GetMesHooter(int millNo, CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        if (millNo is < 1 or > 4)
            return BadRequest(new { Message = "millNo must be 1–4." });

        if (_millPlcProxy.ShouldProxy(millNo))
        {
            return await ProxyToMillAsync(
                    millNo,
                    HttpMethod.Get,
                    $"api/Settings/plc/mill/{millNo}/mes-hooter",
                    body: null,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        try
        {
            var resolved = await _hooterValues.ResolveAsync(millNo, cancellationToken).ConfigureAwait(false);
            return Ok(new
            {
                poNumber = resolved.PoNumber,
                pipeSize = resolved.PipeSize,
                threshold = resolved.Threshold,
                accumulated = resolved.Accumulated,
                bundleNear = resolved.Threshold > 0 && resolved.Accumulated > resolved.Threshold
            });
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "MES hooter resolve failed for mill {Mill}.", millNo);
            return StatusCode(500, new { Message = "Failed to resolve MES hooter values.", Error = ex.Message });
        }
    }

    /// <summary>
    /// Test-pulse the NDT bundle hooter on the mill's live S7 connection using the same PAS / Q /
    /// WriteOutputBit / DurationMs path as production. When <c>requireThreshold</c> is true (default),
    /// also requires MW56 &gt; MW58. On Shared, forwards to the mill instance.
    /// </summary>
    [HttpPost("plc/mill/{millNo:int}/test-hooter")]
    public async Task<IActionResult> TestHooter(
        int millNo,
        [FromBody] TestHooterRequest? request,
        CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        if (millNo is < 1 or > 4)
            return BadRequest(new { Message = "millNo must be 1–4." });

        var requireThreshold = request?.RequireThreshold ?? true;

        if (_millPlcProxy.ShouldProxy(millNo))
        {
            _logger.LogInformation(
                "Settings hooter test for Mill {Mill} proxied to mill instance (requireThreshold={RequireThreshold}).",
                millNo,
                requireThreshold);
            return await ProxyToMillAsync(
                    millNo,
                    HttpMethod.Post,
                    $"api/Settings/plc/mill/{millNo}/test-hooter",
                    new TestHooterRequest { RequireThreshold = requireThreshold },
                    cancellationToken)
                .ConfigureAwait(false);
        }

        var handshakeCfg = _options.PlcHandshake ?? new PlcHandshakeOptions();
        if (!handshakeCfg.Enabled)
        {
            return BadRequest(new
            {
                Message = "PlcHandshake is disabled on this instance; hooter pulse requires the mill handshake loop."
            });
        }

        _logger.LogInformation(
            "Settings hooter test requested for Mill {Mill} (requireThreshold={RequireThreshold}).",
            millNo,
            requireThreshold);

        var result = await _handshakeCoordinator
            .RunSettingsHooterTestAsync(millNo, requireThreshold, cancellationToken)
            .ConfigureAwait(false);

        if (!result.Success)
        {
            return BadRequest(new
            {
                result.Success,
                result.Message,
                result.MillNo,
                result.MillName,
                result.PlcConnected,
                result.HooterEnabled,
                result.PoNumber,
                result.PipeSize,
                result.Accumulated,
                result.Threshold,
                result.AboveThreshold,
                result.RequireThreshold,
                result.PasEnable,
                result.QBefore,
                result.Pulsed,
                result.DurationMs,
                result.OutputAddress,
                result.PasEnableAddress,
                steps = result.Steps
            });
        }

        return Ok(new
        {
            result.Success,
            result.Message,
            result.MillNo,
            result.MillName,
            result.PlcConnected,
            result.HooterEnabled,
            result.PoNumber,
            result.PipeSize,
            result.Accumulated,
            result.Threshold,
            result.AboveThreshold,
            result.RequireThreshold,
            result.PasEnable,
            result.QBefore,
            result.Pulsed,
            result.DurationMs,
            result.OutputAddress,
            result.PasEnableAddress,
            steps = result.Steps,
            logHint = "Filter logs for [Settings test] and NDT bundle hooter."
        });
    }

    /// <summary>
    /// Operator override for open-bundle PLC accumulation (MES <c>sizeCounts</c> / MW56 toward next close).
    /// Use after restart when slit-edge state was lost and MES count no longer matches actual production.
    /// On Shared, forwards to the mill instance HTTP API so MW56 sync hits the live S7 connection.
    /// </summary>
    [HttpPost("plc/mill/{millNo:int}/open-accumulation")]
    public async Task<IActionResult> SetOpenAccumulation(
        int millNo,
        [FromBody] OpenAccumulationOverrideRequest request,
        CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        if (request is null)
            return BadRequest(new { Message = "Request body is required." });

        if (millNo is < 1 or > 4)
            return BadRequest(new { Message = "millNo must be 1–4." });

        if (_millPlcProxy.ShouldProxy(millNo))
        {
            _logger.LogInformation(
                "Settings open-accumulation for Mill {Mill} proxied to mill instance.",
                millNo);
            return await ProxyToMillAsync(
                    millNo,
                    HttpMethod.Post,
                    $"api/Settings/plc/mill/{millNo}/open-accumulation",
                    request,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        var result = await _openAccumulationOverride
            .SetOpenAccumulationAsync(
                millNo,
                request.Accumulated,
                request.PoNumber,
                request.SizeKey,
                cancellationToken)
            .ConfigureAwait(false);

        if (!result.Success)
            return BadRequest(new { Message = result.Message, result.MillNo });

        return Ok(new
        {
            Message = result.Message,
            result.MillNo,
            PoNumber = result.PoNumber,
            SizeKey = result.SizeKey,
            PreviousAccumulated = result.PreviousAccumulated,
            NewAccumulated = result.NewAccumulated,
            Threshold = result.Threshold,
            HooterSyncedToPlc = result.HooterSyncedToPlc,
            MesHooter = result.ResolvedHooter is null
                ? null
                : new
                {
                    result.ResolvedHooter.PoNumber,
                    result.ResolvedHooter.PipeSize,
                    Threshold = result.ResolvedHooter.Threshold,
                    Accumulated = result.ResolvedHooter.Accumulated
                }
        });
    }

    /// <summary>Reconnect one mill's S7 handshake after a manual disconnect. On Shared, forwards to the mill instance.</summary>
    [HttpPost("plc/mill/{millNo:int}/connect")]
    public async Task<IActionResult> ConnectMillPlc(int millNo, CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        return await SetMillPlcConnectionAsync(millNo, enabled: true, cancellationToken).ConfigureAwait(false);
    }

    private async Task<IActionResult> SetMillPlcConnectionAsync(
        int millNo,
        bool enabled,
        CancellationToken cancellationToken)
    {
        if (millNo is < 1 or > 4)
            return BadRequest(new { Message = "millNo must be 1–4." });

        if (_millPlcProxy.ShouldProxy(millNo))
        {
            var action = enabled ? "connect" : "disconnect";
            _logger.LogInformation(
                "Settings PLC {Action} for Mill {Mill} proxied to mill instance.",
                action,
                millNo);
            return await ProxyToMillAsync(
                    millNo,
                    HttpMethod.Post,
                    $"api/Settings/plc/mill/{millNo}/{action}",
                    body: null,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        var handshakeCfg = _options.PlcHandshake ?? new PlcHandshakeOptions();
        if (!handshakeCfg.Enabled)
        {
            return BadRequest(new
            {
                Message = "PlcHandshake is disabled. Enable NdtBundle:PlcHandshake:Enabled and restart NdtBundleService."
            });
        }

        var millCfg = handshakeCfg.Mills?.FirstOrDefault(m => m.ResolveMillNo() == millNo);
        if (millCfg is null || string.IsNullOrWhiteSpace(millCfg.IpAddress))
        {
            return BadRequest(new
            {
                Message = $"Mill {millNo} is not configured in NdtBundle:PlcHandshake:Mills (missing IpAddress)."
            });
        }

        var localAction = enabled ? "connect" : "disconnect";
        _logger.LogInformation("Settings PLC {Action} requested for Mill {Mill}.", localAction, millNo);

        var result = _handshakeCoordinator.SetMillPlcConnectionEnabled(millNo, enabled);
        if (!result.Success)
            return BadRequest(new { result.Success, result.Message, result.MillNo });

        return Ok(new
        {
            result.Success,
            result.MillNo,
            result.MillName,
            result.PlcConnectionEnabled,
            result.Connected,
            result.Message
        });
    }

    private async Task<IActionResult> ProxyToMillAsync(
        int millNo,
        HttpMethod method,
        string relativePath,
        object? body,
        CancellationToken cancellationToken)
    {
        var json = body is null ? null : JsonSerializer.Serialize(body, ProxyJsonOptions);
        var proxied = await _millPlcProxy
            .ForwardAsync(millNo, method, relativePath, json, cancellationToken)
            .ConfigureAwait(false);
        return new ContentResult
        {
            StatusCode = proxied.StatusCode,
            Content = proxied.Body,
            ContentType = proxied.ContentType
        };
    }

    /// <summary>
    /// Authoritative MES hooter for Shared Settings: mill proxy first, then PLC MW56/MW58 telemetry
    /// fallback (never Shared-local sizeCounts, which go stale after mill-side overrides).
    /// </summary>
    private async Task<object?> TryGetSharedMesHooterAsync(
        int millNo,
        PlcHandshakeMillStatus? live,
        CancellationToken cancellationToken)
    {
        if (_millPlcProxy.ShouldProxy(millNo))
        {
            try
            {
                var proxied = await _millPlcProxy
                    .ForwardAsync(
                        millNo,
                        HttpMethod.Get,
                        $"api/Settings/plc/mill/{millNo}/mes-hooter",
                        jsonBody: null,
                        cancellationToken)
                    .ConfigureAwait(false);
                if (proxied.StatusCode is >= 200 and < 300 &&
                    !string.IsNullOrWhiteSpace(proxied.Body))
                {
                    using var doc = JsonDocument.Parse(proxied.Body);
                    var root = doc.RootElement;
                    int? accumulated = root.TryGetProperty("accumulated", out var accEl) && accEl.TryGetInt32(out var acc)
                        ? acc
                        : null;
                    int? threshold = root.TryGetProperty("threshold", out var thrEl) && thrEl.TryGetInt32(out var thr)
                        ? thr
                        : null;
                    if (accumulated is not null && threshold is not null)
                    {
                        string? poNumber = root.TryGetProperty("poNumber", out var poEl) && poEl.ValueKind == JsonValueKind.String
                            ? poEl.GetString()
                            : null;
                        string? pipeSize = root.TryGetProperty("pipeSize", out var psEl) && psEl.ValueKind == JsonValueKind.String
                            ? psEl.GetString()
                            : null;
                        return new
                        {
                            poNumber,
                            pipeSize,
                            threshold = threshold.Value,
                            accumulated = accumulated.Value,
                            bundleNear = threshold.Value > 0 && accumulated.Value > threshold.Value
                        };
                    }
                }
                else
                {
                    _logger.LogWarning(
                        "Shared MES hooter proxy for Mill {Mill} returned HTTP {Status}.",
                        millNo,
                        proxied.StatusCode);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Shared MES hooter proxy failed for Mill {Mill}.", millNo);
            }
        }

        // Fallback: mill-written MW56/MW58 from Mill_Instance_Status (still better than Shared sizeCounts).
        if (live?.AccumulatedValue is int plcAcc && live.ThresholdValue is int plcThr)
        {
            return new
            {
                poNumber = live.RunningPoNumber,
                pipeSize = (string?)null,
                threshold = plcThr,
                accumulated = plcAcc,
                bundleNear = plcThr > 0 && plcAcc > plcThr
            };
        }

        return null;
    }

    /// <summary>
    /// Shared Settings PLC tab: live OK/NOK/NDT and connection state from mill telemetry;
    /// mill host / hooter addresses from <c>PlcHandshake:Mills</c> (S7 stays on each mill process).
    /// </summary>
    private async Task<IActionResult> GetSharedPlcDiagnosticsAsync(CancellationToken cancellationToken, bool liveOnly)
    {
        var handshakeCfg = _options.PlcHandshake ?? new PlcHandshakeOptions();
        var millsCfg = (handshakeCfg.Mills ?? new List<MillConfig>())
            .Where(m => m.ResolveMillNo() is >= 1 and <= 4)
            .GroupBy(m => m.ResolveMillNo())
            .Select(g => g.First())
            .OrderBy(m => m.ResolveMillNo())
            .ToList();

        if (millsCfg.Count == 0)
            millsCfg = SharedDisplayMillDefaults();

        var liveSnap = _plcLive.GetSnapshot();
        var liveByMill = liveSnap.Mills.ToDictionary(m => m.MillNo, m => m);
        var mills = new List<object>();

        foreach (var cfg in millsCfg)
        {
            var millNo = cfg.ResolveMillNo();
            liveByMill.TryGetValue(millNo, out var live);
            var host = (cfg.IpAddress ?? string.Empty).Trim();
            if (string.IsNullOrEmpty(host) && !string.IsNullOrWhiteSpace(live?.IpAddress))
                host = live!.IpAddress.Trim();

            var handshakeConnected = live?.Connected ?? false;
            bool reachable;
            if (liveOnly)
                reachable = handshakeConnected;
            else if (handshakeConnected)
                reachable = true;
            else
            {
                reachable = !string.IsNullOrEmpty(host) &&
                            await TcpProbeCachedAsync(host, 102, cancellationToken).ConfigureAwait(false);
            }

            // Never resolve sizeCounts on Shared — that state lives on the mill process.
            // Proxy MES hooter from the mill (same path as open-accumulation writes).
            object? mesHooter = null;
            if (!liveOnly && cfg.Hooter?.Enabled == true && millNo is >= 1 and <= 4)
            {
                mesHooter = await TryGetSharedMesHooterAsync(millNo, live, cancellationToken)
                    .ConfigureAwait(false);
            }

            var poEndSource = cfg.ResolvePoEndSource(_options);
            var plcLinkEnabled = live?.PlcConnectionEnabled ?? cfg.PlcHandshakeEnabled;
            mills.Add(new
            {
                millNo,
                name = string.IsNullOrWhiteSpace(cfg.Name) ? $"Mill-{millNo}" : cfg.Name,
                poEndSource = MillPoEndSourceResolver.ToConfigValue(poEndSource),
                poEndSourceDescription = MillPoEndSourceResolver.Describe(poEndSource),
                tcpOpenCommHost = (string?)null,
                tcpOpenPort = (int?)null,
                tcpOpenConnectTimeoutMs = (int?)null,
                tcpOpenReceiveTimeoutMs = (int?)null,
                driver = "S7-Handshake",
                host,
                port = 102,
                reachable,
                poEndAddress = cfg.TriggerAddress,
                mesAckAddress = cfg.AckAddress,
                handshakeConnected,
                plcConnectionEnabled = plcLinkEnabled,
                triggerActive = live?.TriggerActive ?? false,
                ackActive = live?.AckActive ?? false,
                handshakeState = live?.HandshakeState
                    ?? (plcLinkEnabled ? "Unknown" : "Disconnected (mill not publishing)"),
                lastPoChangeUtc = live?.LastPoChangeUtc,
                lastError = live?.LastError,
                // Connect / test / open-accumulation are proxied to the mill process.
                testAvailable = _millPlcProxy.ShouldProxy(millNo),
                lineRunning = live?.LineRunning,
                lineRunningAddress = FormatLineRunningAddress(handshakeCfg),
                accumulatedValue = live?.AccumulatedValue,
                thresholdValue = live?.ThresholdValue,
                hooterActive = live?.HooterActive ?? false,
                okCount = live?.OkCount,
                nokCount = live?.NokCount,
                ndtCount = live?.NdtCount,
                poId = live?.PoId,
                countsUpdatedUtc = live?.CountsUpdatedUtc,
                hooterEnabled = cfg.Hooter?.Enabled ?? false,
                hooterAccumAddress = cfg.Hooter?.Enabled == true
                    ? $"MW{cfg.Hooter.AccumulatedWordOffset}"
                    : null,
                hooterThresholdAddress = cfg.Hooter?.Enabled == true
                    ? $"MW{cfg.Hooter.ThresholdWordOffset}"
                    : null,
                hooterOutputAddress = cfg.Hooter?.Enabled == true
                    ? $"Q{cfg.Hooter.OutputByte}.{cfg.Hooter.OutputBit}"
                    : null,
                hooterPasEnableAddress = cfg.Hooter?.Enabled == true
                    ? $"DB{cfg.Hooter.PasEnableDbNumber}.DBX{cfg.Hooter.PasEnableByteOffset}.{cfg.Hooter.PasEnableBit}"
                    : null,
                hooterDurationMs = cfg.Hooter?.DurationMs,
                mesHooter
            });
        }

        var poEndSourceByMill = Enumerable.Range(1, 4).ToDictionary(
            m => m.ToString(),
            m =>
            {
                var src = MillPoEndSourceResolver.ForMill(m, _options);
                return new
                {
                    source = MillPoEndSourceResolver.ToConfigValue(src),
                    description = MillPoEndSourceResolver.Describe(src)
                };
            });

        var anyLive = liveSnap.Mills.Any(m => m.Connected);
        return Ok(new
        {
            plcPoEndEnabled = true,
            plcHandshakeEnabled = true,
            plcHandshakeTelemetryOnly = handshakeCfg.TelemetryOnly,
            poEndSourceByMill,
            driver = "S7-Handshake-Sql",
            lastReadOk = anyLive,
            lastPlcError = liveSnap.Mills.Select(m => m.LastError).FirstOrDefault(e => !string.IsNullOrWhiteSpace(e))
                ?? (anyLive ? null : "Waiting for mill instances to publish Mill_Instance_Status."),
            lastPlcCheckUtc = liveSnap.Mills.Count > 0
                ? liveSnap.Mills.Max(m => m.LastUpdateUtc)
                : _plcHealth.LastUpdateUtc,
            readLineRunning = handshakeCfg.ReadLineRunning,
            recoverLatchedTriggerAtStartup = handshakeCfg.RecoverLatchedTriggerAtStartup,
            runPoEndWorkflowOnStartupRecovery = handshakeCfg.RunPoEndWorkflowOnStartupRecovery,
            lineRunningSignal = new
            {
                address = FormatLineRunningAddress(handshakeCfg),
                dbNumber = handshakeCfg.LineRunningDbNumber,
                byteOffset = handshakeCfg.LineRunningByteOffset,
                bit = handshakeCfg.LineRunningBit
            },
            poEndByMill = Enumerable.Range(1, 4).ToDictionary(
                m => m.ToString(),
                m => liveByMill.TryGetValue(m, out var row) && row.TriggerActive),
            mills,
            message =
                "Shared Settings PLC view uses mill telemetry for PLC bits, MillInstanceProxy for MES hooter / writes. S7 stays on each mill process."
        });
    }

    /// <summary>Fallback mill rows when Shared <c>PlcHandshake:Mills</c> is empty (production IPs).</summary>
    private static List<MillConfig> SharedDisplayMillDefaults() =>
    [
        new MillConfig
        {
            Name = "Mill-1",
            MillNo = 1,
            PoEndSource = "Plc",
            PlcHandshakeEnabled = true,
            IpAddress = "192.168.0.13",
            Rack = 0,
            Slot = 2,
            CpuType = "S7300",
            TriggerByte = 40,
            TriggerBit = 6,
            AckByte = 40,
            AckBit = 7,
            Hooter = new MillHooterOptions
            {
                Enabled = true,
                PasEnableDbNumber = 260,
                PasEnableByteOffset = 3,
                PasEnableBit = 6,
                AccumulatedWordOffset = 56,
                ThresholdWordOffset = 58,
                OutputByte = 6,
                OutputBit = 7,
                DurationMs = 10000
            }
        },
        new MillConfig
        {
            Name = "Mill-2",
            MillNo = 2,
            PoEndSource = "Plc",
            PlcHandshakeEnabled = true,
            IpAddress = "192.168.0.60",
            Rack = 0,
            Slot = 2,
            CpuType = "S7300",
            TriggerByte = 40,
            TriggerBit = 6,
            AckByte = 40,
            AckBit = 7
        },
        new MillConfig
        {
            Name = "Mill-3",
            MillNo = 3,
            PoEndSource = "Plc",
            PlcHandshakeEnabled = true,
            IpAddress = "192.168.0.17",
            Rack = 0,
            Slot = 2,
            CpuType = "S7300",
            TriggerByte = 40,
            TriggerBit = 6,
            AckByte = 40,
            AckBit = 7
        },
        new MillConfig
        {
            Name = "Mill-4",
            MillNo = 4,
            PoEndSource = "Plc",
            PlcHandshakeEnabled = true,
            IpAddress = "192.168.0.19",
            Rack = 0,
            Slot = 2,
            CpuType = "S7300",
            TriggerByte = 40,
            TriggerBit = 6,
            AckByte = 40,
            AckBit = 7
        }
    ];

    private async Task<IActionResult> GetLegacyPlcDiagnosticsAsync(CancellationToken cancellationToken)
    {
        var plcCfg = _options.PlcPoEnd ?? new PlcPoEndOptions();
        var mills = new List<object>();

        if (plcCfg.Enabled && PlcPoEndOptions.IsS7Driver(plcCfg))
        {
            foreach (var ep in plcCfg.Mills.OrderBy(m => m.MillNo))
            {
                var host = (ep.Host ?? string.Empty).Trim();
                var port = ep.Port > 0 ? ep.Port : 102;
                var reachable = !string.IsNullOrEmpty(host) &&
                                await TcpProbeCachedAsync(host, port, cancellationToken).ConfigureAwait(false);
                mills.Add(new
                {
                    millNo = ep.MillNo,
                    driver = "S7",
                    host,
                    port,
                    reachable,
                    poEndAddress = ep.S7PoEndAddress,
                    mesAckAddress = ep.S7MesAckAddress,
                    testAvailable = false
                });
            }
        }
        else if (plcCfg.Enabled && string.Equals(plcCfg.Driver, "ModbusTcp", StringComparison.OrdinalIgnoreCase))
        {
            foreach (var ep in plcCfg.Mills.OrderBy(m => m.MillNo))
            {
                var host = (ep.Host ?? string.Empty).Trim();
                var port = ep.Port > 0 ? ep.Port : 502;
                var reachable = !string.IsNullOrEmpty(host) &&
                                await TcpProbeCachedAsync(host, port, cancellationToken).ConfigureAwait(false);
                mills.Add(new
                {
                    millNo = ep.MillNo,
                    driver = "ModbusTcp",
                    host,
                    port,
                    reachable,
                    testAvailable = false
                });
            }
        }

        IReadOnlyDictionary<int, bool> poEndByMill;
        try
        {
            poEndByMill = await _plcClient.GetPoEndSignalsByMillAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            poEndByMill = new Dictionary<int, bool>();
            _logger.LogWarning(ex, "PLC PO-end read failed during settings diagnostics.");
        }

        var diag = _poEndDiagnostics.GetSnapshot();

        return Ok(new
        {
            plcPoEndEnabled = plcCfg.Enabled,
            plcHandshakeEnabled = false,
            driver = plcCfg.Driver ?? "Stub",
            lastReadOk = _plcHealth.LastReadOk,
            lastPlcError = _plcHealth.LastError,
            lastPlcCheckUtc = _plcHealth.LastUpdateUtc,
            poEndByMill = Enumerable.Range(1, 4).ToDictionary(
                m => m.ToString(),
                m => poEndByMill.TryGetValue(m, out var v) && v),
            mills,
            detection = diag.Mills.Select((m, i) => new
            {
                millNo = i + 1,
                m.CurrentPoIdFromPlc,
                m.TrackedPrevPoId,
                m.LastError
            })
        });
    }

    [HttpGet("printers")]
    public async Task<IActionResult> GetPrinters(CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        var configured = await _millPrinters.GetAllAsync(cancellationToken).ConfigureAwait(false);
        var results = new List<object>();
        foreach (var p in configured)
        {
            var resolved = _millPrinters.ResolveForMill(p.MillNo);
            var reachable = resolved.Configured &&
                            await TcpProbeAsync(resolved.Address, resolved.Port, cancellationToken).ConfigureAwait(false);
            results.Add(new
            {
                millNo = p.MillNo,
                // Mill-1 row is the WIP printer (auto-print + Settings Mill-1 WIP).
                role = p.MillNo == 1 ? ReprintPrinterTarget.Wip : null,
                displayName = p.MillNo == 1 ? "Mill-1 WIP printer" : $"Mill-{p.MillNo}",
                address = p.Address,
                port = p.Port,
                effectiveAddress = resolved.Address,
                effectivePort = resolved.Port,
                configured = resolved.Configured,
                reachable,
                status = !resolved.Configured ? "NotConfigured" : reachable ? "Ready" : "Unreachable"
            });
        }

        async Task<object> ProbeNamedAsync(NamedPrinterEndpoint endpoint, string displayName, string role)
        {
            var addr = (endpoint.Address ?? string.Empty).Trim();
            var port = endpoint.Port > 0 ? endpoint.Port : 9100;
            var configuredOk = !string.IsNullOrWhiteSpace(addr) &&
                               !addr.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase);
            var reachable = configuredOk &&
                            await TcpProbeAsync(addr, port, cancellationToken).ConfigureAwait(false);
            return new
            {
                key = endpoint.Key,
                role,
                displayName,
                address = addr,
                port,
                effectiveAddress = addr,
                effectivePort = port,
                configured = configuredOk,
                reachable,
                status = !configuredOk ? "NotConfigured" : reachable ? "Ready" : "Unreachable"
            };
        }

        var mill1Wip = await _millPrinters.GetMill1WipAsync(cancellationToken).ConfigureAwait(false);
        var mill1Fg = await _millPrinters.GetMill1FgAsync(cancellationToken).ConfigureAwait(false);
        var pasRoom = await _millPrinters.GetPasRoomAsync(cancellationToken).ConfigureAwait(false);
        var mill1FgStatus = await ProbeNamedAsync(mill1Fg, "Mill-1 FG printer", ReprintPrinterTarget.Fg)
            .ConfigureAwait(false);
        var pasRoomStatus = await ProbeNamedAsync(pasRoom, "PAS Room printer", ReprintPrinterTarget.Pas)
            .ConfigureAwait(false);
        // mill1Wip is already reflected in mills[0]; keep a dedicated object for UI convenience.
        var mill1WipStatus = await ProbeNamedAsync(mill1Wip, "Mill-1 WIP printer", ReprintPrinterTarget.Wip)
            .ConfigureAwait(false);

        var stationResults = new List<object>();
        if (_stationPrinters is not null)
        {
            var stations = await _stationPrinters.GetAllAsync(cancellationToken).ConfigureAwait(false);
            foreach (var s in stations)
            {
                var resolved = _stationPrinters.Resolve(s.StationCode);
                var reachable = resolved.Configured &&
                                await TcpProbeAsync(resolved.Address, resolved.Port, cancellationToken).ConfigureAwait(false);
                stationResults.Add(new
                {
                    stationCode = s.StationCode,
                    displayName = StationPrinterTarget.DisplayName(s.StationCode),
                    address = s.Address,
                    port = s.Port,
                    effectiveAddress = resolved.Address,
                    effectivePort = resolved.Port,
                    configured = resolved.Configured,
                    reachable,
                    status = !resolved.Configured ? "NotConfigured" : reachable ? "Ready" : "Unreachable"
                });
            }
        }

        return Ok(new
        {
            Mills = results,
            Mill1Wip = mill1WipStatus,
            Mill1Fg = mill1FgStatus,
            PasRoom = pasRoomStatus,
            Stations = stationResults
        });
    }

    [HttpPut("printers")]
    public async Task<IActionResult> PutPrinters(
        [FromBody] SaveMillPrintersRequest request,
        CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        if (request?.Mills is null)
            return BadRequest(new { Message = "Mills array is required." });

        if (request.Stations is { Count: > 0 } && _stationPrinters is null)
        {
            return BadRequest(new { Message = "Station printers can only be saved on Shared." });
        }

        var endpoints = request.Mills
            .Where(m => m.MillNo is >= 1 and <= 4)
            .Select(m => new MillPrinterEndpoint(
                m.MillNo,
                (m.Address ?? string.Empty).Trim(),
                m.Port > 0 ? m.Port : 9100))
            .ToList();

        // Prefer explicit Mill1Wip when provided; otherwise Mill-1 row is treated as WIP.
        if (request.Mill1Wip is not null)
        {
            var existing = endpoints.FirstOrDefault(m => m.MillNo == 1);
            var wip = new MillPrinterEndpoint(
                1,
                (request.Mill1Wip.Address ?? string.Empty).Trim(),
                request.Mill1Wip.Port > 0 ? request.Mill1Wip.Port : 9100);
            if (existing is null)
                endpoints.Add(wip);
            else
            {
                endpoints.Remove(existing);
                endpoints.Add(wip);
            }
        }

        await _millPrinters.SaveAllAsync(endpoints, cancellationToken).ConfigureAwait(false);

        NamedPrinterEndpoint? mill1Fg = null;
        NamedPrinterEndpoint? pasRoom = null;
        if (request.Mill1Fg is not null)
        {
            mill1Fg = new NamedPrinterEndpoint(
                "MILL_1_FG",
                (request.Mill1Fg.Address ?? string.Empty).Trim(),
                request.Mill1Fg.Port > 0 ? request.Mill1Fg.Port : 9100);
        }

        if (request.PasRoom is not null)
        {
            pasRoom = new NamedPrinterEndpoint(
                "PAS_ROOM",
                (request.PasRoom.Address ?? string.Empty).Trim(),
                request.PasRoom.Port > 0 ? request.PasRoom.Port : 9100);
        }

        if (mill1Fg is not null || pasRoom is not null)
        {
            await _millPrinters
                .SaveReprintPrintersAsync(mill1Wip: null, mill1Fg, pasRoom, cancellationToken)
                .ConfigureAwait(false);
        }

        var stationCount = 0;
        if (_stationPrinters is not null && request.Stations is { Count: > 0 })
        {
            var stationEndpoints = request.Stations
                .Select(s => new StationPrinterEndpoint(
                    (s.StationCode ?? string.Empty).Trim(),
                    (s.Address ?? string.Empty).Trim(),
                    s.Port > 0 ? s.Port : 9100))
                .ToList();
            await _stationPrinters.SaveAllAsync(stationEndpoints, cancellationToken).ConfigureAwait(false);
            stationCount = stationEndpoints.Count;
        }

        return Ok(new
        {
            Message = "Printer settings saved.",
            Count = endpoints.Count,
            StationCount = stationCount,
            Mill1FgSaved = mill1Fg is not null,
            PasRoomSaved = pasRoom is not null
        });
    }

    [HttpPost("printers/test")]
    public async Task<IActionResult> TestPrinter(
        [FromBody] TestMillPrinterRequest request,
        CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        if (TryGetStationCode(request, out var stationCode, out var stationError))
        {
            if (stationError is not null)
                return stationError;
            return await TestStationPrinterAsync(stationCode!, cancellationToken).ConfigureAwait(false);
        }

        if (!string.IsNullOrWhiteSpace(request?.PrinterTarget))
        {
            var target = ReprintPrinterTarget.Normalize(request.PrinterTarget);
            if (!ReprintPrinterTarget.IsKnown(request.PrinterTarget))
                return BadRequest(new { Message = $"Unknown printerTarget '{request.PrinterTarget}'." });

            var millNo = target is ReprintPrinterTarget.Wip or ReprintPrinterTarget.Fg
                ? 1
                : request.MillNo is >= 1 and <= 4 ? request.MillNo : 1;
            var resolvedTarget = _millPrinters.ResolveReprintTarget(millNo, target);
            if (!resolvedTarget.Configured)
            {
                return Ok(new
                {
                    millNo,
                    printerTarget = target,
                    displayName = resolvedTarget.DisplayName,
                    status = "NotConfigured",
                    reachable = false
                });
            }

            var targetReachable = await TcpProbeAsync(
                    resolvedTarget.Address,
                    resolvedTarget.Port,
                    cancellationToken)
                .ConfigureAwait(false);
            return Ok(new
            {
                millNo,
                printerTarget = target,
                displayName = resolvedTarget.DisplayName,
                address = resolvedTarget.Address,
                port = resolvedTarget.Port,
                status = targetReachable ? "Ready" : "Unreachable",
                reachable = targetReachable
            });
        }

        if (request?.MillNo is < 1 or > 4)
            return BadRequest(new { Message = "millNo must be 1–4, or provide stationCode / printerTarget." });

        var resolved = _millPrinters.ResolveForMill(request.MillNo);
        if (!resolved.Configured)
            return Ok(new { millNo = request.MillNo, status = "NotConfigured", reachable = false });

        var reachable = await TcpProbeAsync(resolved.Address, resolved.Port, cancellationToken).ConfigureAwait(false);
        return Ok(new
        {
            millNo = request.MillNo,
            address = resolved.Address,
            port = resolved.Port,
            status = reachable ? "Ready" : "Unreachable",
            reachable
        });
    }

    /// <summary>
    /// Sends a dummy ZPL label to the configured mill printer (TCP 9100) so operators can verify print path,
    /// not just TCP reachability. Requires Settings → Save first for the mill IP in use.
    /// </summary>
    [HttpPost("printers/test-print")]
    public async Task<IActionResult> TestPrintPrinter(
        [FromBody] TestMillPrinterRequest request,
        CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        if (TryGetStationCode(request, out var stationCode, out var stationError))
        {
            if (stationError is not null)
                return stationError;
            return await TestPrintStationPrinterAsync(stationCode!, cancellationToken).ConfigureAwait(false);
        }

        if (!_zplToggle.IsEnabled)
        {
            return BadRequest(new
            {
                Message =
                    "NDT tag ZPL and network print are disabled (NdtBundle:EnableNdtTagZplAndPrint / runtime toggle). " +
                    "Enable printing, then try again."
            });
        }

        string address;
        int port;
        int millNo;
        string displayLabel;

        if (!string.IsNullOrWhiteSpace(request?.PrinterTarget))
        {
            var target = ReprintPrinterTarget.Normalize(request.PrinterTarget);
            if (!ReprintPrinterTarget.IsKnown(request.PrinterTarget))
                return BadRequest(new { Message = $"Unknown printerTarget '{request.PrinterTarget}'." });

            millNo = target is ReprintPrinterTarget.Wip or ReprintPrinterTarget.Fg
                ? 1
                : request.MillNo is >= 1 and <= 4 ? request.MillNo : 1;
            var resolvedTarget = _millPrinters.ResolveReprintTarget(millNo, target);
            if (!resolvedTarget.Configured)
            {
                return BadRequest(new
                {
                    Message =
                        $"{resolvedTarget.DisplayName} has no printer IP configured. Enter an address, click Save printers, then try Print test tag.",
                    millNo,
                    printerTarget = target,
                    status = "NotConfigured",
                    success = false
                });
            }

            address = resolvedTarget.Address;
            port = resolvedTarget.Port;
            displayLabel = resolvedTarget.DisplayName;
        }
        else
        {
            if (request?.MillNo is < 1 or > 4)
                return BadRequest(new { Message = "millNo must be 1–4, or provide stationCode / printerTarget." });

            millNo = request.MillNo;
            var resolved = _millPrinters.ResolveForMill(millNo);
            if (!resolved.Configured)
            {
                return BadRequest(new
                {
                    Message =
                        $"Mill {millNo} has no printer IP configured. Enter an address, click Save printers, then try Print test tag.",
                    millNo,
                    status = "NotConfigured",
                    success = false
                });
            }

            address = resolved.Address;
            port = resolved.Port;
            displayLabel = $"Mill {millNo} printer";
        }

        var bundleNo = $"TEST-M{millNo}";
        _logger.LogInformation(
            "Settings test-print: sending dummy ZPL to {Label} {Address}:{Port}.",
            displayLabel,
            address,
            port);

        try
        {
            var zplBytes = ZplDummyLabelBuilder.BuildDummyLabelZpl(
                bundleNo: bundleNo,
                specification: "SETTINGS-TEST",
                pipeType: "TEST",
                pipeSize: "1.5",
                pipeLen: "6",
                pcsPerBundle: 1,
                slitNo: $"M{millNo}",
                labelWidthMm: _options.NdtTagLabelWidthMm,
                labelLengthMm: ResolveDummyLabelLengthMm(millNo));

            var sendResult = await _networkPrinterSender
                .SendAsync(address, port, zplBytes, cancellationToken)
                .ConfigureAwait(false);

            if (sendResult.Success)
            {
                return Ok(new
                {
                    success = true,
                    millNo,
                    printerTarget = request?.PrinterTarget,
                    address,
                    port,
                    status = "Printed",
                    message =
                        $"Dummy ZPL tag sent to {displayLabel} {address}:{port}. " +
                        "The physical label should print now."
                });
            }

            return StatusCode(500, new
            {
                success = false,
                millNo,
                printerTarget = request?.PrinterTarget,
                address,
                port,
                status = "SendFailed",
                message =
                    $"Failed to send ZPL to {address}:{port}. " +
                    "Check printer power, network, firewall, and that the device accepts ZPL on port 9100.",
                detail = sendResult.ErrorDetail ?? string.Empty
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Settings test-print failed for {Label}.", displayLabel);
            return StatusCode(500, new
            {
                success = false,
                millNo,
                message = "Print failed: " + ex.Message
            });
        }
    }

    [HttpGet("mill-sequence")]
    public async Task<IActionResult> GetMillSequence(CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        if (!_millSequence.IsEnabled)
            return StatusCode(503, new { Message = "Mill_Sequence requires SQL mode." });

        var mills = await _millSequence.GetSnapshotsAsync(cancellationToken).ConfigureAwait(false);
        return Ok(new { mills });
    }

    [HttpPost("mill-sequence")]
    public async Task<IActionResult> SetMillSequence(
        [FromBody] MillSequenceSetRequest? request,
        CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        if (request is null)
            return BadRequest(new { Message = "Request body is required." });
        if (request.MillNo is < 1 or > 4)
            return BadRequest(new { Message = "MillNo must be 1–4." });
        if (string.IsNullOrWhiteSpace(request.Reason))
            return BadRequest(new { Message = "Reason is required." });

        try
        {
            var result = await _millSequence
                .SetCurrentSequenceAsync(
                    request.MillNo,
                    request.CurrentSequence,
                    request.Reason.Trim(),
                    string.IsNullOrWhiteSpace(request.UpdatedBy) ? "Settings" : request.UpdatedBy.Trim(),
                    request.ForceBelowLiveMax,
                    cancellationToken)
                .ConfigureAwait(false);
            return Ok(result);
        }
        catch (InvalidOperationException ex)
        {
            return BadRequest(new { Message = ex.Message });
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { Message = ex.Message });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Settings mill-sequence set failed for mill {Mill}.", request.MillNo);
            return StatusCode(500, new { Message = ex.Message });
        }
    }

    private int ResolveDummyLabelLengthMm(int millNo)
    {
        if (millNo >= 1
            && millNo <= 4
            && _options.NdtTagLabelLengthMmByMill.TryGetValue(millNo.ToString(), out var overrideLength)
            && overrideLength > 0)
        {
            return overrideLength;
        }

        return _options.NdtTagLabelLengthMm;
    }

    private bool TryAuthorize(out IActionResult? denied)
    {
        if (!_auth.IsConfigured)
        {
            denied = StatusCode(503, new { Message = "Settings admin password is not configured on the server." });
            return false;
        }

        if (!_auth.ValidateToken(GetToken()))
        {
            denied = Unauthorized(new { Message = "Settings login required." });
            return false;
        }

        denied = null;
        return true;
    }

    private string? GetToken() =>
        Request.Headers.TryGetValue("X-Settings-Token", out var values)
            ? values.FirstOrDefault()
            : null;

    private static string FormatLineRunningAddress(PlcHandshakeOptions options) =>
        $"DB{options.LineRunningDbNumber}.DBX{options.LineRunningByteOffset}.{options.LineRunningBit}";

    private bool TryGetStationCode(
        TestMillPrinterRequest? request,
        out string? stationCode,
        out IActionResult? error)
    {
        stationCode = null;
        error = null;
        if (string.IsNullOrWhiteSpace(request?.StationCode))
            return false;

        if (request.MillNo is >= 1 and <= 4)
        {
            error = BadRequest(new { Message = "Provide millNo or stationCode, not both." });
            return true;
        }

        if (_stationPrinters is null)
        {
            error = BadRequest(new { Message = "Station printers can only be tested on Shared." });
            return true;
        }

        if (!StationPrinterTarget.IsKnown(request.StationCode))
        {
            error = BadRequest(new { Message = $"Unknown stationCode '{request.StationCode}'." });
            return true;
        }

        stationCode = StationPrinterTarget.Normalize(request.StationCode);
        return true;
    }

    private async Task<IActionResult> TestStationPrinterAsync(string stationCode, CancellationToken cancellationToken)
    {
        var resolved = _stationPrinters!.Resolve(stationCode);
        if (!resolved.Configured)
        {
            return Ok(new
            {
                stationCode,
                displayName = StationPrinterTarget.DisplayName(stationCode),
                status = "NotConfigured",
                reachable = false,
                message = StationPrinterTarget.UnconfiguredMessage(stationCode)
            });
        }

        var reachable = await TcpProbeAsync(resolved.Address, resolved.Port, cancellationToken).ConfigureAwait(false);
        return Ok(new
        {
            stationCode,
            displayName = StationPrinterTarget.DisplayName(stationCode),
            address = resolved.Address,
            port = resolved.Port,
            status = reachable ? "Ready" : "Unreachable",
            reachable
        });
    }

    private async Task<IActionResult> TestPrintStationPrinterAsync(string stationCode, CancellationToken cancellationToken)
    {
        if (!_zplToggle.IsEnabled)
        {
            return BadRequest(new
            {
                Message =
                    "NDT tag ZPL and network print are disabled (NdtBundle:EnableNdtTagZplAndPrint / runtime toggle). " +
                    "Enable printing, then try again."
            });
        }

        var resolved = _stationPrinters!.Resolve(stationCode);
        var display = StationPrinterTarget.DisplayName(stationCode);
        if (!resolved.Configured)
        {
            return BadRequest(new
            {
                Message = StationPrinterTarget.UnconfiguredMessage(stationCode)
                    + ". Enter an address, click Save printers, then try Print test tag.",
                stationCode,
                displayName = display,
                status = "NotConfigured",
                success = false
            });
        }

        _logger.LogInformation(
            "Settings test-print: sending dummy ZPL to {Station} printer {Address}:{Port}.",
            display,
            resolved.Address,
            resolved.Port);

        try
        {
            var zplBytes = ZplDummyLabelBuilder.BuildDummyLabelZpl(
                bundleNo: $"TEST-{stationCode}",
                specification: "SETTINGS-TEST",
                pipeType: "TEST",
                pipeSize: "1.5",
                pipeLen: "6",
                pcsPerBundle: 1,
                slitNo: stationCode,
                labelWidthMm: _options.NdtTagLabelWidthMm,
                labelLengthMm: _options.NdtTagLabelLengthMm > 0 ? _options.NdtTagLabelLengthMm : 100);

            var sendResult = await _networkPrinterSender
                .SendAsync(resolved.Address, resolved.Port, zplBytes, cancellationToken)
                .ConfigureAwait(false);

            if (sendResult.Success)
            {
                return Ok(new
                {
                    success = true,
                    stationCode,
                    displayName = display,
                    address = resolved.Address,
                    port = resolved.Port,
                    status = "Printed",
                    message =
                        $"Dummy ZPL tag sent to {display} printer {resolved.Address}:{resolved.Port}. " +
                        "The physical label should print now."
                });
            }

            return StatusCode(500, new
            {
                success = false,
                stationCode,
                displayName = display,
                address = resolved.Address,
                port = resolved.Port,
                status = "SendFailed",
                message =
                    $"Failed to send ZPL to {display} printer {resolved.Address}:{resolved.Port}. " +
                    "Check printer power, network, firewall, and that the device accepts ZPL on port 9100.",
                detail = sendResult.ErrorDetail ?? string.Empty
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Settings test-print failed for station {Station}.", display);
            return StatusCode(500, new
            {
                success = false,
                stationCode,
                displayName = display,
                status = "SendFailed",
                message = ex.Message
            });
        }
    }

    private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, (bool Ok, DateTime ExpiresUtc)> TcpProbeCache = new();

    /// <summary>Only one full PLC diagnostics request at a time (TCP probes + MES hooter resolve).</summary>
    private static readonly SemaphoreSlim FullPlcDiagnosticsGate = new(1, 1);

    private static async Task<bool> TcpProbeCachedAsync(string host, int port, CancellationToken cancellationToken)
    {
        var key = $"{host}:{port}";
        if (TcpProbeCache.TryGetValue(key, out var cached) && cached.ExpiresUtc > DateTime.UtcNow)
            return cached.Ok;

        var ok = await TcpProbeAsync(host, port, cancellationToken).ConfigureAwait(false);
        TcpProbeCache[key] = (ok, DateTime.UtcNow.AddSeconds(15));
        return ok;
    }

    private static async Task<bool> TcpProbeAsync(string host, int port, CancellationToken cancellationToken)
    {
        try
        {
            using var tcp = new TcpClient();
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            linked.CancelAfter(2000);
            await tcp.ConnectAsync(host, port, linked.Token).ConfigureAwait(false);
            return tcp.Connected;
        }
        catch
        {
            return false;
        }
    }

    public sealed class SettingsLoginRequest
    {
        public string? Password { get; set; }
    }

    public sealed class SaveFormationChartRequest
    {
        public List<FormationChartRowDto>? Entries { get; set; }
    }

    public sealed class FormationChartRowDto
    {
        public string? PipeSize { get; set; }
        public int RequiredNdtPcs { get; set; }
    }

    public sealed class SaveMillPrintersRequest
    {
        public List<MillPrinterRowDto>? Mills { get; set; }
        public List<StationPrinterRowDto>? Stations { get; set; }

        /// <summary>Mill-1 WIP printer (optional; Mill-1 row in <see cref="Mills"/> is also treated as WIP).</summary>
        public PrinterEndpointDto? Mill1Wip { get; set; }

        /// <summary>Mill-1 FG printer IP (toggled plant-side with WIP).</summary>
        public PrinterEndpointDto? Mill1Fg { get; set; }

        /// <summary>PAS Room printer used for operator-desk reprints.</summary>
        public PrinterEndpointDto? PasRoom { get; set; }
    }

    public sealed class MillPrinterRowDto
    {
        public int MillNo { get; set; }
        public string? Address { get; set; }
        public int Port { get; set; } = 9100;
    }

    public sealed class PrinterEndpointDto
    {
        public string? Address { get; set; }
        public int Port { get; set; } = 9100;
    }

    public sealed class StationPrinterRowDto
    {
        public string? StationCode { get; set; }
        public string? Address { get; set; }
        public int Port { get; set; } = 9100;
    }

    public sealed class TestMillPrinterRequest
    {
        public int MillNo { get; set; }
        public string? StationCode { get; set; }

        /// <summary>Optional: <c>wip</c>, <c>fg</c>, or <c>pas</c>.</summary>
        public string? PrinterTarget { get; set; }
    }

    public sealed class TestPoChangeRequest
    {
        public int MillNo { get; set; }
    }

    public sealed class TestHooterRequest
    {
        /// <summary>
        /// When true (default), require MW56 &gt; MW58 like production.
        /// When false, still apply PAS enable + Q-already-on gates and WriteOutputBit.
        /// </summary>
        public bool RequireThreshold { get; set; } = true;
    }

    public sealed class OpenAccumulationOverrideRequest
    {
        /// <summary>NDT pipes toward the next bundle close for the active pipe size.</summary>
        public int Accumulated { get; set; }

        /// <summary>Optional; defaults to the mill's running PO.</summary>
        public string? PoNumber { get; set; }

        /// <summary>Optional formation-chart size key; defaults from running PO pipe size.</summary>
        public string? SizeKey { get; set; }
    }

    public sealed class MillSequenceSetRequest
    {
        public int MillNo { get; set; }
        public int CurrentSequence { get; set; }
        public string Reason { get; set; } = string.Empty;
        public string? UpdatedBy { get; set; }
        public bool ForceBelowLiveMax { get; set; }
    }
}
