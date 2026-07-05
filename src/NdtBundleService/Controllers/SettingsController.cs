using System.Net.Sockets;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.PlcHandshake;

namespace NdtBundleService.Controllers;

/// <summary>
/// Password-protected dashboard settings: formation chart thresholds, PLC checks, per-mill printer IPs.
/// </summary>
[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
public sealed class SettingsController : ControllerBase
{
    private readonly SettingsAuthService _auth;
    private readonly IFormationChartSettingsService _formationChart;
    private readonly IMillPrinterSettingsService _millPrinters;
    private readonly IPlcClient _plcClient;
    private readonly PlcConnectionHealth _plcHealth;
    private readonly PoEndDetectionDiagnostics _poEndDiagnostics;
    private readonly PlcHandshakeStatusRegistry _handshakeStatus;
    private readonly PlcHandshakeCoordinator _handshakeCoordinator;
    private readonly IMillHooterPlcValuesService _hooterValues;
    private readonly NdtBundleOptions _options;
    private readonly ILogger<SettingsController> _logger;

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
        IOptions<NdtBundleOptions> options,
        ILogger<SettingsController> logger)
    {
        _auth = auth;
        _formationChart = formationChart;
        _millPrinters = millPrinters;
        _plcClient = plcClient;
        _plcHealth = plcHealth;
        _poEndDiagnostics = poEndDiagnostics;
        _handshakeStatus = handshakeStatus;
        _handshakeCoordinator = handshakeCoordinator;
        _hooterValues = hooterValues;
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
    /// </summary>
    [HttpPost("plc/test-po-change")]
    public async Task<IActionResult> TestPoChange(
        [FromBody] TestPoChangeRequest request,
        CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

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

        if (request?.MillNo is < 1 or > 4)
            return BadRequest(new { Message = "millNo must be 1–4." });

        _logger.LogInformation("Settings PO change test requested for Mill {Mill}.", request!.MillNo);

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
    /// </summary>
    [HttpPost("plc/mill/{millNo:int}/disconnect")]
    public IActionResult DisconnectMillPlc(int millNo)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        return SetMillPlcConnection(millNo, enabled: false);
    }

    /// <summary>Reconnect one mill's S7 handshake after a manual disconnect.</summary>
    [HttpPost("plc/mill/{millNo:int}/connect")]
    public IActionResult ConnectMillPlc(int millNo)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        return SetMillPlcConnection(millNo, enabled: true);
    }

    private IActionResult SetMillPlcConnection(int millNo, bool enabled)
    {
        if (millNo is < 1 or > 4)
            return BadRequest(new { Message = "millNo must be 1–4." });

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

        var action = enabled ? "connect" : "disconnect";
        _logger.LogInformation("Settings PLC {Action} requested for Mill {Mill}.", action, millNo);

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
                address = p.Address,
                port = p.Port,
                effectiveAddress = resolved.Address,
                effectivePort = resolved.Port,
                configured = resolved.Configured,
                reachable,
                status = !resolved.Configured ? "NotConfigured" : reachable ? "Ready" : "Unreachable"
            });
        }

        return Ok(new { Mills = results });
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

        var endpoints = request.Mills
            .Where(m => m.MillNo is >= 1 and <= 4)
            .Select(m => new MillPrinterEndpoint(
                m.MillNo,
                (m.Address ?? string.Empty).Trim(),
                m.Port > 0 ? m.Port : 9100))
            .ToList();

        await _millPrinters.SaveAllAsync(endpoints, cancellationToken).ConfigureAwait(false);
        return Ok(new { Message = "Printer settings saved.", Count = endpoints.Count });
    }

    [HttpPost("printers/test")]
    public async Task<IActionResult> TestPrinter(
        [FromBody] TestMillPrinterRequest request,
        CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        if (request?.MillNo is < 1 or > 4)
            return BadRequest(new { Message = "millNo must be 1–4." });

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
    }

    public sealed class MillPrinterRowDto
    {
        public int MillNo { get; set; }
        public string? Address { get; set; }
        public int Port { get; set; } = 9100;
    }

    public sealed class TestMillPrinterRequest
    {
        public int MillNo { get; set; }
    }

    public sealed class TestPoChangeRequest
    {
        public int MillNo { get; set; }
    }
}
