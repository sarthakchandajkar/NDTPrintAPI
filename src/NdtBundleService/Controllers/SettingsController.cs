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
        return Ok(new
        {
            Entries = entries.Select(e => new { pipeSize = e.PipeSize, requiredNdtPcs = e.RequiredNdtPcs }),
            SourcePath = _options.FormationChartCsvPath
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
    public async Task<IActionResult> GetPlcDiagnostics(CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        var handshakeCfg = _options.PlcHandshake ?? new PlcHandshakeOptions();
        if (handshakeCfg.Enabled)
        {
            var snapshot = _handshakeStatus.GetSnapshot();
            var poEndByMill = _handshakeStatus.GetPoEndByMill();
            var mills = new List<object>();

            foreach (var cfg in handshakeCfg.Mills.OrderBy(m => m.ResolveMillNo()))
            {
                var millNo = cfg.ResolveMillNo();
                var live = snapshot.FirstOrDefault(s => s.MillNo == millNo);
                var host = (cfg.IpAddress ?? string.Empty).Trim();
                var reachable = !string.IsNullOrEmpty(host) &&
                                await TcpProbeAsync(host, 102, cancellationToken).ConfigureAwait(false);

                mills.Add(new
                {
                    millNo,
                    name = cfg.Name,
                    driver = "S7-Handshake",
                    host,
                    port = 102,
                    reachable,
                    poEndAddress = cfg.TriggerAddress,
                    mesAckAddress = cfg.AckAddress,
                    handshakeConnected = live?.Connected ?? false,
                    triggerActive = live?.TriggerActive ?? false,
                    ackActive = live?.AckActive ?? false,
                    handshakeState = live?.HandshakeState ?? "Unknown",
                    lastPoChangeUtc = live?.LastPoChangeUtc,
                    lastError = live?.LastError,
                    testAvailable = _handshakeCoordinator.IsMillRegistered(millNo)
                });
            }

            return Ok(new
            {
                plcPoEndEnabled = true,
                plcHandshakeEnabled = true,
                driver = "S7-Handshake",
                lastReadOk = snapshot.Count > 0 && snapshot.All(m => m.Connected),
                lastPlcError = _handshakeStatus.FirstError() ?? _plcHealth.LastError,
                lastPlcCheckUtc = snapshot.Count > 0
                    ? snapshot.Max(m => m.LastUpdateUtc)
                    : _plcHealth.LastUpdateUtc,
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
                                await TcpProbeAsync(host, port, cancellationToken).ConfigureAwait(false);
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
                                await TcpProbeAsync(host, port, cancellationToken).ConfigureAwait(false);
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
