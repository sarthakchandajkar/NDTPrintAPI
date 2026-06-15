using System.Net.Sockets;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using NdtBundleService.Services.PlcHandshake;

namespace NdtBundleService.Controllers;

/// <summary>
/// PLC and printer status for the dashboard.
/// </summary>
[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
public sealed class StatusController : ControllerBase
{
    private readonly IPlcClient _plcClient;
    private readonly NdtBundleOptions _options;
    private readonly IZplGenerationToggle _zplToggle;
    private readonly PoEndDetectionDiagnostics _poEndDiagnostics;
    private readonly PlcConnectionHealth _plcHealth;
    private readonly ISqlTraceabilityHealth _sqlHealth;
    private readonly ISqlTraceabilityWriteTracker _sqlWriteTracker;
    private readonly AppLogReader _appLogReader;
    private readonly PlcHandshakeStatusRegistry _handshakeStatus;
    private readonly ILogger<StatusController> _logger;

    public StatusController(
        IPlcClient plcClient,
        IOptions<NdtBundleOptions> options,
        IZplGenerationToggle zplToggle,
        PoEndDetectionDiagnostics poEndDiagnostics,
        PlcConnectionHealth plcHealth,
        PlcHandshakeStatusRegistry handshakeStatus,
        ISqlTraceabilityHealth sqlHealth,
        ISqlTraceabilityWriteTracker sqlWriteTracker,
        AppLogReader appLogReader,
        ILogger<StatusController> logger)
    {
        _plcClient = plcClient;
        _options = options.Value;
        _zplToggle = zplToggle;
        _poEndDiagnostics = poEndDiagnostics;
        _plcHealth = plcHealth;
        _handshakeStatus = handshakeStatus;
        _sqlHealth = sqlHealth;
        _sqlWriteTracker = sqlWriteTracker;
        _appLogReader = appLogReader;
        _logger = logger;
    }

    /// <summary>
    /// SQL traceability status for JazeeraMES_Prod: connectivity, table presence, row counts, and recent NDT_Bundle rows.
    /// </summary>
    [HttpGet("sql-traceability")]
    public async Task<IActionResult> GetSqlTraceabilityStatus(CancellationToken cancellationToken)
    {
        var report = await _sqlHealth.GetReportAsync(cancellationToken).ConfigureAwait(false);
        var recentWrites = _sqlWriteTracker.GetRecentResults();
        var healthy = report.Enabled
            && report.Connected
            && report.IsExpectedDatabase
            && report.MissingTables.Count == 0
            && string.IsNullOrWhiteSpace(report.Error);

        return Ok(new
        {
            Healthy = healthy,
            ExpectedDatabase = SqlTraceabilityHealth.ExpectedDatabaseName,
            report.Enabled,
            report.Connected,
            report.ConfiguredServer,
            report.ConfiguredDatabase,
            report.Database,
            report.DataSource,
            report.IsExpectedDatabase,
            report.MissingTables,
            report.RowCounts,
            report.RecentBundles,
            RecentWrites = recentWrites,
            report.Error,
            report.MachineName,
            report.ProcessWindowsLogin,
            SuggestedSqlLogins = report.SuggestedSqlLogins,
            Message = !report.Enabled
                ? "SQL traceability disabled in config."
                : !report.Connected
                    ? "Cannot connect to SQL Server. Check NdtBundle:ConnectionString or NdtBundle__ConnectionString."
                    : !report.IsExpectedDatabase
                        ? $"Connected to {report.Database}, not {SqlTraceabilityHealth.ExpectedDatabaseName}."
                        : report.MissingTables.Count > 0
                            ? "Connected but traceability tables are missing. Run docs/NDT_Traceability_Schema.sql."
                            : "Connected to JazeeraMES_Prod; traceability tables present."
        });
    }

    /// <summary>
    /// PLC connection/signal status. <see cref="Connected"/> is true only after a successful Modbus read when Modbus TCP is enabled.
    /// </summary>
    [HttpGet("plc")]
    public async Task<IActionResult> GetPlcStatus(CancellationToken cancellationToken)
    {
        var handshakeCfg = _options.PlcHandshake ?? new PlcHandshakeOptions();
        if (handshakeCfg.Enabled)
        {
            var byMill = _handshakeStatus.GetPoEndByMill();
            var poEndActive = byMill.Values.Any(v => v);
            var mills = _handshakeStatus.GetSnapshot();
            var connected = mills.Count > 0 && mills.All(m => m.Connected);

            return Ok(new
            {
                Connected = connected,
                LastPlcError = _handshakeStatus.FirstError() ?? _plcHealth.LastError,
                LastPlcCheckUtc = mills.Count > 0 ? mills.Max(m => m.LastUpdateUtc) : _plcHealth.LastUpdateUtc,
                PoEndActive = poEndActive,
                PlcPoEndEnabled = true,
                PlcHandshakeEnabled = true,
                Driver = "S7-Handshake",
                PoEndDetectionMode = "PersistentHandshake",
                PoEndByMill = new Dictionary<string, bool>
                {
                    ["1"] = byMill.TryGetValue(1, out var h1) && h1,
                    ["2"] = byMill.TryGetValue(2, out var h2) && h2,
                    ["3"] = byMill.TryGetValue(3, out var h3) && h3,
                    ["4"] = byMill.TryGetValue(4, out var h4) && h4
                },
                HandshakeMills = mills.Select(m => new
                {
                    m.MillName,
                    m.MillNo,
                    m.IpAddress,
                    m.Connected,
                    m.TriggerActive,
                    m.AckActive,
                    m.HandshakeState,
                    m.LastPoChangeUtc,
                    m.LastError,
                    m.LastUpdateUtc,
                    m.OkCount,
                    m.NokCount,
                    m.NdtCount,
                    m.LineRunning,
                    m.AccumulatedValue,
                    m.ThresholdValue,
                    m.HooterActive,
                    m.PoId,
                    m.SlitId,
                    m.CountsUpdatedUtc,
                    LastPoEnd = m.LastPoEnd is null
                        ? null
                        : new
                        {
                            m.LastPoEnd.PoId,
                            m.LastPoEnd.NdtCountFinal,
                            TimestampUtc = m.LastPoEnd.TimestampUtc
                        }
                }),
                Message = "Persistent per-mill S7 PO-change handshake plus DB251 OK/NOK/NDT counts on the same connection. " +
                    "When RecoverLatchedTriggerAtStartup is true, a latched trigger at connect is cleared via MES ack; " +
                    "RunPoEndWorkflowOnStartupRecovery controls whether bundle close also runs (default false). " +
                    "Use GET /api/Status/plc-live for dashboard polling."
            });
        }

        var legacyByMill = await _plcClient.GetPoEndSignalsByMillAsync(cancellationToken).ConfigureAwait(false);
        var legacyPoEndActive = legacyByMill.Values.Any(v => v);
        var plcCfg = _options.PlcPoEnd;
        var plcIoEnabled = plcCfg?.Enabled == true &&
            (PlcPoEndOptions.IsS7Driver(plcCfg) ||
             string.Equals(plcCfg.Driver, "ModbusTcp", StringComparison.OrdinalIgnoreCase));
        var legacyConnected = plcIoEnabled && _plcHealth.LastReadOk == true;
        var diag = _poEndDiagnostics.GetSnapshot();
        var poIdMode = PlcPoEndOptions.IsModbusPoIdTransition(plcCfg);
        var message = !plcIoEnabled
            ? "PLC PO-end polling uses Stub (all false) unless PlcPoEnd.Enabled=true and Driver=S7 or ModbusTcp."
            : PlcPoEndOptions.IsS7Driver(plcCfg)
                ? "Siemens S7 PO-end M-bits from PlcPoEnd.Mills (per-mill POChangeTOMES / MES ack), same map as plc-server."
                : poIdMode
                    ? "Modbus TCP PO_Id transition mode (NdtBundle:PlcPoEnd.DetectionMode and Mills register map)."
                    : "Modbus TCP PO-end coils from PlcPoEnd.Mills (per-mill POChangeTOMES / MES ack).";
        return Ok(new
        {
            Connected = legacyConnected,
            LastPlcError = _plcHealth.LastError,
            LastPlcCheckUtc = _plcHealth.LastUpdateUtc,
            PoEndActive = legacyPoEndActive,
            PlcPoEndEnabled = plcCfg?.Enabled ?? false,
            PlcHandshakeEnabled = false,
            Driver = plcCfg?.Driver ?? "Stub",
            PoEndDetectionMode = plcCfg?.DetectionMode ?? "CoilRisingEdge",
            PoEndByMill = new Dictionary<string, bool>
            {
                ["1"] = legacyByMill.TryGetValue(1, out var m1) && m1,
                ["2"] = legacyByMill.TryGetValue(2, out var m2) && m2,
                ["3"] = legacyByMill.TryGetValue(3, out var m3) && m3,
                ["4"] = legacyByMill.TryGetValue(4, out var m4) && m4
            },
            PoEndDetection = new
            {
                diag.DetectionMode,
                Mills = diag.Mills.Select((m, i) => new
                {
                    MillNo = i + 1,
                    m.CurrentPoIdFromPlc,
                    m.TrackedPrevPoId,
                    m.LastSlitEntryCount,
                    m.LastTransitionUtc,
                    m.LastEndedPoNumber,
                    m.LastError
                })
            },
            Message = message
        });
    }

    /// <summary>
    /// Live OK/NOK/NDT counts from DB251 when <see cref="PlcHandshakeOptions.Enabled"/> is true
    /// (same S7 connection as PO-change handshake — no plc-server required).
    /// </summary>
    [HttpGet("plc-live")]
    public IActionResult GetPlcLiveCounts()
    {
        var handshakeCfg = _options.PlcHandshake ?? new PlcHandshakeOptions();
        if (!handshakeCfg.Enabled)
        {
            return Ok(new
            {
                PlcHandshakeEnabled = false,
                Message = "PlcHandshake disabled; use plc-server Socket.IO for live counts.",
                Mills = Array.Empty<object>()
            });
        }

        var mills = _handshakeStatus.GetSnapshot();
        return Ok(new
        {
            PlcHandshakeEnabled = true,
            Mills = mills.Select(m => new
            {
                m.MillName,
                m.MillNo,
                m.IpAddress,
                m.Connected,
                Status = m.Connected ? "connected" : "disconnected",
                m.OkCount,
                m.NokCount,
                m.NdtCount,
                m.LineRunning,
                m.AccumulatedValue,
                m.ThresholdValue,
                m.HooterActive,
                m.PoId,
                m.SlitId,
                PoEndActive = m.TriggerActive,
                m.HandshakeState,
                m.LastError,
                Timestamp = FormatPlcTimestamp(m.CountsUpdatedUtc ?? m.LastUpdateUtc),
                LastUpdateUtc = m.LastUpdateUtc,
                LastPoEnd = m.LastPoEnd is null
                    ? null
                    : new
                    {
                        m.LastPoEnd.PoId,
                        m.LastPoEnd.NdtCountFinal,
                        Timestamp = FormatPlcTimestamp(m.LastPoEnd.TimestampUtc)
                    }
            })
        });
    }

    private static string FormatPlcTimestamp(DateTimeOffset utc)
    {
        var local = utc.LocalDateTime;
        return $"{local:yyyy-MM-dd HH:mm:ss}";
    }

    /// <summary>
    /// Printer status. When a TCP address is configured, performs a short connect probe to <see cref="NdtBundleOptions.NdtTagPrinterPort"/>.
    /// </summary>
    [HttpGet("printer")]
    public async Task<IActionResult> GetPrinterStatus(CancellationToken cancellationToken)
    {
        var addr = _options.NdtTagPrinterAddress?.Trim();
        var name = _options.NdtTagPrinterName?.Trim();
        var port = _options.NdtTagPrinterPort > 0 ? _options.NdtTagPrinterPort : 9100;

        var hasName = !string.IsNullOrWhiteSpace(name);
        var hasIp = !string.IsNullOrWhiteSpace(addr) &&
                    !addr.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase);

        if (!hasName && !hasIp)
        {
            return Ok(new
            {
                Status = "NotConfigured",
                Message = "No printer configured (tags saved to PDF only)."
            });
        }

        if (hasIp)
        {
            var reachable = await TryTcpReachableAsync(addr!, port, cancellationToken).ConfigureAwait(false);
            return Ok(new
            {
                Status = reachable ? "Ready" : "Unreachable",
                Message = reachable
                    ? "Printer TCP port is reachable."
                    : "Printer address is set but TCP connection failed (printer off-line or blocked)."
            });
        }

        return Ok(new
        {
            Status = "Configured",
            Message = "Printer name set (Windows/local); TCP reachability is not checked."
        });
    }

    private static async Task<bool> TryTcpReachableAsync(string host, int port, CancellationToken cancellationToken)
    {
        try
        {
            using var tcp = new TcpClient();
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            linked.CancelAfter(1500);
            await tcp.ConnectAsync(host, port, linked.Token).ConfigureAwait(false);
            return tcp.Connected;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Tail of the rolling application log file (for dashboard / remote troubleshooting).
    /// </summary>
    [HttpGet("logs")]
    public async Task<IActionResult> GetApplicationLogs([FromQuery] int lines = 200, CancellationToken cancellationToken = default)
    {
        var tail = await _appLogReader.ReadTailAsync(lines, cancellationToken).ConfigureAwait(false);
        return Ok(new
        {
            Folder = tail.Folder,
            File = tail.FileName,
            LineCount = tail.Lines.Count,
            Lines = tail.Lines
        });
    }

    [HttpGet("zpl-generation")]
    public IActionResult GetZplGenerationStatus()
    {
        return Ok(new
        {
            Enabled = _zplToggle.IsEnabled
        });
    }

    [HttpPost("zpl-generation")]
    public IActionResult SetZplGenerationStatus([FromBody] SetZplGenerationRequest request)
    {
        if (request is null)
            return BadRequest(new { Message = "Request body is required." });

        var enabled = _zplToggle.SetEnabled(request.Enabled);
        _logger.LogInformation("ZPL generation toggle set to {Enabled}.", enabled);
        return Ok(new { Enabled = enabled });
    }

    public sealed class SetZplGenerationRequest
    {
        public bool Enabled { get; set; }
    }
}
