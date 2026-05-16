using System.Net.Sockets;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;

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
    private readonly ILogger<StatusController> _logger;

    public StatusController(
        IPlcClient plcClient,
        IOptions<NdtBundleOptions> options,
        IZplGenerationToggle zplToggle,
        PoEndDetectionDiagnostics poEndDiagnostics,
        PlcConnectionHealth plcHealth,
        ISqlTraceabilityHealth sqlHealth,
        ILogger<StatusController> logger)
    {
        _plcClient = plcClient;
        _options = options.Value;
        _zplToggle = zplToggle;
        _poEndDiagnostics = poEndDiagnostics;
        _plcHealth = plcHealth;
        _sqlHealth = sqlHealth;
        _logger = logger;
    }

    /// <summary>
    /// SQL traceability status for JazeeraMES_Prod: connectivity, table presence, row counts, and recent NDT_Bundle rows.
    /// </summary>
    [HttpGet("sql-traceability")]
    public async Task<IActionResult> GetSqlTraceabilityStatus(CancellationToken cancellationToken)
    {
        var report = await _sqlHealth.GetReportAsync(cancellationToken).ConfigureAwait(false);
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
            report.Database,
            report.DataSource,
            report.IsExpectedDatabase,
            report.MissingTables,
            report.RowCounts,
            report.RecentBundles,
            report.Error,
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
        var byMill = await _plcClient.GetPoEndSignalsByMillAsync(cancellationToken).ConfigureAwait(false);
        var poEndActive = byMill.Values.Any(v => v);
        var plcCfg = _options.PlcPoEnd;
        var modbusEnabled = plcCfg?.Enabled == true &&
            string.Equals(plcCfg.Driver, "ModbusTcp", StringComparison.OrdinalIgnoreCase);
        var connected = modbusEnabled && _plcHealth.LastReadOk == true;
        var diag = _poEndDiagnostics.GetSnapshot();
        var poIdMode = PlcPoEndOptions.IsModbusPoIdTransition(plcCfg);
        var message = !modbusEnabled
            ? "PLC PO-end polling uses Stub (all false) unless PlcPoEnd.Enabled=true and Driver=ModbusTcp."
            : poIdMode
                ? "Modbus TCP PO_Id transition mode (NdtBundle:PlcPoEnd.DetectionMode and Mills register map)."
                : "Modbus TCP PO-end coils from PlcPoEnd.Mills (per-mill POChangeTOMES / MES ack).";
        return Ok(new
        {
            Connected = connected,
            LastPlcError = _plcHealth.LastError,
            LastPlcCheckUtc = _plcHealth.LastUpdateUtc,
            PoEndActive = poEndActive,
            PlcPoEndEnabled = plcCfg?.Enabled ?? false,
            Driver = plcCfg?.Driver ?? "Stub",
            PoEndDetectionMode = plcCfg?.DetectionMode ?? "CoilRisingEdge",
            PoEndByMill = new Dictionary<string, bool>
            {
                ["1"] = byMill.TryGetValue(1, out var m1) && m1,
                ["2"] = byMill.TryGetValue(2, out var m2) && m2,
                ["3"] = byMill.TryGetValue(3, out var m3) && m3,
                ["4"] = byMill.TryGetValue(4, out var m4) && m4
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
