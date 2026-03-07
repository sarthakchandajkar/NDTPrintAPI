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
    private readonly ILogger<StatusController> _logger;

    public StatusController(IPlcClient plcClient, IOptions<NdtBundleOptions> options, ILogger<StatusController> logger)
    {
        _plcClient = plcClient;
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>
    /// PLC connection/signal status. Stub returns Connected; real implementation would reflect actual PLC state.
    /// </summary>
    [HttpGet("plc")]
    public async Task<IActionResult> GetPlcStatus(CancellationToken cancellationToken)
    {
        var poEndActive = await _plcClient.GetPoEndAsync(cancellationToken).ConfigureAwait(false);
        return Ok(new
        {
            Connected = true,
            PoEndActive = poEndActive
        });
    }

    /// <summary>
    /// Printer status. Indicates whether the NDT tag printer is configured and assumed ready.
    /// </summary>
    [HttpGet("printer")]
    public IActionResult GetPrinterStatus()
    {
        var hasPrinter = !string.IsNullOrWhiteSpace(_options.NdtTagPrinterName) ||
            (!string.IsNullOrWhiteSpace(_options.NdtTagPrinterAddress) &&
             !_options.NdtTagPrinterAddress.Trim().Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase));
        return Ok(new
        {
            Status = hasPrinter ? "Ready" : "NotConfigured",
            Message = hasPrinter ? "Printer configured." : "No printer configured (tags saved to PDF only)."
        });
    }
}
