using Microsoft.AspNetCore.Mvc;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using NdtBundleService.Services.PoChangeRemediation;

namespace NdtBundleService.Controllers;

/// <summary>
/// Dashboard wizard: backfill finished PO-change batches + single forming stamp on Shared.
/// Commit requires Settings unlock (<c>X-Settings-Token</c>).
/// </summary>
[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
[InstanceRole(InstanceRoleModes.Monolith, InstanceRoleModes.Shared)]
public sealed class PoChangeRemediationController : ControllerBase
{
    private readonly IPoChangeRemediationService _service;
    private readonly SettingsAuthService _auth;

    public PoChangeRemediationController(
        IPoChangeRemediationService service,
        SettingsAuthService auth)
    {
        _service = service;
        _auth = auth;
    }

    /// <summary>
    /// Scan mill for dual-forming / phantom CsvAdvance stamps and their slits (for UI consolidate).
    /// </summary>
    [HttpGet("mill-status")]
    public async Task<IActionResult> MillStatus(
        [FromQuery] int millNo,
        CancellationToken cancellationToken)
    {
        var result = await _service.GetMillStatusAsync(millNo, cancellationToken).ConfigureAwait(false);
        if (!result.Ok)
            return BadRequest(result);
        return Ok(result);
    }

    [HttpPost("preview")]
    public async Task<IActionResult> Preview(
        [FromBody] PoChangeRemediationRequest request,
        CancellationToken cancellationToken)
    {
        var result = await _service.PreviewAsync(request, cancellationToken).ConfigureAwait(false);
        if (!result.Ok)
            return BadRequest(result);
        return Ok(result);
    }

    [HttpPost("commit")]
    public async Task<IActionResult> Commit(
        [FromBody] PoChangeRemediationRequest request,
        CancellationToken cancellationToken)
    {
        if (!TryAuthorize(out var denied))
            return denied!;

        var result = await _service.CommitAsync(request, cancellationToken).ConfigureAwait(false);
        if (!result.Ok)
            return BadRequest(result);
        return Ok(result);
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
}
