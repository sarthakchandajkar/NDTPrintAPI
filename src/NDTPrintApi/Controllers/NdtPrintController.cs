using Microsoft.AspNetCore.Mvc;
using NDTPrintApi.Core.Models;
using NDTPrintApi.Services;

namespace NDTPrintApi.Controllers;

/// <summary>
/// API for printing NDT pipe bundle tags.
/// </summary>
[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
public class NdtPrintController : ControllerBase
{
    private readonly INdtPrintService _ndtPrintService;

    public NdtPrintController(INdtPrintService ndtPrintService)
    {
        _ndtPrintService = ndtPrintService;
    }

    /// <summary>
    /// Print an NDT pipe bundle tag for the given bundle and mill.
    /// </summary>
    /// <param name="request">Print request (BundleNo, MillNo, Reprint, optional NdtBundleId and PoPlanId).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Success or error response with echoed print details.</returns>
    [HttpPost("print")]
    [ProducesResponseType(typeof(NdtPrintResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(NdtPrintResponse), StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(NdtPrintResponse), StatusCodes.Status500InternalServerError)]
    public async Task<ActionResult<NdtPrintResponse>> Print(
        [FromBody] NdtPrintRequest request,
        CancellationToken cancellationToken)
    {
        if (request == null)
            return BadRequest(new NdtPrintResponse { Success = false, Message = "Request body is required." });

        try
        {
            var response = await _ndtPrintService.PrintAsync(request, cancellationToken).ConfigureAwait(false);

            if (!response.Success)
            {
                // Server-side failures (print error, DB/config) -> 500; validation/not found -> 400
                var isServerError = response.Message.StartsWith("Print failed:", StringComparison.OrdinalIgnoreCase)
                    || response.Message.Contains("connection string", StringComparison.OrdinalIgnoreCase);
                return isServerError
                    ? StatusCode(StatusCodes.Status500InternalServerError, response)
                    : BadRequest(response);
            }

            return Ok(response);
        }
        catch (Exception)
        {
            return StatusCode(StatusCodes.Status500InternalServerError,
                new NdtPrintResponse { Success = false, Message = "An unexpected error occurred while processing the print request." });
        }
    }
}
