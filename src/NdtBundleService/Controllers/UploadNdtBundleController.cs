using Microsoft.AspNetCore.Mvc;
using NdtBundleService.Services;

namespace NdtBundleService.Controllers;

[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
public sealed class UploadNdtBundleController : ControllerBase
{
    private readonly IUploadNdtBundleFileService _service;

    public UploadNdtBundleController(IUploadNdtBundleFileService service)
    {
        _service = service;
    }

    [HttpPost("generate-now")]
    public async Task<IActionResult> GenerateNow(CancellationToken cancellationToken)
    {
        try
        {
            var result = await _service.GenerateAsync(cancellationToken).ConfigureAwait(false);
            return Ok(new
            {
                Message = "Upload NDT bundle CSV generated.",
                result.FilePath,
                result.RowCount
            });
        }
        catch (InvalidOperationException ex)
        {
            return BadRequest(new { Message = ex.Message });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { Message = ex.Message });
        }
    }
}

