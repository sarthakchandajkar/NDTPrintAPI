using Microsoft.AspNetCore.Mvc;
using NdtBundleService.Configuration;
using NdtBundleService.Services;

namespace NdtBundleService.Controllers;

[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
[InstanceRole(InstanceRoleModes.Monolith, InstanceRoleModes.Shared)]
public sealed class UploadNdtBundleController : ControllerBase
{
    private readonly IUploadNdtBundleFileService _service;

    public UploadNdtBundleController(IUploadNdtBundleFileService service)
    {
        _service = service;
    }

    /// <summary>
    /// Lists NDT batches that already have a MES PAS NDT upload CSV (SQL and/or folder).
    /// </summary>
    [HttpGet("generated")]
    public async Task<IActionResult> ListGenerated(
        [FromQuery] int take = 200,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var items = await _service.ListGeneratedAsync(take, cancellationToken).ConfigureAwait(false);
            return Ok(new
            {
                Count = items.Count,
                Items = items.Select(i => new
                {
                    i.NdtBatchNo,
                    i.PoNo,
                    i.MillNo,
                    i.SlitNo,
                    i.NumOfPipes,
                    i.FileName,
                    i.FilePath,
                    i.GeneratedAtUtc,
                    i.FileExistsOnDisk
                })
            });
        }
        catch (Exception ex)
        {
            return StatusCode(500, new { Message = ex.Message });
        }
    }

    /// <summary>
    /// Generates (or regenerates) the upload CSV for a batch. Creates a new timestamped file; does not delete older ones.
    /// </summary>
    [HttpPost("generate-now")]
    public async Task<IActionResult> GenerateNow(
        [FromBody] GenerateUploadNdtBundleRequest? request,
        CancellationToken cancellationToken)
    {
        var batch = request?.NdtBatchNo?.Trim();
        if (string.IsNullOrWhiteSpace(batch))
            return BadRequest(new { Message = "NdtBatchNo is required. Upload CSV is generated per bundle after Revisual." });

        try
        {
            var result = await _service.GenerateForBatchAsync(batch, cancellationToken).ConfigureAwait(false);
            return Ok(new
            {
                Message = "Upload NDT bundle CSV generated.",
                result.FilePath,
                result.RowCount,
                result.NdtBatchNo
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

public sealed class GenerateUploadNdtBundleRequest
{
    public string? NdtBatchNo { get; set; }
}
