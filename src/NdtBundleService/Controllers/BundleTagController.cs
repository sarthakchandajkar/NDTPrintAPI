using Microsoft.AspNetCore.Mvc;
using NdtBundleService.Models;
using NdtBundleService.Services;

namespace NdtBundleService.Controllers;

/// <summary>
/// POC-style NDT bundle tag printing: single entry point PrintNDTBundleTag(printData) returns bool.
/// Layout and data shape match NDT_Bundle_Printing_POC / Rpt_NDTLabel.
/// </summary>
[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
public sealed class BundleTagController : ControllerBase
{
    private readonly INdtBundleTagPrinter _tagPrinter;

    public BundleTagController(INdtBundleTagPrinter tagPrinter)
    {
        _tagPrinter = tagPrinter;
    }

    /// <summary>
    /// Print an NDT bundle tag from print data (POC flow). Renders the tag (100×100mm, panel 9.7×9.5cm, same layout as Rpt_NDTLabel),
    /// sends to configured printer (e.g. port 9100) or saves PDF to OutputBundleFolder. Returns true on success.
    /// </summary>
    [HttpPost("print")]
    public async Task<ActionResult<PrintNDTBundleTagResponse>> PrintNDTBundleTag([FromBody] NDTBundlePrintData printData, CancellationToken cancellationToken)
    {
        if (printData == null)
            return BadRequest(new PrintNDTBundleTagResponse { Success = false, Message = "Print data is required." });

        var success = await _tagPrinter.PrintNDTBundleTagAsync(printData, cancellationToken).ConfigureAwait(false);
        return Ok(new PrintNDTBundleTagResponse
        {
            Success = success,
            Message = success ? "Tag sent to printer or saved to output folder." : "Print or save failed; check logs.",
            BundleNo = printData.BundleNo
        });
    }
}

public sealed class PrintNDTBundleTagResponse
{
    public bool Success { get; set; }
    public string Message { get; set; } = string.Empty;
    public string? BundleNo { get; set; }
}
