using Microsoft.AspNetCore.Mvc;
using NdtBundleService.Models;
using NdtBundleService.Services;

namespace NdtBundleService.Controllers;

/// <summary>
/// Reconcile Bundle: operators can change the NDT pipe count for a bundle and update DB, CSVs, and reprint the tag.
/// </summary>
[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
public sealed class ReconcileController : ControllerBase
{
    private readonly INdtBundleRepository _bundleRepository;
    private readonly INdtLabelPrinter _labelPrinter;
    private readonly ILogger<ReconcileController> _logger;

    public ReconcileController(INdtBundleRepository bundleRepository, INdtLabelPrinter labelPrinter, ILogger<ReconcileController> logger)
    {
        _bundleRepository = bundleRepository;
        _labelPrinter = labelPrinter;
        _logger = logger;
    }

    /// <summary>
    /// List all NDT bundles (from database or from output CSV folder). Used for dropdown in Reconcile UI.
    /// </summary>
    [HttpGet("bundles")]
    public async Task<IActionResult> GetBundles(CancellationToken cancellationToken)
    {
        var list = await _bundleRepository.GetBundlesAsync(cancellationToken).ConfigureAwait(false);
        return Ok(list.Select(b => new
        {
            b.BundleNo,
            b.PoNumber,
            b.MillNo,
            b.TotalNdtPcs,
            b.SlitNo
        }).ToList());
    }

    /// <summary>
    /// Reconcile a bundle: set the NDT pipe count to the operator-specified value.
    /// Updates database (if configured), updates all output CSV files containing this NDT Batch No, and reprints the tag.
    /// </summary>
    [HttpPost("reconcile")]
    public async Task<IActionResult> Reconcile([FromBody] ReconcileRequest request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.NdtBatchNo))
            return BadRequest(new { Message = "NdtBatchNo is required." });
        if (request.NewNdtPipes < 0)
            return BadRequest(new { Message = "NewNdtPipes must be non-negative." });

        var batchNo = request.NdtBatchNo.Trim();
        var bundle = await _bundleRepository.GetByBatchNoAsync(batchNo, cancellationToken).ConfigureAwait(false);
        if (bundle is null)
            return NotFound(new { Message = $"Bundle {batchNo} not found." });

        await _bundleRepository.UpdateBundlePipesAsync(batchNo, request.NewNdtPipes, cancellationToken).ConfigureAwait(false);
        var filesUpdated = await _bundleRepository.UpdateOutputCsvFilesForBundleAsync(batchNo, request.NewNdtPipes, cancellationToken).ConfigureAwait(false);

        var contextRecord = new InputSlitRecord
        {
            PoNumber = bundle.PoNumber,
            SlitNo = bundle.SlitNo,
            MillNo = bundle.MillNo,
            NdtPipes = request.NewNdtPipes,
            RejectedPipes = bundle.RejectedPipes,
            SlitStartTime = bundle.SlitStartTime,
            SlitFinishTime = bundle.SlitFinishTime,
            NdtShortLengthPipe = bundle.NdtShortLengthPipe,
            RejectedShortLengthPipe = bundle.RejectedShortLengthPipe
        };

        try
        {
            await _labelPrinter.PrintLabelAsync(contextRecord, batchNo, request.NewNdtPipes, isReprint: true, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Reconcile: tag reprint failed for bundle {BatchNo}. DB and CSV were updated.", batchNo);
            return Ok(new
            {
                Message = "Bundle reconciled and CSV(s) updated. Tag reprint failed.",
                NdtBatchNo = batchNo,
                NewNdtPipes = request.NewNdtPipes,
                CsvFilesUpdated = filesUpdated
            });
        }

        _logger.LogInformation("Reconciled bundle {BatchNo}: NewNdtPipes={NewPipes}, CsvFilesUpdated={Count}, tag reprinted.", batchNo, request.NewNdtPipes, filesUpdated);
        return Ok(new
        {
            Message = "Bundle reconciled. CSV(s) updated and new tag printed.",
            NdtBatchNo = batchNo,
            NewNdtPipes = request.NewNdtPipes,
            CsvFilesUpdated = filesUpdated
        });
    }

    /// <summary>
    /// Reprint the tag for an existing bundle (no count change). Uses current TotalNdtPcs for that bundle.
    /// </summary>
    [HttpPost("reprint")]
    public async Task<IActionResult> Reprint([FromBody] ReprintRequest request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.NdtBatchNo))
            return BadRequest(new { Message = "NdtBatchNo is required." });

        var batchNo = request.NdtBatchNo.Trim();
        var bundle = await _bundleRepository.GetByBatchNoAsync(batchNo, cancellationToken).ConfigureAwait(false);
        if (bundle is null)
            return NotFound(new { Message = $"Bundle {batchNo} not found." });

        var contextRecord = new InputSlitRecord
        {
            PoNumber = bundle.PoNumber,
            SlitNo = bundle.SlitNo,
            MillNo = bundle.MillNo,
            NdtPipes = bundle.TotalNdtPcs,
            RejectedPipes = bundle.RejectedPipes,
            SlitStartTime = bundle.SlitStartTime,
            SlitFinishTime = bundle.SlitFinishTime,
            NdtShortLengthPipe = bundle.NdtShortLengthPipe,
            RejectedShortLengthPipe = bundle.RejectedShortLengthPipe
        };

        try
        {
            await _labelPrinter.PrintLabelAsync(contextRecord, batchNo, bundle.TotalNdtPcs, isReprint: true, cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("Reprinted tag for bundle {BatchNo}.", batchNo);
            return Ok(new { Message = "Tag reprinted.", NdtBatchNo = batchNo });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Reprint failed for bundle {BatchNo}.", batchNo);
            return StatusCode(500, new { Message = "Reprint failed.", NdtBatchNo = batchNo });
        }
    }

    public sealed class ReconcileRequest
    {
        public string NdtBatchNo { get; set; } = string.Empty;
        public int NewNdtPipes { get; set; }
    }

    public sealed class ReprintRequest
    {
        public string NdtBatchNo { get; set; } = string.Empty;
    }
}
