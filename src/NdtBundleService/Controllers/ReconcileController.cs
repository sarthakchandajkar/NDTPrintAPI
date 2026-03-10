using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;

namespace NdtBundleService.Controllers;

/// <summary>
/// Reconcile Bundle: operators can change the NDT pipe count for a bundle and update DB and CSVs.
/// </summary>
[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
public sealed class ReconcileController : ControllerBase
{
    private readonly INdtBundleRepository _bundleRepository;
    private readonly IWipLabelProvider _wipLabelProvider;
    private readonly INetworkPrinterSender _printerSender;
    private readonly NdtBundleOptions _options;
    private readonly ILogger<ReconcileController> _logger;

    public ReconcileController(
        INdtBundleRepository bundleRepository,
        IWipLabelProvider wipLabelProvider,
        INetworkPrinterSender printerSender,
        IOptions<NdtBundleOptions> options,
        ILogger<ReconcileController> logger)
    {
        _bundleRepository = bundleRepository;
        _wipLabelProvider = wipLabelProvider;
        _printerSender = printerSender;
        _options = options.Value;
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
    /// Updates database (if configured) and all output CSV files containing this NDT Batch No.
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

        _logger.LogInformation("Reconciled bundle {BatchNo}: NewNdtPipes={NewPipes}, CsvFilesUpdated={Count}.", batchNo, request.NewNdtPipes, filesUpdated);
        return Ok(new
        {
            Message = "Bundle reconciled. CSV(s) updated.",
            NdtBatchNo = batchNo,
            NewNdtPipes = request.NewNdtPipes,
            CsvFilesUpdated = filesUpdated
        });
    }

    /// <summary>
    /// Print the selected bundle with its current (reconciled) NDT pipe count as a ZPL tag (with "Reprint" on the label).
    /// </summary>
    [HttpPost("print-bundle")]
    public async Task<IActionResult> PrintBundle([FromBody] PrintBundleRequest request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.NdtBatchNo))
            return BadRequest(new { Message = "NdtBatchNo is required." });

        var batchNo = request.NdtBatchNo.Trim();
        var bundle = await _bundleRepository.GetByBatchNoAsync(batchNo, cancellationToken).ConfigureAwait(false);
        if (bundle is null)
            return NotFound(new { Message = $"Bundle {batchNo} not found." });

        var address = (_options.NdtTagPrinterAddress ?? "").Trim();
        var useAddress = !string.IsNullOrEmpty(address) && !address.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase);
        if (!useAddress)
            return BadRequest(new { Message = "Printer not configured (NdtTagPrinterAddress)." });

        var wip = await _wipLabelProvider.GetWipLabelAsync(bundle.PoNumber, bundle.MillNo, cancellationToken).ConfigureAwait(false);
        var pipeGrade = wip?.PipeGrade;
        var pipeSize = wip?.PipeSize ?? "";
        var pipeThickness = wip?.PipeThickness ?? "";
        var pipeLength = wip?.PipeLength ?? "";
        var pipeWeight = wip?.PipeWeightPerMeter ?? "";
        var pipeType = wip?.PipeType ?? "";

        var zplBytes = ZplNdtLabelBuilder.BuildNdtTagZpl(
            bundle.BundleNo,
            bundle.MillNo,
            bundle.PoNumber,
            pipeGrade,
            pipeSize,
            pipeThickness,
            pipeLength,
            pipeWeight,
            pipeType,
            DateTime.Now,
            bundle.TotalNdtPcs,
            isReprint: true);

        _logger.LogInformation("Printing reconciled bundle {BatchNo} (Reprint) with {NdtPcs} pcs.", batchNo, bundle.TotalNdtPcs);

        try
        {
            var sent = await _printerSender.SendAsync(address, _options.NdtTagPrinterPort, zplBytes, cancellationToken).ConfigureAwait(false);
            if (sent)
                return Ok(new { Message = "Bundle tag (Reprint) sent to printer.", NdtBatchNo = batchNo, NdtPcs = bundle.TotalNdtPcs });
            return StatusCode(500, new { Message = "Failed to send to printer. Check printer configuration (NdtTagPrinterAddress/Port)." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Print bundle failed for {BatchNo}.", batchNo);
            return StatusCode(500, new { Message = "Print failed: " + ex.Message });
        }
    }

    public sealed class ReconcileRequest
    {
        public string NdtBatchNo { get; set; } = string.Empty;
        public int NewNdtPipes { get; set; }
    }

    public sealed class PrintBundleRequest
    {
        public string NdtBatchNo { get; set; } = string.Empty;
    }
}
