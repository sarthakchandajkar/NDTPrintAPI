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
    private readonly ITraceabilityRepository _traceability;
    private readonly IFormationChartProvider _formationChartProvider;
    private readonly IPipeSizeProvider _pipeSizeProvider;
    private readonly IWipLabelProvider _wipLabelProvider;
    private readonly INetworkPrinterSender _printerSender;
    private readonly NdtBundleOptions _options;
    private readonly IZplGenerationToggle _zplToggle;
    private readonly ILogger<ReconcileController> _logger;

    public ReconcileController(
        INdtBundleRepository bundleRepository,
        ITraceabilityRepository traceability,
        IFormationChartProvider formationChartProvider,
        IPipeSizeProvider pipeSizeProvider,
        IWipLabelProvider wipLabelProvider,
        INetworkPrinterSender printerSender,
        IOptions<NdtBundleOptions> options,
        IZplGenerationToggle zplToggle,
        ILogger<ReconcileController> logger)
    {
        _bundleRepository = bundleRepository;
        _traceability = traceability;
        _formationChartProvider = formationChartProvider;
        _pipeSizeProvider = pipeSizeProvider;
        _wipLabelProvider = wipLabelProvider;
        _printerSender = printerSender;
        _options = options.Value;
        _zplToggle = zplToggle;
        _logger = logger;
    }

    /// <summary>
    /// List all NDT bundles (from database or from output CSV folder). Used for dropdown in Reconcile UI.
    /// </summary>
    [HttpGet("bundles")]
    public async Task<IActionResult> GetBundles(CancellationToken cancellationToken)
    {
        var list = await _bundleRepository.GetBundlesAsync(cancellationToken).ConfigureAwait(false);
        var filtered = await ExcludeOpenPartialLatestBatchesAsync(list, cancellationToken).ConfigureAwait(false);
        return Ok(filtered.Select(b => new
        {
            b.BundleNo,
            b.PoNumber,
            b.MillNo,
            b.TotalNdtPcs,
            b.SlitNo
        }).ToList());
    }

    private async Task<IReadOnlyList<NdtBundleRecord>> ExcludeOpenPartialLatestBatchesAsync(
        IReadOnlyList<NdtBundleRecord> bundles,
        CancellationToken cancellationToken)
    {
        var pipeSizes = await _pipeSizeProvider.GetPipeSizeByPoAsync(cancellationToken).ConfigureAwait(false);
        var formation = await _formationChartProvider.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);

        var byPoMill = bundles.GroupBy(b => (b.PoNumber, b.MillNo));
        var result = new List<NdtBundleRecord>();

        foreach (var group in byPoMill)
        {
            var threshold = ResolveThreshold(group.Key.PoNumber, pipeSizes, formation);
            var ordered = group
                .OrderBy(b => ParseBatchSequence(b.BundleNo))
                .ThenBy(b => b.BundleNo, StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (ordered.Count == 0)
                continue;

            var maxSeq = ParseBatchSequence(ordered[^1].BundleNo);
            foreach (var bundle in ordered)
            {
                var seq = ParseBatchSequence(bundle.BundleNo);
                var isLatest = seq == maxSeq;
                var isOpenPartial = isLatest && bundle.TotalNdtPcs > 0 && bundle.TotalNdtPcs < threshold;
                if (!isOpenPartial)
                    result.Add(bundle);
            }
        }

        return result
            .OrderByDescending(b => ParseBatchSequence(b.BundleNo))
            .ThenByDescending(b => b.BundleNo, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static int ResolveThreshold(
        string poNumber,
        IReadOnlyDictionary<string, string> pipeSizes,
        IReadOnlyDictionary<string, FormationChartEntry> formation)
    {
        pipeSizes.TryGetValue(poNumber, out var pipeSize);
        return FormationChartLookup.ResolveThreshold(formation, pipeSize);
    }

    private static int ParseBatchSequence(string bundleNo)
    {
        if (string.IsNullOrWhiteSpace(bundleNo) || bundleNo.Length < 5)
            return 0;
        var tail = bundleNo[^5..];
        return int.TryParse(tail, out var seq) ? seq : 0;
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
    /// Returns slit details for a bundle (per-slit totals) from per-slit output CSVs.
    /// </summary>
    [HttpGet("bundles/{ndtBatchNo}/slits")]
    public async Task<IActionResult> GetBundleSlits(string ndtBatchNo, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(ndtBatchNo))
            return BadRequest(new { Message = "NdtBatchNo is required." });

        var batchNo = ndtBatchNo.Trim();
        var bundle = await _bundleRepository.GetByBatchNoAsync(batchNo, cancellationToken).ConfigureAwait(false);
        if (bundle is null)
            return NotFound(new { Message = $"Bundle {batchNo} not found." });

        var slits = await _bundleRepository.GetSlitsForBatchAsync(batchNo, cancellationToken).ConfigureAwait(false);
        return Ok(new
        {
            Bundle = new
            {
                bundle.BundleNo,
                bundle.PoNumber,
                bundle.MillNo,
                bundle.TotalNdtPcs,
                bundle.SlitNo
            },
            Slits = slits.Select(s => new { SlitNo = s.SlitNo, NdtPipes = s.NdtPipes }).ToList()
        });
    }

    /// <summary>
    /// Reconcile a single slit within a bundle: overwrite the per-slit output CSV row(s) for that slit and batch,
    /// then recompute and persist the bundle total (DB + NDT_Bundle_*.csv if present).
    /// </summary>
    [HttpPost("reconcile-slit")]
    public async Task<IActionResult> ReconcileSlit([FromBody] ReconcileSlitRequest request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.NdtBatchNo))
            return BadRequest(new { Message = "NdtBatchNo is required." });
        if (string.IsNullOrWhiteSpace(request.SlitNo))
            return BadRequest(new { Message = "SlitNo is required." });
        if (request.NewNdtPipes < 0)
            return BadRequest(new { Message = "NewNdtPipes must be non-negative." });

        var batchNo = request.NdtBatchNo.Trim();
        var slitNo = request.SlitNo.Trim();

        var bundle = await _bundleRepository.GetByBatchNoAsync(batchNo, cancellationToken).ConfigureAwait(false);
        if (bundle is null)
            return NotFound(new { Message = $"Bundle {batchNo} not found." });

        var filesUpdated = await _bundleRepository.UpdateOutputCsvFilesForSlitAsync(batchNo, slitNo, request.NewNdtPipes, cancellationToken).ConfigureAwait(false);
        if (filesUpdated == 0)
            return NotFound(new { Message = $"No output CSV row found for bundle {batchNo} and slit {slitNo}." });

        var slits = await _bundleRepository.GetSlitsForBatchAsync(batchNo, cancellationToken).ConfigureAwait(false);
        var newTotal = slits.Sum(s => s.NdtPipes);

        await _bundleRepository.UpdateBundleTotalInDatabaseAsync(batchNo, newTotal, cancellationToken).ConfigureAwait(false);
        var summaryUpdated = await _bundleRepository.UpdateBundleSummaryCsvAsync(batchNo, newTotal, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "Reconciled slit {SlitNo} for bundle {BatchNo}: NewNdtPipes={NewPipes}, FilesUpdated={FilesUpdated}, NewTotal={NewTotal}, SummaryUpdated={SummaryUpdated}.",
            slitNo, batchNo, request.NewNdtPipes, filesUpdated, newTotal, summaryUpdated);

        return Ok(new
        {
            Message = "Slit reconciled. Output CSV updated and bundle total recomputed.",
            NdtBatchNo = batchNo,
            SlitNo = slitNo,
            NewNdtPipes = request.NewNdtPipes,
            FilesUpdated = filesUpdated,
            NewBundleTotalNdtPcs = newTotal,
            BundleSummaryUpdated = summaryUpdated,
            Slits = slits.Select(s => new { SlitNo = s.SlitNo, NdtPipes = s.NdtPipes }).ToList()
        });
    }

    /// <summary>
    /// Removes selected slit row(s) for a bundle from per-slit output CSVs (deletes the file when no data rows remain),
    /// deletes matching Output_Slit_Row traceability rows when SQL is configured (Input_Slit_Row is unchanged), and recomputes bundle total.
    /// </summary>
    [HttpPost("delete-slits")]
    public async Task<IActionResult> DeleteSlits([FromBody] DeleteSlitsRequest request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.NdtBatchNo))
            return BadRequest(new { Message = "NdtBatchNo is required." });
        if (request.SlitNos is null || request.SlitNos.Count == 0)
            return BadRequest(new { Message = "At least one SlitNo is required." });

        var batchNo = request.NdtBatchNo.Trim();
        var bundle = await _bundleRepository.GetByBatchNoAsync(batchNo, cancellationToken).ConfigureAwait(false);
        if (bundle is null)
            return NotFound(new { Message = $"Bundle {batchNo} not found." });

        var (rowsRemoved, traceRefs) = await _bundleRepository
            .DeletePerSlitOutputRowsForBatchSlitsAsync(batchNo, request.SlitNos, cancellationToken)
            .ConfigureAwait(false);
        if (rowsRemoved == 0)
            return NotFound(new { Message = $"No per-slit output rows found for bundle {batchNo} and the selected slit(s)." });

        await _traceability.DeleteOutputSlitRowsForRemovedOutputLinesAsync(batchNo, traceRefs, cancellationToken).ConfigureAwait(false);

        var slits = await _bundleRepository.GetSlitsForBatchAsync(batchNo, cancellationToken).ConfigureAwait(false);
        var newTotal = slits.Sum(s => s.NdtPipes);

        await _bundleRepository.UpdateBundleTotalInDatabaseAsync(batchNo, newTotal, cancellationToken).ConfigureAwait(false);
        var summaryUpdated = await _bundleRepository.UpdateBundleSummaryCsvAsync(batchNo, newTotal, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "Deleted {RowsRemoved} slit output row(s) for bundle {BatchNo}; trace refs {TraceCount}; new total {NewTotal}.",
            rowsRemoved, batchNo, traceRefs.Count, newTotal);

        return Ok(new
        {
            Message = "Selected slit row(s) removed from output CSV(s); Output_Slit_Row entries removed where configured (Input_Slit_Row unchanged); bundle total updated.",
            NdtBatchNo = batchNo,
            RowsRemoved = rowsRemoved,
            NewBundleTotalNdtPcs = newTotal,
            BundleSummaryUpdated = summaryUpdated,
            Slits = slits.Select(s => new { SlitNo = s.SlitNo, NdtPipes = s.NdtPipes }).ToList()
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

        if (!_zplToggle.IsEnabled)
            return BadRequest(new { Message = "NDT tag ZPL and network print are disabled (runtime toggle)." });

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

    public sealed class ReconcileSlitRequest
    {
        public string NdtBatchNo { get; set; } = string.Empty;
        public string SlitNo { get; set; } = string.Empty;
        public int NewNdtPipes { get; set; }
    }

    public sealed class DeleteSlitsRequest
    {
        public string NdtBatchNo { get; set; } = string.Empty;
        public List<string> SlitNos { get; set; } = new();
    }
}
