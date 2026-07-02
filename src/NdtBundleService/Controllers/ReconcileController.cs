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
    private readonly IReconcileSyncService _reconcileSync;
    private readonly IFormationChartProvider _formationChartProvider;
    private readonly IPipeSizeProvider _pipeSizeProvider;
    private readonly IWipLabelProvider _wipLabelProvider;
    private readonly INetworkPrinterSender _printerSender;
    private readonly IMillPrinterSettingsService _millPrinters;
    private readonly NdtBundleOptions _options;
    private readonly IZplGenerationToggle _zplToggle;
    private readonly ILogger<ReconcileController> _logger;

    public ReconcileController(
        INdtBundleRepository bundleRepository,
        ITraceabilityRepository traceability,
        IReconcileSyncService reconcileSync,
        IFormationChartProvider formationChartProvider,
        IPipeSizeProvider pipeSizeProvider,
        IWipLabelProvider wipLabelProvider,
        INetworkPrinterSender printerSender,
        IMillPrinterSettingsService millPrinters,
        IOptions<NdtBundleOptions> options,
        IZplGenerationToggle zplToggle,
        ILogger<ReconcileController> logger)
    {
        _bundleRepository = bundleRepository;
        _traceability = traceability;
        _reconcileSync = reconcileSync;
        _formationChartProvider = formationChartProvider;
        _pipeSizeProvider = pipeSizeProvider;
        _wipLabelProvider = wipLabelProvider;
        _millPrinters = millPrinters;
        _printerSender = printerSender;
        _options = options.Value;
        _zplToggle = zplToggle;
        _logger = logger;
    }

    /// <summary>
    /// List all NDT bundles (from database or from output CSV folder). Used for dropdown in Reconcile UI.
    /// </summary>
    [HttpGet("bundles")]
    public async Task<IActionResult> GetBundles(
        [FromQuery] bool includeOpenPartials = false,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var list = await _bundleRepository.GetBundlesAsync(CancellationToken.None).ConfigureAwait(false);
            var filtered = includeOpenPartials
                ? SortBundlesNewestFirst(list)
                : await ExcludeOpenPartialLatestBatchesAsync(list, CancellationToken.None).ConfigureAwait(false);
            return Ok(filtered.Select(b => new
            {
                b.BundleNo,
                b.PoNumber,
                b.MillNo,
                b.TotalNdtPcs,
                b.SlitNo,
                SlitStartTime = b.SlitStartTime,
                SlitFinishTime = b.SlitFinishTime,
                PrintedAt = b.PrintedAt
            }).ToList());
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning("GetBundles canceled before bundle list could be returned.");
            return StatusCode(499, new { Message = "Request canceled." });
        }
    }

    private async Task<IReadOnlyList<NdtBundleRecord>> ExcludeOpenPartialLatestBatchesAsync(
        IReadOnlyList<NdtBundleRecord> bundles,
        CancellationToken cancellationToken)
    {
        try
        {
            var pipeSizes = _pipeSizeProvider.TryGetCachedPipeSizes();
            if (pipeSizes is null)
            {
                _ = _pipeSizeProvider.GetPipeSizeByPoAsync(CancellationToken.None);
                return SortBundlesNewestFirst(bundles);
            }

            var formation = await _formationChartProvider.GetFormationChartAsync(CancellationToken.None).ConfigureAwait(false);

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

            return SortBundlesNewestFirst(result);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning(
                "Returning bundle list without open-partial filtering because the request was canceled while loading pipe sizes or formation chart.");
            return SortBundlesNewestFirst(bundles);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Returning bundle list without open-partial filtering because pipe sizes or formation chart could not be loaded.");
            return SortBundlesNewestFirst(bundles);
        }
    }

    private static IReadOnlyList<NdtBundleRecord> SortBundlesNewestFirst(IReadOnlyList<NdtBundleRecord> bundles) =>
        bundles
            .OrderByDescending(b => ParseBatchSequence(b.BundleNo))
            .ThenByDescending(b => b.BundleNo, StringComparer.OrdinalIgnoreCase)
            .ToList();

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

    private async Task<IActionResult> BuildReconcileSlitSuccessResponseAsync(
        string batchNo,
        string slitNo,
        int newNdtPipes,
        int filesUpdated,
        int sqlRowsUpdated,
        int fallbackBundleTotal,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<(string SlitNo, int NdtPipes)> slits = Array.Empty<(string, int)>();
        var bundleTotal = fallbackBundleTotal;
        var syncedTotal = 0;
        string? warning = null;

        try
        {
            slits = await _bundleRepository.GetSlitsForBatchAsync(batchNo, cancellationToken).ConfigureAwait(false);
            syncedTotal = await _bundleRepository
                .TrySyncBundleTotalFromSlitsAsync(batchNo, forceFromSlits: true, cancellationToken)
                .ConfigureAwait(false);
            var bundle = await _bundleRepository.GetByBatchNoAsync(batchNo, cancellationToken).ConfigureAwait(false);
            bundleTotal = bundle?.TotalNdtPcs ?? (syncedTotal > 0 ? syncedTotal : fallbackBundleTotal);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception postEx)
        {
            warning = "Slit saved successfully; reloading bundle totals failed. Refresh the page to see updated values.";
            _logger.LogWarning(
                postEx,
                "Slit reconcile persisted for bundle {BatchNo} slit {SlitNo} but post-update reload/sync failed.",
                batchNo,
                slitNo);
            if (slits.Count > 0)
                bundleTotal = slits.Sum(s => s.NdtPipes);
        }

        _logger.LogInformation(
            "Reconciled slit {SlitNo} for bundle {BatchNo}: NewNdtPipes={NewPipes}, FilesUpdated={FilesUpdated}, SqlRowsUpdated={SqlRowsUpdated}, BundleTotalNdtPcs={BundleTotal}.",
            slitNo,
            batchNo,
            newNdtPipes,
            filesUpdated,
            sqlRowsUpdated,
            bundleTotal);

        var message = filesUpdated > 0
            ? "Slit reconciled. Per-slit output CSV updated."
            : "Slit reconciled in SQL (no matching per-slit output CSV row was updated on disk).";
        if (sqlRowsUpdated > 0)
            message += $" {sqlRowsUpdated} Output_Slit_Row row(s) updated.";
        if (syncedTotal > 0)
            message += $" Bundle total synced to {syncedTotal} NDT pipe(s) from slit sum.";
        if (!string.IsNullOrWhiteSpace(warning))
            message += " " + warning;

        return Ok(new
        {
            Message = message,
            Warning = warning,
            NdtBatchNo = batchNo,
            SlitNo = ReconcileCsvParsing.NormalizeSlitKey(slitNo),
            NewNdtPipes = newNdtPipes,
            FilesUpdated = filesUpdated,
            SqlRowsUpdated = sqlRowsUpdated,
            NewBundleTotalNdtPcs = bundleTotal,
            BundleSummaryUpdated = syncedTotal > 0,
            Slits = slits.Select(s => new { SlitNo = s.SlitNo, NdtPipes = s.NdtPipes }).ToList()
        });
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
        var summaryUpdated = await _bundleRepository.UpdateBundleSummaryCsvAsync(batchNo, request.NewNdtPipes, cancellationToken).ConfigureAwait(false);

        await _reconcileSync.SyncAfterBundleTotalReconcileAsync(
            batchNo,
            bundle.PoNumber,
            request.NewNdtPipes,
            cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("Reconciled bundle {BatchNo}: NewNdtPipes={NewPipes}.", batchNo, request.NewNdtPipes);
        return Ok(new
        {
            Message = "Bundle reconciled. Output CSVs, NDT process CSV, bundle summary, and SQL traceability updated where configured.",
            NdtBatchNo = batchNo,
            NewNdtPipes = request.NewNdtPipes,
            BundleSummaryUpdated = summaryUpdated
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
    public async Task<IActionResult> ReconcileSlit([FromBody] ReconcileSlitRequest? request, CancellationToken cancellationToken)
    {
        if (request is null)
            return BadRequest(new { Message = "Request body is required." });

        if (string.IsNullOrWhiteSpace(request.NdtBatchNo))
            return BadRequest(new { Message = "NdtBatchNo is required." });
        if (request.NewNdtPipes < 0)
            return BadRequest(new { Message = "NewNdtPipes must be non-negative." });

        var batchNo = request.NdtBatchNo.Trim();
        var slitNo = ReconcileCsvParsing.NormalizeSlitKey(request.SlitNo);

        try
        {
            var bundle = await _bundleRepository.GetByBatchNoAsync(batchNo, cancellationToken).ConfigureAwait(false);
            if (bundle is null)
                return NotFound(new { Message = $"Bundle {batchNo} not found." });

            // Per-slit CSV + SQL updates can exceed HTTP/proxy timeouts on large UNC shares.
            var filesUpdated = await _bundleRepository
                .UpdateOutputCsvFilesForSlitAsync(batchNo, slitNo, request.NewNdtPipes, CancellationToken.None)
                .ConfigureAwait(false);

            var sqlRowsUpdated = await _reconcileSync
                .SyncAfterSlitReconcileAsync(batchNo, slitNo, request.NewNdtPipes, CancellationToken.None)
                .ConfigureAwait(false);

            if (filesUpdated == 0 && sqlRowsUpdated == 0)
            {
                return NotFound(new
                {
                    Message =
                        $"No per-slit output CSV row or SQL Output_Slit_Row entry found for bundle {batchNo} and slit {ReconcileCsvParsing.NormalizeSlitKey(slitNo)}."
                });
            }

            return await BuildReconcileSlitSuccessResponseAsync(
                batchNo,
                slitNo,
                request.NewNdtPipes,
                filesUpdated,
                sqlRowsUpdated,
                bundle.TotalNdtPcs,
                CancellationToken.None).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning("Reconcile slit canceled for bundle {BatchNo} slit {SlitNo}.", batchNo, slitNo);
            return StatusCode(499, new { Message = "Request canceled." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Reconcile slit failed for bundle {BatchNo} slit {SlitNo}.", batchNo, slitNo);
            return StatusCode(500, new
            {
                Message = "Slit reconcile failed.",
                Error = ex.Message,
                Detail = ex.GetType().Name
            });
        }
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
        var syncedTotal = await _bundleRepository
            .TrySyncBundleTotalFromSlitsAsync(batchNo, forceFromSlits: true, cancellationToken)
            .ConfigureAwait(false);
        bundle = await _bundleRepository.GetByBatchNoAsync(batchNo, cancellationToken).ConfigureAwait(false);
        var bundleTotal = bundle?.TotalNdtPcs ?? syncedTotal;

        _logger.LogInformation(
            "Deleted {RowsRemoved} slit output row(s) for bundle {BatchNo}; trace refs {TraceCount}; bundle total {BundleTotal}.",
            rowsRemoved, batchNo, traceRefs.Count, bundleTotal);

        return Ok(new
        {
            Message = syncedTotal > 0
                ? "Selected slit row(s) removed; bundle total synced from remaining slit sum."
                : "Selected slit row(s) removed from output CSV(s); Output_Slit_Row entries removed where configured (Input_Slit_Row unchanged).",
            NdtBatchNo = batchNo,
            RowsRemoved = rowsRemoved,
            NewBundleTotalNdtPcs = bundleTotal,
            BundleSummaryUpdated = syncedTotal > 0,
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

        var (address, printerPort, printerConfigured) = _millPrinters.ResolveForMill(bundle.MillNo);
        if (!printerConfigured)
            return BadRequest(new { Message = $"Printer not configured for Mill {bundle.MillNo} (Settings → printers)." });

        var wip = await _wipLabelProvider.GetWipLabelAsync(bundle.PoNumber, bundle.MillNo, cancellationToken).ConfigureAwait(false);
        var pipeGrade = wip?.PipeGrade;
        var pipeSize = wip?.PipeSize ?? "";
        var pipeThickness = wip?.PipeThickness ?? "";
        var pipeLength = wip?.PipeLength ?? "";
        var bundleWeight = NdtBundleWeightCalculator.FormatBundleWeight(
            wip?.PipeWeightPerMeter,
            pipeLength,
            bundle.TotalNdtPcs);
        var pipeType = wip?.PipeType ?? "";

        if (string.IsNullOrWhiteSpace(bundleWeight))
        {
            _logger.LogWarning(
                "Reprint tag for bundle {BatchNo} PO {PO}: bundle weight is empty (weight/m={Weight}, length={Length}, pcs={Pcs}).",
                batchNo,
                bundle.PoNumber,
                wip?.PipeWeightPerMeter ?? "(missing)",
                pipeLength,
                bundle.TotalNdtPcs);
        }

        var labelDate = bundle.PrintedAt ?? bundle.SlitFinishTime ?? bundle.SlitStartTime ?? DateTime.Now;

        var zplBytes = ZplNdtLabelBuilder.BuildNdtTagZpl(
            bundle.BundleNo,
            bundle.MillNo,
            bundle.PoNumber,
            pipeGrade,
            pipeSize,
            pipeThickness,
            pipeLength,
            bundleWeight,
            pipeType,
            labelDate,
            bundle.TotalNdtPcs,
            isReprint: true);

        _logger.LogInformation("Printing reconciled bundle {BatchNo} (Reprint) with {NdtPcs} pcs.", batchNo, bundle.TotalNdtPcs);

        try
        {
            await NdtBundleOutputPaths.TrySaveBundleZplAsync(_options, bundle.BundleNo, zplBytes, cancellationToken)
                .ConfigureAwait(false);

            var sendResult = await _printerSender.SendAsync(address, printerPort, zplBytes, cancellationToken).ConfigureAwait(false);
            if (sendResult.Success)
                return Ok(new { Message = "Bundle tag (Reprint) sent to printer.", NdtBatchNo = batchNo, NdtPcs = bundle.TotalNdtPcs });
            return StatusCode(500, new { Message = "Failed to send to printer. Check NdtTagPrinterAddress/Port and optional NdtTagPrinterLocalBindAddress.", Detail = sendResult.ErrorDetail });
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
