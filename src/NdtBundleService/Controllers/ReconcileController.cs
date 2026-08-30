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
[InstanceRole(InstanceRoleModes.Monolith, InstanceRoleModes.Shared)]
public sealed class ReconcileController : ControllerBase
{
    private const string PoMismatchWarningText =
        "Slit rows for this bundle reference a different PO (or multiple POs) than the bundle itself. "
        + "This usually means a provisional bundle-number collision or a recent number correction — verify the "
        + "physical tag and PO before acting on the displayed totals.";

    private readonly INdtBundleRepository _bundleRepository;
    private readonly ITraceabilityRepository _traceability;
    private readonly IReconcileSyncService _reconcileSync;
    private readonly IFormationChartProvider _formationChartProvider;
    private readonly IPipeSizeProvider _pipeSizeProvider;
    private readonly IReconcileBundleTagService _reconcileTagService;
    private readonly IOutputSlitSapStatusRepository _sapStatus;
    private readonly IPpcCorrectionRepository _ppcCorrections;
    private readonly IBundleMergeService _bundleMerge;
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly ILogger<ReconcileController> _logger;

    public ReconcileController(
        INdtBundleRepository bundleRepository,
        ITraceabilityRepository traceability,
        IReconcileSyncService reconcileSync,
        IFormationChartProvider formationChartProvider,
        IPipeSizeProvider pipeSizeProvider,
        IReconcileBundleTagService reconcileTagService,
        IOutputSlitSapStatusRepository sapStatus,
        IPpcCorrectionRepository ppcCorrections,
        IBundleMergeService bundleMerge,
        IOptionsMonitor<NdtBundleOptions> options,
        ILogger<ReconcileController> logger)
    {
        _bundleRepository = bundleRepository;
        _traceability = traceability;
        _reconcileSync = reconcileSync;
        _formationChartProvider = formationChartProvider;
        _pipeSizeProvider = pipeSizeProvider;
        _reconcileTagService = reconcileTagService;
        _sapStatus = sapStatus;
        _ppcCorrections = ppcCorrections;
        _bundleMerge = bundleMerge;
        _options = options;
        _logger = logger;
    }

    /// <summary>
    /// List all NDT bundles (from database or from output CSV folder). Used for dropdown in Reconcile UI.
    /// </summary>
    [HttpGet("bundles")]
    public async Task<IActionResult> GetBundles(
        [FromQuery] bool includeOpenPartials = false,
        [FromQuery] bool includeForming = false,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var list = await _bundleRepository.GetBundlesAsync(CancellationToken.None).ConfigureAwait(false);
            var filtered = includeForming || includeOpenPartials
                ? SortBundlesNewestFirst(list)
                : await ExcludeOpenPartialLatestBatchesAsync(list, CancellationToken.None).ConfigureAwait(false);

            if (includeForming)
                filtered = await MergeFormingBundlesAsync(filtered, CancellationToken.None).ConfigureAwait(false);

            var slitTotals = includeForming
                ? await _bundleRepository.GetSlitTotalsByBatchAsync(CancellationToken.None).ConfigureAwait(false)
                : null;
            var formingBatchNos = includeForming
                ? (await _bundleRepository.GetFormingBundlesFromSqlAsync(CancellationToken.None).ConfigureAwait(false))
                    .Select(b => b.BundleNo)
                    .ToHashSet(StringComparer.OrdinalIgnoreCase)
                : null;
            var poMismatchedBatches = (await _bundleRepository
                    .GetBatchesWithPoMismatchedSlitRowsAsync(CancellationToken.None)
                    .ConfigureAwait(false))
                .ToHashSet(StringComparer.OrdinalIgnoreCase);

            return Ok(filtered.Select(b =>
            {
                var slitSum = slitTotals is not null && slitTotals.TryGetValue(b.BundleNo, out var sum) ? sum : 0;
                var slitOnly = formingBatchNos?.Contains(b.BundleNo) == true;
                var isForming = includeForming && ReconcileFormingBundleHelper.IsForming(b, slitSum, slitOnly);
                var displayTotal = includeForming
                    ? ReconcileFormingBundleHelper.ResolveDisplayTotal(b, slitSum, isForming)
                    : b.TotalNdtPcs;
                var poMismatch = poMismatchedBatches.Contains(b.BundleNo);
                return new
                {
                    b.BundleNo,
                    b.PoNumber,
                    b.MillNo,
                    TotalNdtPcs = displayTotal,
                    b.SlitNo,
                    SlitStartTime = b.SlitStartTime,
                    SlitFinishTime = b.SlitFinishTime,
                    PrintedAt = b.PrintedAt,
                    b.ManualRecon,
                    b.ManualReconReason,
                    b.PostReconCsvSum,
                    IsForming = isForming,
                    SlitSum = includeForming ? slitSum : (int?)null,
                    PoMismatch = poMismatch,
                    PoMismatchWarning = poMismatch ? PoMismatchWarningText : null
                };
            }).ToList());
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogWarning("GetBundles canceled before bundle list could be returned.");
            return StatusCode(499, new { Message = "Request canceled." });
        }
    }

    private async Task<IReadOnlyList<NdtBundleRecord>> MergeFormingBundlesAsync(
        IReadOnlyList<NdtBundleRecord> bundles,
        CancellationToken cancellationToken)
    {
        var forming = await _bundleRepository.GetFormingBundlesFromSqlAsync(cancellationToken).ConfigureAwait(false);
        if (forming.Count == 0)
            return bundles;

        var byBatch = bundles.ToDictionary(b => b.BundleNo, StringComparer.OrdinalIgnoreCase);
        foreach (var row in forming)
        {
            if (string.IsNullOrWhiteSpace(row.BundleNo))
                continue;
            if (!byBatch.ContainsKey(row.BundleNo))
                byBatch[row.BundleNo] = row;
        }

        return SortBundlesNewestFirst(byBatch.Values.ToList());
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
                    if (ReconcileBundleListFilter.ShouldExcludeFromList(
                            isLatest,
                            bundle.TotalNdtPcs,
                            threshold,
                            bundle.ManualRecon,
                            bundle.PrintStatus))
                    {
                        continue;
                    }

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
        int? oldNdtPipes,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<(string SlitNo, int NdtPipes)> slits = Array.Empty<(string, int)>();
        var bundleTotal = fallbackBundleTotal;
        var syncedTotal = 0;
        var manualReconLocked = false;
        string? warning = null;

        try
        {
            slits = await _bundleRepository.GetSlitsForBatchAsync(batchNo, cancellationToken).ConfigureAwait(false);
            manualReconLocked = await _bundleRepository.IsManualReconLockedAsync(batchNo, cancellationToken).ConfigureAwait(false);
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
        if (manualReconLocked)
            message += $" Manual reconcile total remains {bundleTotal} NDT pipe(s).";
        else if (syncedTotal > 0)
            message += $" Bundle total synced to {syncedTotal} NDT pipe(s) from slit sum.";
        if (!string.IsNullOrWhiteSpace(warning))
            message += " " + warning;

        // Slit reconcile on SAP-Accepted data is allowed (MES-side correction, as production works
        // today) but SAP itself is unchanged. Phase 3: auto-create/refresh one Ppc_Correction_Item
        // per Accepted file so the needed SAP-side fix is tracked instead of an untracked email.
        // Nothing is sent automatically; the operator clears the item after PPC confirms the fix.
        IReadOnlyList<string> sapAcceptedFiles = Array.Empty<string>();
        var ppcItemsCreated = 0;
        var ppcItemsUpdated = 0;
        try
        {
            var (filesBySlit, statusByFile) = await GetSlitSapStatusAsync(batchNo, cancellationToken).ConfigureAwait(false);
            sapAcceptedFiles = ResolveSapAcceptedFilesForSlits(filesBySlit, statusByFile, new[] { slitNo });

            foreach (var fileName in sapAcceptedFiles)
            {
                var upsert = await _ppcCorrections
                    .UpsertOpenItemAsync(batchNo, fileName, slitNo, oldNdtPipes, newNdtPipes, cancellationToken)
                    .ConfigureAwait(false);
                if (upsert is null)
                    continue;
                if (upsert.Created)
                    ppcItemsCreated++;
                else
                    ppcItemsUpdated++;
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception sapEx)
        {
            _logger.LogWarning(sapEx, "SAP-Accepted check after slit reconcile failed for {BatchNo} slit {SlitNo}.", batchNo, slitNo);
        }

        if (sapAcceptedFiles.Count > 0)
        {
            message += $" Note: this slit's data comes from SAP-Accepted file(s) ({string.Join(", ", sapAcceptedFiles)});"
                       + " the local correction does not change SAP.";
            message += ppcItemsCreated + ppcItemsUpdated > 0
                ? $" A PPC correction item was {(ppcItemsCreated > 0 ? "created" : "updated")} — the bundle is marked"
                  + " 'PPC correction pending' until PPC applies the SAP-side fix and the item is cleared."
                : " Recording the PPC correction item failed (see service log) — email PPC manually.";
        }

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
            SapAcceptedFiles = sapAcceptedFiles,
            PpcCorrectionItemsCreated = ppcItemsCreated,
            PpcCorrectionItemsUpdated = ppcItemsUpdated,
            Slits = slits.Select(s => new { SlitNo = s.SlitNo, NdtPipes = s.NdtPipes }).ToList()
        });
    }

    /// <summary>
    /// Operator manual bundle reconcile: force-finalize awaiting recon when applicable, lock bundle,
    /// set corrected total, and reprint tag (with "Reprint" marker). Works with or without slit rows.
    /// </summary>
    [HttpPost("manual-bundle-reconcile")]
    public async Task<IActionResult> ManualBundleReconcile(
        [FromBody] ManualBundleReconcileRequest request,
        CancellationToken cancellationToken)
    {
        if (request is null)
            return BadRequest(new { Message = "Request body is required." });
        if (string.IsNullOrWhiteSpace(request.NdtBatchNo))
            return BadRequest(new { Message = "NdtBatchNo is required." });
        if (request.CorrectedTotal < 0)
            return BadRequest(new { Message = "CorrectedTotal must be non-negative." });

        var batchNo = request.NdtBatchNo.Trim();

        string? poMismatchWarning = null;
        try
        {
            var mismatched = await _bundleRepository
                .GetBatchesWithPoMismatchedSlitRowsAsync(cancellationToken)
                .ConfigureAwait(false);
            if (mismatched.Contains(batchNo, StringComparer.OrdinalIgnoreCase))
            {
                poMismatchWarning = PoMismatchWarningText;
                _logger.LogWarning(
                    "Manual bundle reconcile requested for {BatchNo} whose slit rows have PO mismatch/multiple POs — displayed totals may include another PO's pieces.",
                    batchNo);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "PO-mismatch pre-check failed for {BatchNo}; continuing with reconcile.", batchNo);
        }

        ManualBundleReconcileResult? result;
        try
        {
            result = await _bundleRepository
                .ManualReconcileBundleAsync(
                    batchNo,
                    request.CorrectedTotal,
                    reason: string.Empty,
                    reconciledBy: string.Empty,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Manual bundle reconcile failed for {BatchNo}.", batchNo);
            return StatusCode(500, new { Message = "Manual bundle reconcile failed.", Error = ex.Message });
        }

        if (result is null)
            return NotFound(new { Message = $"Bundle {batchNo} not found." });

        await _bundleRepository
            .UpdateBundleSummaryCsvAsync(batchNo, request.CorrectedTotal, CancellationToken.None)
            .ConfigureAwait(false);

        var bundleForPrint = await _bundleRepository
            .GetByBatchNoAsync(batchNo, cancellationToken)
            .ConfigureAwait(false)
            ?? result.Bundle;

        var printResult = await _reconcileTagService
            .ReprintAsync(bundleForPrint, cancellationToken)
            .ConfigureAwait(false);

        // Plan case (c): mid-fill corrected target below already-stamped CSV → open PPC resubmit items
        // for any SAP-Accepted files on this batch (operator email path; system never auto-emails).
        var ppcItemsCreated = 0;
        var ppcItemsUpdated = 0;
        IReadOnlyList<string> sapAcceptedFiles = Array.Empty<string>();
        if (result.FillOvershootVsCorrectedTarget)
        {
            try
            {
                var (filesBySlit, statusByFile) = await GetSlitSapStatusAsync(batchNo, cancellationToken)
                    .ConfigureAwait(false);
                var allSlits = filesBySlit.Keys.ToList();
                sapAcceptedFiles = ResolveSapAcceptedFilesForSlits(filesBySlit, statusByFile, allSlits);
                foreach (var fileName in sapAcceptedFiles)
                {
                    var upsert = await _ppcCorrections
                        .UpsertOpenItemAsync(
                            batchNo,
                            fileName,
                            slitNo: string.Empty,
                            oldNdtPipes: result.CsvFilledAtReconcile,
                            correctedNdtPipes: result.CorrectedTotal,
                            cancellationToken)
                        .ConfigureAwait(false);
                    if (upsert is null)
                        continue;
                    if (upsert.Created)
                        ppcItemsCreated++;
                    else
                        ppcItemsUpdated++;
                }
            }
            catch (Exception ppcEx)
            {
                _logger.LogWarning(
                    ppcEx,
                    "PPC correction upsert after manual reconcile overshoot failed for {BatchNo}.",
                    batchNo);
            }
        }

        return Ok(new
        {
            Message = printResult.Success
                ? "Bundle manually reconciled and tag reprinted."
                : "Bundle manually reconciled but tag reprint failed.",
            PrintSuccess = printResult.Success,
            PrintMessage = printResult.Message,
            PrintErrorDetail = printResult.ErrorDetail,
            NdtBatchNo = batchNo,
            OriginalTotal = result.OriginalTotal,
            CorrectedTotal = result.CorrectedTotal,
            ForceFinalized = result.ForceFinalized,
            CountDiscrepancyLogged = result.CountDiscrepancyLogged,
            SlitSumAtFinalize = result.SlitSumAtFinalize,
            ManualReconOriginalTotal = result.Bundle.ManualReconOriginalTotal ?? result.OriginalTotal,
            CsvFilledAtReconcile = result.CsvFilledAtReconcile,
            FillOvershootVsCorrectedTarget = result.FillOvershootVsCorrectedTarget,
            SapAcceptedFiles = sapAcceptedFiles,
            PpcCorrectionItemsCreated = ppcItemsCreated,
            PpcCorrectionItemsUpdated = ppcItemsUpdated,
            PoMismatchWarning = poMismatchWarning
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
        if (await _bundleRepository.IsManualReconLockedAsync(batchNo, cancellationToken).ConfigureAwait(false))
        {
            var locked = await _bundleRepository.GetByBatchNoAsync(batchNo, cancellationToken).ConfigureAwait(false);
            return Conflict(new
            {
                Message = "Bundle is manually reconciled and locked. Use manual bundle reconcile instead of slit-based reconcile.",
                ManualReconReason = locked?.ManualReconReason
            });
        }

        var bundle = await _bundleRepository.GetByBatchNoAsync(batchNo, cancellationToken).ConfigureAwait(false);
        if (bundle is null)
            return NotFound(new { Message = $"Bundle {batchNo} not found." });

        await _bundleRepository.UpdateBundlePipesAsync(batchNo, request.NewNdtPipes, CancellationToken.None).ConfigureAwait(false);
        var summaryUpdated = await _bundleRepository.UpdateBundleSummaryCsvAsync(batchNo, request.NewNdtPipes, CancellationToken.None).ConfigureAwait(false);

        await _reconcileSync.SyncAfterBundleTotalReconcileAsync(
            batchNo,
            bundle.PoNumber,
            request.NewNdtPipes,
            CancellationToken.None).ConfigureAwait(false);

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
        var fromDatabase = await _bundleRepository.GetByBatchNoAsync(batchNo, cancellationToken).ConfigureAwait(false);
        var bundle = fromDatabase
            ?? await _bundleRepository.TryGetBundleForReconcileAsync(batchNo, cancellationToken).ConfigureAwait(false);
        if (bundle is null)
            return NotFound(new { Message = $"Bundle {batchNo} not found." });

        var slits = await _bundleRepository.GetSlitsForBatchAsync(batchNo, cancellationToken).ConfigureAwait(false);
        var slitSum = slits.Sum(s => s.NdtPipes);
        var bundleIsForming = ReconcileFormingBundleHelper.IsForming(bundle, slitSum, fromDatabase is null);
        var displayTotal = ReconcileFormingBundleHelper.ResolveDisplayTotal(bundle, slitSum, bundleIsForming);
        var (filesBySlit, statusByFile) = await GetSlitSapStatusAsync(batchNo, cancellationToken).ConfigureAwait(false);
        var manualReview = await _bundleRepository.IsManualReviewFlaggedAsync(batchNo, cancellationToken).ConfigureAwait(false);
        // Derived, never stored: any Open Ppc_Correction_Item makes the bundle "PPC correction pending".
        var ppcOpenCount = await _ppcCorrections.CountOpenItemsForBatchAsync(batchNo, cancellationToken).ConfigureAwait(false);
        return Ok(new
        {
            Bundle = new
            {
                bundle.BundleNo,
                bundle.PoNumber,
                bundle.MillNo,
                TotalNdtPcs = displayTotal,
                bundle.SlitNo,
                bundle.ManualRecon,
                bundle.ManualReconReason,
                bundle.ManualReconOriginalTotal,
                bundle.PostReconCsvSum,
                bundle.AwaitingCsvRecon,
                ManualReview = manualReview,
                PpcCorrectionPending = ppcOpenCount > 0,
                PpcOpenCorrectionCount = ppcOpenCount,
                IsForming = bundleIsForming,
                SlitSum = slitSum
            },
            Slits = slits.Select(s =>
            {
                var files = filesBySlit.TryGetValue(ReconcileCsvParsing.NormalizeSlitKey(s.SlitNo), out var f)
                    ? f
                    : new List<string>();
                var known = files
                    .Select(fn => statusByFile.TryGetValue(fn, out var st) ? st : null)
                    .Where(static st => st is not null)
                    .Select(static st => st!)
                    .ToList();
                var strongest = OutputSlitSapStatusPolicy.Strongest(known.Select(static st => st.Status));
                DateTime? strongestAtUtc = strongest.HasValue
                    ? known.Where(st => st.Status == strongest.Value).Max(static st => (DateTime?)st.StatusAtUtc)
                    : null;
                return new
                {
                    SlitNo = s.SlitNo,
                    NdtPipes = s.NdtPipes,
                    SapStatus = strongest.HasValue ? OutputSlitSapStatusPolicy.ToDbString(strongest.Value) : null,
                    SapStatusAtUtc = strongestAtUtc,
                    ResubmitCount = known.Count > 0 ? known.Max(static st => st.ResubmitCount) : 0,
                    SourceFiles = files.Select(fn => new
                    {
                        FileName = fn,
                        SapStatus = statusByFile.TryGetValue(fn, out var st)
                            ? OutputSlitSapStatusPolicy.ToDbString(st.Status)
                            : null
                    }).ToList()
                };
            }).ToList()
        });
    }

    /// <summary>
    /// SAP status per source file for a batch's slit rows: slit key (normalized, empty → "—") to
    /// contributing output-file basenames, plus current status per basename. SQL-only and best-effort —
    /// both maps are empty when SQL is disabled, tables are missing, or the lookup fails.
    /// </summary>
    private async Task<(Dictionary<string, List<string>> FilesBySlit, IReadOnlyDictionary<string, OutputSlitSapFileStatus> StatusByFile)>
        GetSlitSapStatusAsync(string batchNo, CancellationToken cancellationToken)
    {
        var filesBySlit = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        IReadOnlyDictionary<string, OutputSlitSapFileStatus> statusByFile =
            new Dictionary<string, OutputSlitSapFileStatus>(StringComparer.OrdinalIgnoreCase);

        try
        {
            var pairs = await _bundleRepository
                .GetSlitSourceFileNamesForBatchAsync(batchNo, cancellationToken)
                .ConfigureAwait(false);
            foreach (var (slitNo, fileName) in pairs)
            {
                if (!filesBySlit.TryGetValue(slitNo, out var list))
                    filesBySlit[slitNo] = list = new List<string>();
                if (!list.Contains(fileName, StringComparer.OrdinalIgnoreCase))
                    list.Add(fileName);
            }

            var allFiles = filesBySlit.Values
                .SelectMany(static f => f)
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
            if (allFiles.Count > 0)
            {
                statusByFile = await _sapStatus
                    .GetStatusesForFilesAsync(allFiles, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "SAP status enrichment failed for bundle {BatchNo}.", batchNo);
        }

        return (filesBySlit, statusByFile);
    }

    /// <summary>Accepted-file basenames contributing to the requested slits (delete gate / reconcile note).</summary>
    private static IReadOnlyList<string> ResolveSapAcceptedFilesForSlits(
        Dictionary<string, List<string>> filesBySlit,
        IReadOnlyDictionary<string, OutputSlitSapFileStatus> statusByFile,
        IEnumerable<string> requestedSlitNos)
    {
        var accepted = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var requested in requestedSlitNos)
        {
            foreach (var (slitKey, files) in filesBySlit)
            {
                if (!ReconcileCsvParsing.SlitKeysMatch(slitKey, requested))
                    continue;
                foreach (var file in files)
                {
                    if (statusByFile.TryGetValue(file, out var status)
                        && status.Status == OutputSlitSapStatus.Accepted)
                        accepted.Add(file);
                }
            }
        }

        return accepted.ToList();
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

            // Pre-update slit value: when this correction touches SAP-Accepted data, this is the
            // value SAP still has — recorded on the auto-created PPC correction item.
            int? oldNdtPipes = null;
            try
            {
                var slitsBefore = await _bundleRepository.GetSlitsForBatchAsync(batchNo, cancellationToken).ConfigureAwait(false);
                foreach (var (existingSlitNo, ndtPipes) in slitsBefore)
                {
                    if (ReconcileCsvParsing.SlitKeysMatch(existingSlitNo, slitNo))
                    {
                        oldNdtPipes = ndtPipes;
                        break;
                    }
                }
            }
            catch (Exception oldEx)
            {
                _logger.LogDebug(oldEx, "Could not read pre-reconcile slit value for {BatchNo} slit {SlitNo}.", batchNo, slitNo);
            }

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

            await RefreshPostReconCsvSumIfLockedAsync(batchNo, CancellationToken.None).ConfigureAwait(false);

            return await BuildReconcileSlitSuccessResponseAsync(
                batchNo,
                slitNo,
                request.NewNdtPipes,
                filesUpdated,
                sqlRowsUpdated,
                bundle.TotalNdtPcs,
                oldNdtPipes,
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

    private async Task RefreshPostReconCsvSumIfLockedAsync(string batchNo, CancellationToken cancellationToken)
    {
        if (!await _bundleRepository.IsManualReconLockedAsync(batchNo, cancellationToken).ConfigureAwait(false))
            return;

        await _bundleRepository.TryUpdatePostReconCsvSumAsync(batchNo, cancellationToken).ConfigureAwait(false);
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

        // SAP-Accepted gate: slit data already posted to SAP must never be deleted locally —
        // that would silently diverge MES from SAP. Corrections go through PPC instead.
        var (filesBySlit, statusByFile) = await GetSlitSapStatusAsync(batchNo, cancellationToken).ConfigureAwait(false);
        var sapAcceptedFiles = ResolveSapAcceptedFilesForSlits(filesBySlit, statusByFile, request.SlitNos);
        if (sapAcceptedFiles.Count > 0)
        {
            _logger.LogWarning(
                "Delete-slits blocked for bundle {BatchNo}: slit(s) backed by SAP-Accepted file(s) {Files}.",
                batchNo,
                string.Join(", ", sapAcceptedFiles));
            return Conflict(new
            {
                Message = "Selected slit(s) contain data from SAP-Accepted input slit file(s). "
                          + "Deleting is blocked because this data is already posted in SAP — "
                          + "request the correction from PPC instead.",
                NdtBatchNo = batchNo,
                SapAcceptedFiles = sapAcceptedFiles
            });
        }

        var (rowsRemoved, traceRefs) = await _bundleRepository
            .DeletePerSlitOutputRowsForBatchSlitsAsync(batchNo, request.SlitNos, cancellationToken)
            .ConfigureAwait(false);
        if (rowsRemoved == 0)
            return NotFound(new { Message = $"No per-slit output rows found for bundle {batchNo} and the selected slit(s)." });

        await _traceability.DeleteOutputSlitRowsForRemovedOutputLinesAsync(batchNo, traceRefs, cancellationToken).ConfigureAwait(false);

        await RefreshPostReconCsvSumIfLockedAsync(batchNo, cancellationToken).ConfigureAwait(false);

        var slits = await _bundleRepository.GetSlitsForBatchAsync(batchNo, cancellationToken).ConfigureAwait(false);
        var syncedTotal = await _bundleRepository
            .TrySyncBundleTotalFromSlitsAsync(batchNo, forceFromSlits: true, cancellationToken)
            .ConfigureAwait(false);
        bundle = await _bundleRepository.GetByBatchNoAsync(batchNo, cancellationToken).ConfigureAwait(false);
        var bundleTotal = bundle?.TotalNdtPcs ?? syncedTotal;
        var manualReconLocked = await _bundleRepository.IsManualReconLockedAsync(batchNo, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "Deleted {RowsRemoved} slit output row(s) for bundle {BatchNo}; trace refs {TraceCount}; bundle total {BundleTotal}.",
            rowsRemoved, batchNo, traceRefs.Count, bundleTotal);

        return Ok(new
        {
            Message = manualReconLocked
                ? "Selected slit row(s) removed. Manual reconcile total unchanged."
                : syncedTotal > 0
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
    /// Preview merge of this live bundle into the immediately previous live bundle (same PO + mill).
    /// </summary>
    [HttpGet("bundles/{ndtBatchNo}/merge-preview")]
    public async Task<IActionResult> GetMergePreview(string ndtBatchNo, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(ndtBatchNo))
            return BadRequest(new { Message = "NdtBatchNo is required." });

        try
        {
            var preview = await _bundleMerge.TryPreviewAsync(ndtBatchNo.Trim(), cancellationToken).ConfigureAwait(false);
            if (preview is null)
                return NotFound(new { Message = "No immediately previous live bundle for this PO and mill." });
            return Ok(preview);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Merge preview failed for {Batch}.", ndtBatchNo);
            return StatusCode(500, new { Message = ex.Message });
        }
    }

    /// <summary>Merge this bundle into the immediately previous live bundle. Reason is required.</summary>
    [HttpPost("bundles/{ndtBatchNo}/merge-into-previous")]
    public async Task<IActionResult> MergeIntoPrevious(
        string ndtBatchNo,
        [FromBody] MergeIntoPreviousRequest? request,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(ndtBatchNo))
            return BadRequest(new { Message = "NdtBatchNo is required." });
        if (request is null || string.IsNullOrWhiteSpace(request.Reason))
            return BadRequest(new { Message = "Reason is required." });

        try
        {
            var result = await _bundleMerge
                .MergeIntoPreviousAsync(
                    ndtBatchNo.Trim(),
                    request.Reason.Trim(),
                    string.IsNullOrWhiteSpace(request.UpdatedBy) ? "Reconcile" : request.UpdatedBy.Trim(),
                    cancellationToken)
                .ConfigureAwait(false);
            return Ok(result);
        }
        catch (InvalidOperationException ex)
        {
            return BadRequest(new { Message = ex.Message });
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { Message = ex.Message });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Merge into previous failed for {Batch}.", ndtBatchNo);
            return StatusCode(500, new { Message = ex.Message });
        }
    }

    /// <summary>
    [HttpGet("bundles/{ndtBatchNo}/ppc-corrections")]
    public async Task<IActionResult> GetPpcCorrections(
        string ndtBatchNo,
        [FromQuery] bool includeCleared = false,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(ndtBatchNo))
            return BadRequest(new { Message = "NdtBatchNo is required." });

        var batchNo = ndtBatchNo.Trim();
        var items = await _ppcCorrections
            .GetItemsForBatchAsync(batchNo, includeCleared, cancellationToken)
            .ConfigureAwait(false);
        var openCount = items.Count(static i => string.Equals(i.Status, PpcCorrectionItem.StatusOpen, StringComparison.OrdinalIgnoreCase));

        return Ok(new
        {
            NdtBatchNo = batchNo,
            PpcCorrectionPending = openCount > 0,
            OpenCount = openCount,
            Items = items.Select(static i => new
            {
                i.Id,
                i.NdtBatchNo,
                i.FileName,
                i.SlitNo,
                i.OldNdtPipes,
                i.CorrectedNdtPipes,
                i.Status,
                i.CreatedAtUtc,
                i.UpdatedAtUtc,
                i.ClearedAtUtc,
                i.ClearedBy,
                i.ClearedNote
            }).ToList()
        });
    }

    /// <summary>
    /// Marks a PPC correction item cleared after PPC confirms the SAP-side fix was applied.
    /// Clearing the last open item removes the bundle's "PPC correction pending" status (derived).
    /// </summary>
    [HttpPost("ppc-corrections/{id:long}/clear")]
    public async Task<IActionResult> ClearPpcCorrection(
        long id,
        [FromBody] ClearPpcCorrectionRequest? request,
        CancellationToken cancellationToken)
    {
        if (id <= 0)
            return BadRequest(new { Message = "A valid correction item id is required." });

        var cleared = await _ppcCorrections
            .ClearItemAsync(id, request?.ClearedBy, request?.Note, cancellationToken)
            .ConfigureAwait(false);
        if (!cleared)
            return NotFound(new { Message = $"PPC correction item {id} was not found, is already cleared, or SQL is unavailable." });

        return Ok(new { Message = "PPC correction item cleared.", Id = id });
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

        var printResult = await _reconcileTagService.ReprintAsync(bundle, cancellationToken).ConfigureAwait(false);
        if (printResult.Success)
            return Ok(new { Message = printResult.Message, NdtBatchNo = batchNo, NdtPcs = bundle.TotalNdtPcs });
        return StatusCode(500, new
        {
            Message = printResult.Message,
            Detail = printResult.ErrorDetail,
            NdtBatchNo = batchNo,
            NdtPcs = bundle.TotalNdtPcs
        });
    }

    /// <summary>
    /// Operator-initiated Accepted resubmit: regenerate corrected pending CSV (same basename),
    /// bump Resubmit_Count via pending drop, return pre-filled email change summary. Never sends email.
    /// Future move UI can call the same transactional services used by detect-on-resubmit.
    /// </summary>
    [HttpPost("resubmit-accepted-file")]
    public async Task<IActionResult> ResubmitAcceptedFile(
        [FromBody] ResubmitAcceptedFileRequest request,
        CancellationToken cancellationToken)
    {
        if (request is null || string.IsNullOrWhiteSpace(request.FileName))
            return BadRequest(new { Message = "FileName is required." });

        var fileName = Path.GetFileName(request.FileName.Trim());
        var pendingFolder = (_options.CurrentValue.OutputBundleFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(pendingFolder))
            return BadRequest(new { Message = "OutputBundleFolder is not configured." });

        if (request.CsvLines is null || request.CsvLines.Count == 0)
            return BadRequest(new { Message = "CsvLines (full corrected CSV including header) are required." });

        Directory.CreateDirectory(pendingFolder);
        var path = Path.Combine(pendingFolder, fileName);
        await System.IO.File.WriteAllLinesAsync(path, request.CsvLines, cancellationToken).ConfigureAwait(false);

        var emailSummary =
            $"SAP NDT Input Slit resubmission request\r\n"
            + $"File: {fileName}\r\n"
            + $"Old batch: {request.OldBatchNo}\r\n"
            + $"New batch: {request.NewBatchNo}\r\n"
            + $"Corrected totals / notes: {request.ChangeNotes}\r\n"
            + "Please delete/correct the posted SAP record for the OLD key (if batch changed) and re-ingest the pending file.";

        _logger.LogWarning(
            "Operator Accepted resubmit: wrote {Path}. Email is NOT sent; present summary to operator.",
            path);

        return Ok(new
        {
            Message = "Corrected file written to pending folder. Email SAP manually using EmailChangeSummary.",
            FileName = fileName,
            PendingPath = path,
            EmailChangeSummary = emailSummary,
            Note = "Future UI move action should call the same ResubmitDrift / CsvFill batch-move services."
        });
    }

    public sealed class ResubmitAcceptedFileRequest
    {
        public string FileName { get; set; } = string.Empty;
        public IReadOnlyList<string> CsvLines { get; set; } = Array.Empty<string>();
        public string? OldBatchNo { get; set; }
        public string? NewBatchNo { get; set; }
        public string? ChangeNotes { get; set; }
    }

    public sealed class MergeIntoPreviousRequest
    {
        public string Reason { get; set; } = string.Empty;
        public string? UpdatedBy { get; set; }
    }

    public sealed class ManualBundleReconcileRequest
    {
        public string NdtBatchNo { get; set; } = string.Empty;
        public int CorrectedTotal { get; set; }
        public string Reason { get; set; } = string.Empty;
        public string ReconciledBy { get; set; } = string.Empty;
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

    public sealed class ClearPpcCorrectionRequest
    {
        public string? ClearedBy { get; set; }
        public string? Note { get; set; }
    }

    public sealed class DeleteSlitsRequest
    {
        public string NdtBatchNo { get; set; } = string.Empty;
        public List<string> SlitNos { get; set; } = new();
    }
}
