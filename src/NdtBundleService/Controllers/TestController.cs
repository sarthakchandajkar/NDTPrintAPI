using System.Linq;
using Microsoft.Data.SqlClient;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;

namespace NdtBundleService.Controllers;

/// <summary>
/// Test endpoints to simulate PO-end and inspect generated bundle CSV files.
/// Use Swagger UI to invoke these endpoints as a simple test UI.
/// </summary>
[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
    public sealed class TestController : ControllerBase
{
    private readonly ICurrentPoPlanService? _currentPoPlanService;
    private readonly INdtBundleTagPrinter _bundleTagPrinter;
    private readonly INetworkPrinterSender _networkPrinterSender;
    private readonly IZplGenerationToggle _zplToggle;
    private readonly IFormationChartProvider _formationChartProvider;
    private readonly IPipeSizeProvider _pipeSizeProvider;
    private readonly IPoPlanWipEnrichmentProvider _poPlanWipEnrichment;
    private readonly IPoPlanWipRepository _poPlanWipRepository;
    private readonly IActivePoPerMillService _activePoPerMill;
    private readonly IWipBundleRunningPoProvider _wipBundleRunningPo;
    private readonly IMillNdtCountReader _millNdtCountReader;
    private readonly IPoEndWorkflowService _poEndWorkflow;
    private readonly INdtBundleRuntimeStateStore _runtimeState;
    private readonly ILogger<TestController> _logger;
    private readonly NdtBundleOptions _options;

    public TestController(
        IOptions<NdtBundleOptions> options,
        ILogger<TestController> logger,
        INdtBundleTagPrinter bundleTagPrinter,
        INetworkPrinterSender networkPrinterSender,
        IZplGenerationToggle zplToggle,
        IFormationChartProvider formationChartProvider,
        IPipeSizeProvider pipeSizeProvider,
        IPoPlanWipEnrichmentProvider poPlanWipEnrichment,
        IPoPlanWipRepository poPlanWipRepository,
        IActivePoPerMillService activePoPerMill,
        IWipBundleRunningPoProvider wipBundleRunningPo,
        IMillNdtCountReader millNdtCountReader,
        IPoEndWorkflowService poEndWorkflow,
        INdtBundleRuntimeStateStore runtimeState,
        ICurrentPoPlanService? currentPoPlanService = null)
    {
        _bundleTagPrinter = bundleTagPrinter;
        _networkPrinterSender = networkPrinterSender;
        _zplToggle = zplToggle;
        _formationChartProvider = formationChartProvider;
        _pipeSizeProvider = pipeSizeProvider;
        _poPlanWipEnrichment = poPlanWipEnrichment;
        _poPlanWipRepository = poPlanWipRepository;
        _activePoPerMill = activePoPerMill;
        _wipBundleRunningPo = wipBundleRunningPo;
        _millNdtCountReader = millNdtCountReader;
        _poEndWorkflow = poEndWorkflow;
        _runtimeState = runtimeState;
        _currentPoPlanService = currentPoPlanService;
        _logger = logger;
        _options = options.Value;
    }

    public sealed class PoEndRequest
    {
        public string PoNumber { get; set; } = string.Empty;
        public int MillNo { get; set; }
    }

    public sealed class PoEndPendingDto
    {
        public string PoNumber { get; set; } = string.Empty;
        public int MillNo { get; set; }
        public int PendingFromSizeCounts { get; set; }
        public int PendingRunningTotal { get; set; }
        public string? ActivePoForMill { get; set; }
        public bool PoMatchesActiveMill { get; set; }
        public bool WaitingForNewWip { get; set; }
    }

    public sealed class PoEndResponseDto
    {
        public string Message { get; set; } = string.Empty;
        public string PoNumber { get; set; } = string.Empty;
        public int MillNo { get; set; }
        public int BundlesClosed { get; set; }
        public int TotalNdtPcsClosed { get; set; }
        public bool AdvancedPoPlanFile { get; set; }
        public bool WaitingForNewWip { get; set; }
        public string? ActivePoForMill { get; set; }
        public string? Warning { get; set; }
    }

    public sealed class BackfillBundleTotalsRequest
    {
        public string PoNumber { get; set; } = string.Empty;
        public int MillNo { get; set; }
        public bool DryRun { get; set; } = true;
    }

    public sealed class WipInfoDto
    {
        public string PoNumber { get; set; } = string.Empty;
        public string MillNumber { get; set; } = string.Empty;
        public string PlannedMonth { get; set; } = string.Empty;
        public string PipeGrade { get; set; } = string.Empty;
        public string PipeSize { get; set; } = string.Empty;
        public string PipeThickness { get; set; } = string.Empty;
        public string PipeLength { get; set; } = string.Empty;
        public string PipeWeightPerMeter { get; set; } = string.Empty;
        public string PipeType { get; set; } = string.Empty;
        public string OutputItemcode { get; set; } = string.Empty;
        public string ItemDescription { get; set; } = string.Empty;
        public string TotalPieces { get; set; } = string.Empty;
        public string PiecesPerBundle { get; set; } = string.Empty;
        public string ProductType { get; set; } = string.Empty;
        public string POSpecification { get; set; } = string.Empty;
        public string InputWipItemcode { get; set; } = string.Empty;
    }

    public sealed class NdtSummaryDto
    {
        public string PoNumber { get; set; } = string.Empty;
        public int MillNo { get; set; }
        public int TotalNdtPipes { get; set; }
    }

    public sealed class RunningPoNdtSummaryDto
    {
        public int MillNo { get; set; }
        public string PoNumber { get; set; } = string.Empty;
        public int TotalNdtPipes { get; set; }
    }

    /// <summary>One row of WIP plan data for a specific mill (for dashboard Summary).</summary>
    public sealed class WipByMillRowDto
    {
        public int MillNo { get; set; }
        public string PoNumber { get; set; } = string.Empty;
        public string PlannedMonth { get; set; } = string.Empty;
        public string PipeGrade { get; set; } = string.Empty;
        public string PipeSize { get; set; } = string.Empty;
        public string PipeLength { get; set; } = string.Empty;
        public string PiecesPerBundle { get; set; } = string.Empty;
        public string TotalPieces { get; set; } = string.Empty;
        /// <summary>Required NDT pipes per bundle from the formation chart for <see cref="PipeSize"/> (same rules as <see cref="NdtBundleEngine"/>).</summary>
        public int? NdtPcsPerBundle { get; set; }
    }

    /// <summary>
    /// Simulate PO-end for a given PO number and mill (1–4). Same workflow as a PLC PO-end: closes partial bundles, increments NDT batch state for that (PO, Mill),
    /// and optional PO plan file advance when configured for the active PO-end source (PLC handshake, file-based WIP, or legacy PlcPoEnd).
    /// </summary>
    [HttpPost("po-end")]
    public async Task<IActionResult> SimulatePoEnd([FromBody] PoEndRequest? request, CancellationToken cancellationToken)
    {
        if (request is null)
            return BadRequest(new { Message = "Request body is required." });

        if (string.IsNullOrWhiteSpace(request.PoNumber))
            return BadRequest(new { Message = "PoNumber is required." });

        if (request.MillNo is < 1 or > 4)
            return BadRequest(new { Message = "MillNo must be between 1 and 4." });

        var po = InputSlitCsvParsing.NormalizePo(request.PoNumber.Trim());
        var advancePlan = ShouldAdvancePoPlanOnPoEnd();

        _logger.LogInformation("Simulating PO end for PO {PO} Mill {Mill} (advance PO plan file: {Advance})", po, request.MillNo, advancePlan);

        string? activePoForMill = null;
        string? warning = null;
        try
        {
            var activePoByMill = await _activePoPerMill.GetLatestPoByMillAsync(cancellationToken).ConfigureAwait(false);
            if (activePoByMill.TryGetValue(request.MillNo, out var activePo) && !string.IsNullOrWhiteSpace(activePo))
            {
                activePoForMill = InputSlitCsvParsing.NormalizePo(activePo);
                if (!InputSlitCsvParsing.PoEquals(activePoForMill, po))
                {
                    warning =
                        $"Active PO for Mill {request.MillNo} is {activePoForMill}, but you simulated PO end for {po}. " +
                        "Partial bundles are keyed by the PO you enter — use the running PO for this mill if you expect open partials to close.";
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not resolve active PO for Mill {Mill} during simulate PO end.", request.MillNo);
        }

        PoEndWorkflowResult result;
        try
        {
            result = await _poEndWorkflow.ExecuteAsync(po, request.MillNo, advancePlan, cancellationToken).ConfigureAwait(false);
        }
        catch (ArgumentOutOfRangeException ex)
        {
            return BadRequest(new { Message = ex.Message });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Simulate PO end failed for PO {PO} Mill {Mill}.", po, request.MillNo);
            return StatusCode(500, new { Message = "PO end workflow failed.", Error = ex.Message });
        }

        var message = result.BundlesClosed > 0
            ? $"PO end simulated: closed {result.BundlesClosed} bundle(s), {result.TotalNdtPcsClosed} NDT pipe(s)."
            : "PO end simulated: no open partial bundles were found for this PO and mill.";

        if (result.WaitingForNewWip)
            message += " Mill is now waiting for a new WIP bundle file before slit bundling resumes.";

        return Ok(new PoEndResponseDto
        {
            Message = message,
            PoNumber = result.PoNumber,
            MillNo = result.MillNo,
            BundlesClosed = result.BundlesClosed,
            TotalNdtPcsClosed = result.TotalNdtPcsClosed,
            AdvancedPoPlanFile = result.AdvancedPoPlanFile,
            WaitingForNewWip = result.WaitingForNewWip,
            ActivePoForMill = activePoForMill,
            Warning = warning
        });
    }

    /// <summary>
    /// Open partial NDT counts for a PO/mill before simulating PO end (from in-memory runtime state).
    /// </summary>
    [HttpGet("po-end-pending")]
    public async Task<IActionResult> GetPoEndPending([FromQuery] string poNumber, [FromQuery] int millNo, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(poNumber))
            return BadRequest(new { Message = "poNumber is required." });

        if (millNo is < 1 or > 4)
            return BadRequest(new { Message = "millNo must be between 1 and 4." });

        var po = InputSlitCsvParsing.NormalizePo(poNumber.Trim());
        await _runtimeState.EnsureInitializedAsync(cancellationToken).ConfigureAwait(false);

        var sizeCounts = _runtimeState.GetSizeCounts(po, millNo);
        var pendingFromSizes = sizeCounts.Values.Where(static v => v > 0).Sum();
        var pendingRunningTotal = _runtimeState.GetRunningTotal(po, millNo);

        string? activePoForMill = null;
        var poMatchesActive = false;
        try
        {
            var activePoByMill = await _activePoPerMill.GetLatestPoByMillAsync(cancellationToken).ConfigureAwait(false);
            if (activePoByMill.TryGetValue(millNo, out var activePo) && !string.IsNullOrWhiteSpace(activePo))
            {
                activePoForMill = InputSlitCsvParsing.NormalizePo(activePo);
                poMatchesActive = InputSlitCsvParsing.PoEquals(activePoForMill, po);
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Could not resolve active PO for Mill {Mill} during po-end-pending.", millNo);
        }

        return Ok(new PoEndPendingDto
        {
            PoNumber = po,
            MillNo = millNo,
            PendingFromSizeCounts = pendingFromSizes,
            PendingRunningTotal = pendingRunningTotal,
            ActivePoForMill = activePoForMill,
            PoMatchesActiveMill = poMatchesActive,
            WaitingForNewWip = _wipBundleRunningPo.IsWaitingForNewWipAfterPoEnd(millNo)
        });
    }

    private bool ShouldAdvancePoPlanOnPoEnd()
    {
        if (_currentPoPlanService is null)
            return false;

        var o = _options;
        if (o.PlcHandshake?.Enabled == true)
            return o.PlcHandshake.AdvancePoPlanFileOnPoEnd;

        if (o.FileBasedPoEnd?.Enabled == true)
            return o.FileBasedPoEnd.AdvancePoPlanFileOnPoEnd;

        return o.PlcPoEnd?.AdvancePoPlanFileOnPoEnd == true;
    }

    /// <summary>
    /// Clears "waiting for new WIP bundle after PO end" for one mill and re-seeds running PO from the latest WIP file.
    /// Use when PO end was triggered in error (e.g. stale PLC latch at service startup while the same PO is still running).
    /// </summary>
    [HttpPost("resume-wip/{millNo:int}")]
    public async Task<IActionResult> ResumeWipAfterFalsePoEnd(int millNo, CancellationToken cancellationToken)
    {
        if (millNo is < 1 or > 4)
            return BadRequest(new { Message = "MillNo must be between 1 and 4." });

        var wasWaiting = _wipBundleRunningPo.IsWaitingForNewWipAfterPoEnd(millNo);
        var resumed = _wipBundleRunningPo.ResumeRunningWipForMill(millNo);
        var runningPo = await _wipBundleRunningPo.TryGetRunningPoForMillAsync(millNo, cancellationToken).ConfigureAwait(false);

        if (!resumed)
        {
            return Ok(new
            {
                Message = wasWaiting
                    ? "Mill was not in WIP-wait state (already resumed or never waiting)."
                    : "Mill was not waiting for new WIP after PO end; no change.",
                MillNo = millNo,
                WasWaitingForNewWip = wasWaiting,
                Resumed = false,
                RunningPo = runningPo
            });
        }

        _logger.LogInformation(
            "Resume WIP requested for Mill {Mill}; running PO is now {Po}.",
            millNo,
            runningPo ?? "(none)");

        return Ok(new
        {
            Message = "WIP wait cleared; slit processing can continue for the current PO from the latest WIP bundle file.",
            MillNo = millNo,
            WasWaitingForNewWip = true,
            Resumed = true,
            RunningPo = runningPo
        });
    }

    /// <summary>
    /// Returns the WIP (PO/size) information from the current PO plan. When PoPlanFolder is set, uses the current file from that folder; otherwise uses PoPlanCsvPath.
    /// </summary>
    [HttpGet("wip-info")]
    public async Task<IActionResult> GetWipInfo(CancellationToken cancellationToken)
    {
        try
        {
            var path = await ResolveWipPlanCsvPathAsync(cancellationToken).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(path))
            {
                if (TryGetUnreachablePoPlanFolderMessage(out var unreachable))
                    return NotFound(new { Message = unreachable, Path = _options.PoPlanFolder });
                return NotFound(new { Message = "WIP CSV path not configured (PoPlanCsvPath or PoPlanFolder)." });
            }
            if (path.StartsWith("NOTFOUND:", StringComparison.Ordinal))
                return NotFound(new { Message = "WIP CSV file not found.", Path = path["NOTFOUND:".Length..] });

            await using var stream = System.IO.File.OpenRead(path);
            using var reader = new StreamReader(stream);

            var headerLine = await reader.ReadLineAsync(cancellationToken);
            if (headerLine is null)
                return NotFound(new { Message = "WIP CSV is empty." });

            var headers = headerLine.Split(',');

            int IndexOf(string name)
            {
                for (var i = 0; i < headers.Length; i++)
                    if (string.Equals(headers[i].Trim(), name, StringComparison.OrdinalIgnoreCase))
                        return i;
                return -1;
            }

            var dataLine = await reader.ReadLineAsync(cancellationToken);
            if (string.IsNullOrWhiteSpace(dataLine))
                return NotFound(new { Message = "WIP CSV has no data rows." });

            var cols = dataLine.Split(',');

            string Get(string name)
            {
                var i = IndexOf(name);
                if (i < 0 || i >= cols.Length) return string.Empty;
                return cols[i].Trim();
            }

            var dto = new WipInfoDto
            {
                PoNumber = Get("PO_No"),
                MillNumber = Get("Mill Number"),
                PlannedMonth = Get("Planned Month"),
                PipeGrade = Get("Pipe Grade"),
                PipeSize = Get("Pipe Size"),
                PipeThickness = Get("Pipe Thickness"),
                PipeLength = Get("Pipe Length"),
                PipeWeightPerMeter = Get("Pipe Weight Per Meter"),
                PipeType = Get("Pipe Type"),
                OutputItemcode = Get("Output Itemcode"),
                ItemDescription = Get("Item Description"),
                TotalPieces = Get("Total Pieces"),
                PiecesPerBundle = Get("Pieces Per Bundle"),
                ProductType = Get("Product Type"),
                POSpecification = Get("PO Specification"),
                InputWipItemcode = Get("Input WIP Itemcode")
            };

            return Ok(dto);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GetWipInfo failed for path {Path}", _options.PoPlanCsvPath ?? _options.PoPlanFolder);
            return StatusCode(500, new { Message = "Failed to load WIP info.", Error = ex.Message });
        }
    }

    /// <summary>
    /// Returns WIP plan rows grouped for mills 1–4.
    /// <b>Current PO per mill</b> comes from the latest slit rows in <see cref="NdtBundleOptions.InputSlitFolder"/> and, when set,
    /// <see cref="NdtBundleOptions.InputSlitAcceptedFolder"/> (e.g. <c>Z:\To SAP\TM\Input Slit</c>) when <see cref="NdtBundleOptions.PreferInputSlitFilesForRunningPo"/> is true (default).
    /// SQL <c>Input_Slit_Row</c> fills mills missing from the inbox scan. WIP bundle filenames are used only when a mill has no slit PO yet.
    /// Pipe size, planned month, and SAP pieces/bundle are enriched from merged WIP files in <see cref="NdtBundleOptions.PoPlanFolder"/> when the PO matches.
    /// When <see cref="NdtBundleOptions.PoPlanFolder"/> is set, WIP CSVs are merged (newer files override per mill) for enrichment only.
    /// </summary>
    [HttpGet("wip-by-mills")]
    public async Task<IActionResult> GetWipByMills(CancellationToken cancellationToken)
    {
        // Dashboard polling must not abort on client disconnect/timeouts while reading SQL or UNC shares.
        var loadToken = CancellationToken.None;

        try
        {
            IReadOnlyDictionary<int, string> slitPoByMill;
            try
            {
                slitPoByMill = await _activePoPerMill.GetLatestPoByMillAsync(loadToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "GetLatestPoByMillAsync failed; continuing with empty slit PO map for wip-by-mills.");
                slitPoByMill = new Dictionary<int, string>();
            }

            PoPlanWipEnrichmentSnapshot wipEnrichment;
            string sourcePath;
            string? poPlanFolderWarning = null;
            if (TryGetUnreachablePoPlanFolderMessage(out var unreachablePoPlan))
            {
                poPlanFolderWarning = unreachablePoPlan;
                _logger.LogWarning("{Message}", unreachablePoPlan);
            }

            try
            {
                wipEnrichment = _poPlanWipEnrichment.TryGetCachedEnrichment()
                                ?? await _poPlanWipEnrichment.GetEnrichmentAsync(loadToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "GetEnrichmentAsync failed; continuing with empty PO plan enrichment for wip-by-mills.");
                wipEnrichment = EmptyWipEnrichment();
            }

            sourcePath = string.IsNullOrWhiteSpace(wipEnrichment.SourceDescription)
                ? $"{_options.PoPlanFolder ?? _options.PoPlanCsvPath ?? "PO plan"} (enrichment unavailable)"
                : wipEnrichment.SourceDescription;
            if (!string.IsNullOrWhiteSpace(poPlanFolderWarning))
                sourcePath += $"; {poPlanFolderWarning}";

            var mills = new List<WipByMillRowDto>(4);
            for (var m = 1; m <= 4; m++)
            {
                wipEnrichment.ByMill.TryGetValue(m, out var wipRowForMill);
                slitPoByMill.TryGetValue(m, out var slitPo);

                var bundleFilePo = string.IsNullOrWhiteSpace(slitPo)
                    ? await _wipBundleRunningPo.TryGetRunningPoForMillAsync(m, loadToken).ConfigureAwait(false)
                    : null;
                var resolvedPo = !string.IsNullOrWhiteSpace(slitPo)
                    ? slitPo
                    : bundleFilePo;

                if (!string.IsNullOrWhiteSpace(resolvedPo))
                {
                    var row = new WipByMillRowDto { MillNo = m, PoNumber = resolvedPo };
                    var normalizedPo = InputSlitCsvParsing.NormalizePo(resolvedPo);
                    if (wipEnrichment.ByPo.TryGetValue(normalizedPo, out var byPo))
                        CopyWipDetails(row, byPo);
                    else if (wipRowForMill != null && InputSlitCsvParsing.PoEquals(wipRowForMill.PoNumber, resolvedPo))
                        CopyWipDetails(row, wipRowForMill);

                    await TryFillWipDetailsFromSqlAsync(row, normalizedPo, loadToken).ConfigureAwait(false);

                    mills.Add(row);
                }
                else if (wipRowForMill != null && !string.IsNullOrWhiteSpace(wipRowForMill.PoNumber))
                {
                    mills.Add(ToWipByMillRowDto(wipRowForMill));
                }
                else
                {
                    mills.Add(new WipByMillRowDto { MillNo = m, PoNumber = string.Empty });
                }
            }

            var liveOpts = _options.MillSlitLive ?? new MillSlitLiveOptions();
            foreach (var row in mills)
            {
                if (string.IsNullOrWhiteSpace(row.PoNumber) || !string.IsNullOrWhiteSpace(row.PipeSize))
                    continue;
                try
                {
                    await WipBundleWipCsvEnricher.TryEnrichRowAsync(row, liveOpts, _logger, loadToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "WIP bundle CSV enrich skipped for mill {Mill} PO {Po}.", row.MillNo, row.PoNumber);
                }
            }

            IReadOnlyDictionary<string, string> pipeSizeByPo;
            try
            {
                pipeSizeByPo = _pipeSizeProvider.TryGetCachedPipeSizes()
                               ?? await _pipeSizeProvider.GetPipeSizeByPoAsync(loadToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "GetPipeSizeByPoAsync failed; pipe size columns may be empty on wip-by-mills.");
                pipeSizeByPo = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            }

            foreach (var row in mills)
            {
                if (string.IsNullOrWhiteSpace(row.PoNumber) || !string.IsNullOrWhiteSpace(row.PipeSize))
                    continue;
                var normalizedPo = InputSlitCsvParsing.NormalizePo(row.PoNumber);
                if (pipeSizeByPo.TryGetValue(normalizedPo, out var ps) && !string.IsNullOrWhiteSpace(ps))
                    row.PipeSize = ps.Trim();
            }

            IReadOnlyDictionary<string, FormationChartEntry> formation;
            try
            {
                formation = await _formationChartProvider.GetFormationChartAsync(loadToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "GetFormationChartAsync failed; NdtPcsPerBundle on wip-by-mills will use defaults where possible.");
                formation = new Dictionary<string, FormationChartEntry>(StringComparer.OrdinalIgnoreCase);
            }

            foreach (var row in mills)
            {
                if (string.IsNullOrWhiteSpace(row.PoNumber))
                {
                    row.NdtPcsPerBundle = null;
                    continue;
                }

                var pipeSize = (row.PipeSize ?? string.Empty).Trim();
                row.NdtPcsPerBundle = ResolveNdtPcsPerBundleFromChart(formation, pipeSize);
            }

            int? liveNdtCount = null;
            if (liveOpts.ApplyToMillNo is >= 1 and <= 4 && MillSlitLiveS7EndpointConfigured(liveOpts))
            {
                try
                {
                    liveNdtCount = await _millNdtCountReader.TryReadNdtPipesCountAsync(loadToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogDebug(ex, "Live mill NDT count read skipped for wip-by-mills.");
                }
            }

            var slitLocations = string.Join(" | ", _activePoPerMill.GetInputSlitReadFolderPaths().Where(static p => !string.IsNullOrWhiteSpace(p)));
            var bundleLoc = string.Join(
                " | ",
                new[] { liveOpts.WipBundleFolder, liveOpts.WipBundleAcceptedFolder }
                    .Where(static s => !string.IsNullOrWhiteSpace(s))
                    .Distinct(StringComparer.OrdinalIgnoreCase));
            var detail = string.IsNullOrEmpty(bundleLoc)
                ? $"WIP: {sourcePath}; current PO per mill from slit CSVs in: {slitLocations}"
                : $"WIP: {sourcePath}; PO from slits: {slitLocations}; when missing, from TM WIP bundle filenames in: {bundleLoc}";
            return Ok(new
            {
                Mills = mills,
                SourcePath = detail,
                LiveMillNdt = new { millNo = liveOpts.ApplyToMillNo, ndtCount = liveNdtCount },
            });
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            _logger.LogDebug("GetWipByMills canceled by client after building response payload.");
            return StatusCode(499, new { Message = "Request canceled." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GetWipByMills failed.");
            return StatusCode(500, new { Message = "Failed to load WIP by mills.", Error = ex.Message });
        }
    }

    /// <summary>
    /// Lightweight read of the live Siemens NDT counter for <see cref="MillSlitLiveOptions.ApplyToMillNo"/> (same DB read as the slit worker).
    /// Does not require <see cref="MillSlitLiveOptions.Enabled"/>; only the configured mill and S7 host must be set.
    /// Use for dashboard polling when Socket.IO is unavailable. Query <paramref name="millNo"/> defaults to <see cref="MillSlitLiveOptions.ApplyToMillNo"/> when 0.
    /// </summary>
    [HttpGet("live-mill-ndt")]
    public async Task<IActionResult> GetLiveMillNdt(CancellationToken cancellationToken, [FromQuery] int millNo = 0)
    {
        var live = _options.MillSlitLive;
        var m = millNo <= 0 ? live.ApplyToMillNo : millNo;
        if (m is < 1 or > 4)
            return BadRequest(new { Message = "millNo must be 1..4 (or 0 to use MillSlitLive.ApplyToMillNo)." });

        if (m != live.ApplyToMillNo)
        {
            return Ok(new
            {
                millNo = m,
                ndtCount = (int?)null,
                liveMillConfigured = live.ApplyToMillNo,
                message = $"Live NDT PLC read is only wired for Mill {live.ApplyToMillNo} (NdtBundle:MillSlitLive:ApplyToMillNo).",
            });
        }

        if (!MillSlitLiveS7EndpointConfigured(live))
        {
            return Ok(new
            {
                millNo = m,
                ndtCount = (int?)null,
                liveMillConfigured = live.ApplyToMillNo,
                message = "MillSlitLive S7 Host is not configured; cannot read NDT count from PLC.",
            });
        }

        var ndt = await _millNdtCountReader.TryReadNdtPipesCountAsync(cancellationToken).ConfigureAwait(false);
        return Ok(new { millNo = m, ndtCount = ndt });
    }

    private static bool MillSlitLiveS7EndpointConfigured(MillSlitLiveOptions live) =>
        live.S7 is not null && !string.IsNullOrWhiteSpace(live.S7.Host);

    private static void CopyWipDetails(WipByMillRowDto target, PoPlanWipRow source)
    {
        if (!string.IsNullOrWhiteSpace(source.PlannedMonth))
            target.PlannedMonth = source.PlannedMonth;
        if (!string.IsNullOrWhiteSpace(source.PipeGrade))
            target.PipeGrade = source.PipeGrade;
        if (!string.IsNullOrWhiteSpace(source.PipeSize))
            target.PipeSize = source.PipeSize;
        if (!string.IsNullOrWhiteSpace(source.PipeLength))
            target.PipeLength = source.PipeLength;
        if (!string.IsNullOrWhiteSpace(source.PiecesPerBundle))
            target.PiecesPerBundle = source.PiecesPerBundle;
        if (!string.IsNullOrWhiteSpace(source.TotalPieces))
            target.TotalPieces = source.TotalPieces;
    }

    private async Task TryFillWipDetailsFromSqlAsync(
        WipByMillRowDto row,
        string normalizedPo,
        CancellationToken cancellationToken)
    {
        if (!PoPlanWipSql.IsEnabled(_options)
            || string.IsNullOrWhiteSpace(normalizedPo)
            || !string.IsNullOrWhiteSpace(row.PipeSize))
        {
            return;
        }

        try
        {
            var sqlRow = await _poPlanWipRepository.TryGetLatestByPoAsync(normalizedPo, cancellationToken)
                .ConfigureAwait(false);
            if (sqlRow is not null)
                CopyWipDetails(row, sqlRow);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Per-PO SQL WIP lookup failed for PO {Po} on wip-by-mills.", normalizedPo);
        }
    }

    private static WipByMillRowDto ToWipByMillRowDto(PoPlanWipRow source) =>
        new()
        {
            MillNo = source.MillNo,
            PoNumber = source.PoNumber,
            PlannedMonth = source.PlannedMonth,
            PipeGrade = source.PipeGrade,
            PipeSize = source.PipeSize,
            PipeLength = source.PipeLength,
            PiecesPerBundle = source.PiecesPerBundle,
            TotalPieces = source.TotalPieces,
        };

    private static PoPlanWipEnrichmentSnapshot EmptyWipEnrichment() =>
        new(
            new Dictionary<int, PoPlanWipRow>(),
            new Dictionary<string, PoPlanWipRow>(StringComparer.OrdinalIgnoreCase),
            string.Empty);

    /// <summary>
    /// True when <see cref="NdtBundleOptions.PoPlanFolder"/> is set but <see cref="Directory.Exists"/> is false
    /// (common when the service runs as Local System and cannot see mapped <c>Z:\</c> drives).
    /// </summary>
    private bool TryGetUnreachablePoPlanFolderMessage(out string message)
    {
        message = string.Empty;
        var folder = _options.PoPlanFolder?.Trim();
        if (string.IsNullOrWhiteSpace(folder) || Directory.Exists(folder))
            return false;

        message =
            "PoPlanFolder is configured but not reachable from the NDT service account. " +
            "Mapped drives (Z:\\) are not visible to Local System—run NdtBundleService under a user that has Z: mapped, " +
            "or set UNC paths (\\\\server\\share\\...) in appsettings.Production.json.";
        return true;
    }

    /// <summary>Resolves path to the active WIP/PO plan CSV, or null if not configured, or NOTFOUND:originalPath if missing.</summary>
    private async Task<string?> ResolveWipPlanCsvPathAsync(CancellationToken cancellationToken)
    {
        string? path;
        if (!string.IsNullOrWhiteSpace(_options.PoPlanFolder) && _currentPoPlanService != null)
            path = await _currentPoPlanService.GetCurrentPoPlanPathAsync(cancellationToken).ConfigureAwait(false);
        else
            path = _options.PoPlanCsvPath;

        if (string.IsNullOrWhiteSpace(path))
            return null;

        if (System.IO.File.Exists(path))
            return path;

        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        string? fallback = null;
        for (var i = 0; i < 6 && dir != null; i++, dir = dir.Parent)
        {
            fallback = Directory.EnumerateFiles(dir.FullName, "WIP_*.csv").FirstOrDefault();
            if (fallback != null) break;
        }

        if (fallback != null)
        {
            _logger.LogInformation("Using fallback WIP CSV: {Path}", fallback);
            return fallback;
        }

        return "NOTFOUND:" + path;
    }

    /// <summary>Matches <see cref="NdtBundleEngine.ProcessSlitRecordAsync"/> formation lookup (normalized pipe size, then Default, then minimum 10).</summary>
    private static int ResolveNdtPcsPerBundleFromChart(
        IReadOnlyDictionary<string, FormationChartEntry> formation,
        string pipeSize)
    {
        return FormationChartLookup.ResolveThreshold(formation, pipeSize);
    }

    /// <summary>
    /// Returns total NDT pipes for a PO, optionally filtered to one mill.
    /// Counts come from Input Slit CSVs in <see cref="NdtBundleOptions.InputSlitFolder"/> and, when configured,
    /// <see cref="NdtBundleOptions.InputSlitAcceptedFolder"/>. This matches dashboard "active PO" totals from SAP slit flow.
    /// Files are read only; nothing is written, altered, or deleted.
    /// </summary>
    [HttpGet("ndt-summary")]
    public async Task<IActionResult> GetNdtSummary([FromQuery] string poNumber, [FromQuery] int? millNo, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(poNumber))
            return BadRequest(new { Message = "poNumber is required." });

        var poRequested = poNumber.Trim();
        var poNormalized = InputSlitCsvParsing.NormalizePo(poRequested);
        var minUtc = SourceFileEligibility.ParseMinUtc(_options);

        if (UseDatabaseForSummary)
        {
            var totalFromDb = await SumNdtPipesFromDbAsync(poRequested, poNormalized, millNo, minUtc, cancellationToken).ConfigureAwait(false);
            return Ok(new NdtSummaryDto
            {
                PoNumber = poNormalized,
                MillNo = millNo ?? 0,
                TotalNdtPipes = totalFromDb
            });
        }

        var readFolders = _activePoPerMill.GetInputSlitReadFolderPaths()
            .Where(static p => !string.IsNullOrWhiteSpace(p))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
        if (readFolders.Length == 0)
            return Ok(new NdtSummaryDto { PoNumber = poRequested, MillNo = millNo ?? 0, TotalNdtPipes = 0 });

        var total = 0;
        foreach (var folder in readFolders)
        {
            if (!Directory.Exists(folder))
                continue;

            foreach (var file in InputSlitInboxEnumeration.EnumerateFiles(folder))
            {
                if (!SourceFileEligibility.IncludeFileUtc(System.IO.File.GetLastWriteTimeUtc(file), minUtc))
                    continue;
                total += await SumNdtPipesFromFileAsync(file, poRequested, millNo, cancellationToken).ConfigureAwait(false);
            }
        }

        return Ok(new NdtSummaryDto
        {
            PoNumber = InputSlitCsvParsing.NormalizePo(poRequested),
            MillNo = millNo ?? 0,
            TotalNdtPipes = total
        });
    }

    /// <summary>Reads one slit/bundle-output CSV and sums <c>NDT Pipes</c> for the requested PO/mill. File is opened read-only.</summary>
    private static async Task<int> SumNdtPipesFromFileAsync(
        string file,
        string poRequested,
        int? millNo,
        CancellationToken cancellationToken)
    {
        await using var stream = System.IO.File.OpenRead(file);
        using var reader = new StreamReader(stream);

        var headerLine = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
        if (headerLine is null)
            return 0;

        headerLine = InputSlitCsvParsing.StripBom(headerLine);
        var headers = InputSlitCsvParsing.SplitCsvFields(headerLine);
        var poIndex = InputSlitCsvParsing.HeaderIndex(headers, "PO Number", "PO_No", "PO No");
        var ndtIndex = InputSlitCsvParsing.HeaderIndex(headers, "NDT Pipes");
        var millIndex = InputSlitCsvParsing.HeaderIndex(headers, "Mill No", "Mill Number");
        if (poIndex < 0 || ndtIndex < 0 || millIndex < 0)
            return 0;

        var sum = 0;
        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;

            var cols = InputSlitCsvParsing.SplitCsvFields(line);
            if (cols.Length == 0)
                continue;

            string Get(string[] c, int i)
            {
                if (i < 0 || i >= c.Length) return string.Empty;
                return c[i].Trim();
            }

            var poCell = Get(cols, poIndex);
            if (!InputSlitCsvParsing.PoEquals(poCell, poRequested))
                continue;

            var millRaw = Get(cols, millIndex);
            if (!InputSlitCsvParsing.TryParseMillNo(millRaw, out var rowMill))
                continue;

            if (millNo.HasValue && rowMill != millNo.Value)
                continue;

            var ndtRaw = Get(cols, ndtIndex);
            if (InputSlitCsvParsing.TryParseIntFlexible(ndtRaw, out var ndtVal))
                sum += ndtVal;
        }

        return sum;
    }

    /// <summary>
    /// Returns NDT pipe totals for the current running PO in each mill (1..4).
    /// Uses SQL aggregation from Input_Slit_Row when configured; falls back to CSV scanning otherwise.
    /// </summary>
    [HttpGet("ndt-summary-running-po")]
    public async Task<IActionResult> GetNdtSummaryForRunningPoByMill(CancellationToken cancellationToken)
    {
        var latestPoByMill = await _activePoPerMill.GetLatestPoByMillAsync(cancellationToken).ConfigureAwait(false);
        var minUtc = SourceFileEligibility.ParseMinUtc(_options);
        var readFolders = _activePoPerMill.GetInputSlitReadFolderPaths()
            .Where(static p => !string.IsNullOrWhiteSpace(p))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var result = new List<RunningPoNdtSummaryDto>(4);
        for (var mill = 1; mill <= 4; mill++)
        {
            latestPoByMill.TryGetValue(mill, out var poRaw);
            var poRequested = poRaw?.Trim() ?? string.Empty;
            var poNormalized = string.IsNullOrWhiteSpace(poRequested) ? string.Empty : InputSlitCsvParsing.NormalizePo(poRequested);
            if (string.IsNullOrWhiteSpace(poNormalized))
            {
                result.Add(new RunningPoNdtSummaryDto { MillNo = mill, PoNumber = string.Empty, TotalNdtPipes = 0 });
                continue;
            }

            int total;
            if (UseDatabaseForSummary)
            {
                total = await SumNdtPipesFromDbAsync(poRequested, poNormalized, mill, minUtc, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                total = 0;
                foreach (var folder in readFolders)
                {
                    if (!Directory.Exists(folder))
                        continue;

                    foreach (var file in InputSlitInboxEnumeration.EnumerateFiles(folder))
                    {
                        if (!SourceFileEligibility.IncludeFileUtc(System.IO.File.GetLastWriteTimeUtc(file), minUtc))
                            continue;
                        total += await SumNdtPipesFromFileAsync(file, poNormalized, mill, cancellationToken).ConfigureAwait(false);
                    }
                }
            }

            result.Add(new RunningPoNdtSummaryDto
            {
                MillNo = mill,
                PoNumber = poNormalized,
                TotalNdtPipes = total
            });
        }

        return Ok(result);
    }

    private bool UseDatabaseForSummary =>
        _options.UseSqlServerForBundles && !string.IsNullOrWhiteSpace(_options.ConnectionString);

    private async Task<int> SumNdtPipesFromDbAsync(
        string poRequested,
        string poNormalized,
        int? millNo,
        DateTime? minUtc,
        CancellationToken cancellationToken)
    {
        try
        {
            await using var conn = new SqlConnection(_options.ConnectionString);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

            const string sql = @"
WITH Dedup AS
(
    SELECT
        NDT_Pipes,
        ROW_NUMBER() OVER (
            PARTITION BY Source_File, Source_Row_Number
            ORDER BY ImportedAtUtc DESC, Input_Slit_Row_ID DESC
        ) AS src_rn
    FROM dbo.Input_Slit_Row
    WHERE (PO_Number = @PoRequested OR PO_Number = @PoNormalized)
      AND (@MillNo IS NULL OR Mill_No = @MillNo)
      AND (@MinUtc IS NULL OR ImportedAtUtc >= @MinUtc)
      AND Source_File IS NOT NULL
      AND Source_Row_Number IS NOT NULL
)
SELECT COALESCE(SUM(NDT_Pipes), 0)
FROM Dedup
WHERE src_rn = 1;";

            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@PoRequested", poRequested);
            cmd.Parameters.AddWithValue("@PoNormalized", poNormalized);
            cmd.Parameters.AddWithValue("@MillNo", millNo.HasValue ? (object)millNo.Value : DBNull.Value);
            cmd.Parameters.AddWithValue("@MinUtc", minUtc.HasValue ? (object)minUtc.Value : DBNull.Value);

            var scalar = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (scalar is null || scalar is DBNull)
                return 0;
            return Convert.ToInt32(scalar);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "DB summary query failed for PO {PO} Mill {Mill}; falling back to 0.", poNormalized, millNo);
            return 0;
        }
    }

    /// <summary>
    /// One-time backfill: recompute bundle totals for one PO/mill from Input_Slit_Row and update NDT_Bundle rows.
    /// Uses current formation-chart threshold logic and the same "include overshoot in closing bundle" rule as runtime.
    /// </summary>
    [HttpPost("backfill-bundle-totals")]
    public async Task<IActionResult> BackfillBundleTotals([FromBody] BackfillBundleTotalsRequest request, CancellationToken cancellationToken)
    {
        if (request is null || string.IsNullOrWhiteSpace(request.PoNumber))
            return BadRequest(new { Message = "PoNumber is required." });
        if (request.MillNo is < 1 or > 4)
            return BadRequest(new { Message = "MillNo must be between 1 and 4." });
        if (!UseDatabaseForSummary)
            return BadRequest(new { Message = "Backfill requires SQL mode (UseSqlServerForBundles=true and ConnectionString set)." });

        var poRequested = request.PoNumber.Trim();
        var poNormalized = InputSlitCsvParsing.NormalizePo(poRequested);
        var minUtc = SourceFileEligibility.ParseMinUtc(_options);

        var pipeSizeByPo = await _pipeSizeProvider.GetPipeSizeByPoAsync(cancellationToken).ConfigureAwait(false);
        pipeSizeByPo.TryGetValue(poNormalized, out var pipeSize);
        var formation = await _formationChartProvider.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);
        var threshold = FormationChartLookup.ResolveThreshold(formation, pipeSize);

        await using var conn = new SqlConnection(_options.ConnectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        // 1) Read deduplicated NDT pipes rows for this PO/mill from Input_Slit_Row.
        var rowPipes = new List<int>();
        const string sqlRows = @"
WITH Dedup AS
(
    SELECT
        NDT_Pipes,
        ImportedAtUtc,
        Source_File,
        Source_Row_Number,
        ROW_NUMBER() OVER (
            PARTITION BY Source_File, Source_Row_Number
            ORDER BY ImportedAtUtc DESC, Input_Slit_Row_ID DESC
        ) AS src_rn
    FROM dbo.Input_Slit_Row
    WHERE (PO_Number = @PoRequested OR PO_Number = @PoNormalized)
      AND Mill_No = @MillNo
      AND (@MinUtc IS NULL OR ImportedAtUtc >= @MinUtc)
      AND Source_File IS NOT NULL
      AND Source_Row_Number IS NOT NULL
)
SELECT NDT_Pipes
FROM Dedup
WHERE src_rn = 1
ORDER BY ImportedAtUtc, Source_File, Source_Row_Number;";
        await using (var cmdRows = new SqlCommand(sqlRows, conn))
        {
            cmdRows.Parameters.AddWithValue("@PoRequested", poRequested);
            cmdRows.Parameters.AddWithValue("@PoNormalized", poNormalized);
            cmdRows.Parameters.AddWithValue("@MillNo", request.MillNo);
            cmdRows.Parameters.AddWithValue("@MinUtc", minUtc.HasValue ? (object)minUtc.Value : DBNull.Value);
            await using var r = await cmdRows.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await r.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var v = r.IsDBNull(0) ? 0 : r.GetInt32(0);
                if (v > 0)
                    rowPipes.Add(v);
            }
        }

        // 2) Recompute bundle totals with runtime rule: close when threshold reached/exceeded, overshoot stays in that bundle.
        var computedTotals = new List<int>();
        var running = 0;
        foreach (var p in rowPipes)
        {
            running += p;
            if (running >= threshold)
            {
                computedTotals.Add(running);
                running = 0;
            }
        }
        if (running > 0)
            computedTotals.Add(running);

        // 3) Get existing bundle rows in sequence order for this PO/mill.
        var existing = new List<(long Id, string BundleNo, int CurrentTotal)>();
        const string sqlExisting = @"
SELECT
    NDTBundle_ID,
    Bundle_No,
    COALESCE(Total_NDT_Pcs, 0) AS Total_NDT_Pcs
FROM dbo.NDT_Bundle
WHERE (PO_Number = @PoRequested OR PO_Number = @PoNormalized)
  AND Mill_No = @MillNo
ORDER BY
    TRY_CONVERT(int, RIGHT(Bundle_No, 5)),
    Bundle_No;";
        await using (var cmdExisting = new SqlCommand(sqlExisting, conn))
        {
            cmdExisting.Parameters.AddWithValue("@PoRequested", poRequested);
            cmdExisting.Parameters.AddWithValue("@PoNormalized", poNormalized);
            cmdExisting.Parameters.AddWithValue("@MillNo", request.MillNo);
            await using var r = await cmdExisting.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await r.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                existing.Add((
                    r.IsDBNull(0) ? 0L : Convert.ToInt64(r.GetValue(0)),
                    r.IsDBNull(1) ? string.Empty : r.GetString(1),
                    r.IsDBNull(2) ? 0 : r.GetInt32(2)));
            }
        }

        var mapping = new List<object>();
        var max = Math.Max(existing.Count, computedTotals.Count);
        for (var i = 0; i < max; i++)
        {
            var hasDb = i < existing.Count;
            var hasCalc = i < computedTotals.Count;
            var newTotal = hasCalc ? computedTotals[i] : 0;
            mapping.Add(new
            {
                Index = i + 1,
                BundleNo = hasDb ? existing[i].BundleNo : "(no-db-row)",
                BundleId = hasDb ? existing[i].Id : 0,
                CurrentTotal = hasDb ? existing[i].CurrentTotal : 0,
                NewTotal = newTotal
            });
        }

        if (!request.DryRun)
        {
            const string sqlUpdate = @"UPDATE dbo.NDT_Bundle SET Total_NDT_Pcs = @NewTotal WHERE NDTBundle_ID = @Id;";
            await using var tx = await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await using var cmdUpd = new SqlCommand(sqlUpdate, conn, (SqlTransaction)tx);
                cmdUpd.Parameters.Add("@NewTotal", System.Data.SqlDbType.Int);
                cmdUpd.Parameters.Add("@Id", System.Data.SqlDbType.BigInt);
                for (var i = 0; i < existing.Count; i++)
                {
                    var newTotal = i < computedTotals.Count ? computedTotals[i] : 0;
                    cmdUpd.Parameters["@NewTotal"].Value = newTotal;
                    cmdUpd.Parameters["@Id"].Value = existing[i].Id;
                    await cmdUpd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                await tx.RollbackAsync(cancellationToken).ConfigureAwait(false);
                throw;
            }
        }

        return Ok(new
        {
            Message = request.DryRun ? "Dry run completed. No DB changes applied." : "Backfill applied to NDT_Bundle.",
            PoNumberRequested = poRequested,
            PoNumberNormalized = poNormalized,
            MillNo = request.MillNo,
            PipeSize = pipeSize ?? string.Empty,
            Threshold = threshold,
            InputRowsUsed = rowPipes.Count,
            ComputedBundleCount = computedTotals.Count,
            ExistingBundleRows = existing.Count,
            DryRun = request.DryRun,
            Mapping = mapping
        });
    }

    /// <summary>
    /// Lists bundle CSV files generated in the NDT Bundles folder (BundleSummaryOutputFolder).
    /// </summary>
    [HttpGet("bundles")]
    public IActionResult ListBundles()
    {
        var folder = NdtBundleOutputPaths.ResolveBundleArtifactsFolder(_options);
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return Ok(Array.Empty<object>());

        var files = Directory.GetFiles(folder, "NDT_Bundle_*.csv")
            .Select(path => new
            {
                FileName = Path.GetFileName(path),
                FullPath = path
            })
            .OrderBy(f => f.FileName)
            .ToList();

        return Ok(files);
    }

    /// <summary>
    /// Prints a dummy NDT tag to the configured printer using ZPL (Zebra Programming Language).
    /// The Honeywell PD45S and similar label printers expect ZPL on port 9100, not PDF, so this sends a ZPL label to test the connection.
    /// </summary>
    [HttpPost("print-dummy-bundle")]
    public async Task<IActionResult> PrintDummyBundle(CancellationToken cancellationToken)
    {
        if (!_zplToggle.IsEnabled)
            return BadRequest(new { Message = "NDT tag ZPL and network print are disabled (runtime toggle)." });

        var address = (_options.NdtTagPrinterAddress ?? "").Trim();
        var useAddress = !string.IsNullOrEmpty(address) && !address.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase);
        if (!useAddress)
            return BadRequest(new { Message = "Printer not configured. Set NdtBundle:NdtTagPrinterAddress (e.g. 192.168.0.125) in appsettings.json." });

        _logger.LogInformation("Printing dummy ZPL tag to {Address}:{Port} (Honeywell PD45S expects ZPL, not PDF).", address, _options.NdtTagPrinterPort);

        try
        {
            var zplBytes = ZplDummyLabelBuilder.BuildDummyLabelZpl(
                bundleNo: "DUMMY-001",
                specification: "SPEC-DUMMY",
                pipeType: "TypeA",
                pipeSize: "6",
                pipeLen: "40",
                pcsPerBundle: 10,
                slitNo: "SLIT-01");

            var sendResult = await _networkPrinterSender.SendAsync(address, _options.NdtTagPrinterPort, zplBytes, cancellationToken).ConfigureAwait(false);

            if (sendResult.Success)
                return Ok(new { Message = "Dummy ZPL tag sent to printer. The physical label should print now (Honeywell PD45S uses ZPL).", Address = address, Port = _options.NdtTagPrinterPort });
            return StatusCode(500, new
            {
                Message = "Failed to send ZPL to printer. Check printer power, IP " + address + ":" + _options.NdtTagPrinterPort + ", firewall, and NdtBundle:NdtTagPrinterLocalBindAddress (leave empty unless this PC needs a fixed egress IP).",
                Detail = sendResult.ErrorDetail ?? string.Empty,
                Address = address,
                Port = _options.NdtTagPrinterPort
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Print dummy bundle failed.");
            return StatusCode(500, new { Message = "Print failed: " + ex.Message, Error = ex.ToString() });
        }
    }
}

