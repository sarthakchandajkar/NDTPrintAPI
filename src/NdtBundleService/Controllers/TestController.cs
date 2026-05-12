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
    private readonly IActivePoPerMillService _activePoPerMill;
    private readonly IWipBundleRunningPoProvider _wipBundleRunningPo;
    private readonly IMillNdtCountReader _millNdtCountReader;
    private readonly IPoEndWorkflowService _poEndWorkflow;
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
        IActivePoPerMillService activePoPerMill,
        IWipBundleRunningPoProvider wipBundleRunningPo,
        IMillNdtCountReader millNdtCountReader,
        IPoEndWorkflowService poEndWorkflow,
        ICurrentPoPlanService? currentPoPlanService = null)
    {
        _bundleTagPrinter = bundleTagPrinter;
        _networkPrinterSender = networkPrinterSender;
        _zplToggle = zplToggle;
        _formationChartProvider = formationChartProvider;
        _pipeSizeProvider = pipeSizeProvider;
        _activePoPerMill = activePoPerMill;
        _wipBundleRunningPo = wipBundleRunningPo;
        _millNdtCountReader = millNdtCountReader;
        _poEndWorkflow = poEndWorkflow;
        _currentPoPlanService = currentPoPlanService;
        _logger = logger;
        _options = options.Value;
    }

    public sealed class PoEndRequest
    {
        public string PoNumber { get; set; } = string.Empty;
        public int MillNo { get; set; }
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
    /// and optional PO plan file advance when <c>NdtBundle:PlcPoEnd:AdvancePoPlanFileOnPoEnd</c> is true (normally false while testing without PLC).
    /// </summary>
    [HttpPost("po-end")]
    public async Task<IActionResult> SimulatePoEnd([FromBody] PoEndRequest request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.PoNumber))
            return BadRequest(new { Message = "PoNumber is required." });

        if (request.MillNo is < 1 or > 4)
            return BadRequest(new { Message = "MillNo must be between 1 and 4." });

        var po = InputSlitCsvParsing.NormalizePo(request.PoNumber.Trim());
        var advancePlan = _options.PlcPoEnd?.AdvancePoPlanFileOnPoEnd == true && _currentPoPlanService != null;

        _logger.LogInformation("Simulating PO end for PO {PO} Mill {Mill} (advance PO plan file: {Advance})", po, request.MillNo, advancePlan);

        try
        {
            await _poEndWorkflow.ExecuteAsync(po, request.MillNo, advancePlan, cancellationToken).ConfigureAwait(false);
        }
        catch (ArgumentOutOfRangeException ex)
        {
            return BadRequest(new { Message = ex.Message });
        }

        return Ok(new
        {
            Message = "PO end simulated.",
            PoNumber = po,
            MillNo = request.MillNo,
            AdvancedPoPlanFile = advancePlan
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
                return NotFound(new { Message = "WIP CSV path not configured (PoPlanCsvPath or PoPlanFolder)." });
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
    /// <b>Current PO per mill</b> comes first from the latest slit CSV rows in <see cref="NdtBundleOptions.InputSlitFolder"/> and, when set, <see cref="NdtBundleOptions.InputSlitAcceptedFolder"/> (read-only; real-time).
    /// When a mill has no slit row yet, the PO is taken from the latest <c>WIP_MM_…</c> filename in <see cref="MillSlitLiveOptions.WipBundleFolder"/> / <see cref="MillSlitLiveOptions.WipBundleAcceptedFolder"/> (same as <see cref="IWipBundleRunningPoProvider"/>).
    /// Pipe size, planned month, and SAP pieces/bundle are enriched from merged WIP files in <see cref="NdtBundleOptions.PoPlanFolder"/> when the PO matches.
    /// When <see cref="NdtBundleOptions.PoPlanFolder"/> is set, WIP CSVs are merged (newer files override per mill) for enrichment only.
    /// </summary>
    [HttpGet("wip-by-mills")]
    public async Task<IActionResult> GetWipByMills(CancellationToken cancellationToken)
    {
        try
        {
            var wipByMill = new Dictionary<int, WipByMillRowDto>();
            var wipByPo = new Dictionary<string, WipByMillRowDto>(StringComparer.OrdinalIgnoreCase);
            string sourcePath;

            var minUtc = SourceFileEligibility.ParseMinUtc(_options);
            var planFolder = _options.PoPlanFolder?.Trim();
            if (!string.IsNullOrWhiteSpace(planFolder) && Directory.Exists(planFolder))
            {
                var files = Directory.EnumerateFiles(planFolder, "*.csv")
                    .Select(f => new FileInfo(f))
                    .Where(f => SourceFileEligibility.IncludeFileUtc(f.LastWriteTimeUtc, minUtc))
                    .OrderBy(f => f.LastWriteTimeUtc)
                    .ThenBy(f => f.FullName, StringComparer.OrdinalIgnoreCase)
                    .Select(f => f.FullName)
                    .ToArray();

                if (files.Length == 0)
                    return NotFound(new { Message = "PoPlanFolder has no eligible CSV files (check MinSourceFileLastWriteUtc or folder path).", Path = planFolder });

                foreach (var file in files)
                    await MergeWipFileIntoByMillAsync(file, wipByMill, wipByPo, cancellationToken).ConfigureAwait(false);

                sourcePath = $"{planFolder} ({files.Length} WIP CSV file(s); PO per mill from slits, else TM Bundle / Bundle Accepted WIP filenames)";
            }
            else
            {
                var path = await ResolveWipPlanCsvPathAsync(cancellationToken).ConfigureAwait(false);
                if (string.IsNullOrWhiteSpace(path))
                    return NotFound(new { Message = "WIP CSV path not configured (PoPlanCsvPath or PoPlanFolder)." });
                if (path.StartsWith("NOTFOUND:", StringComparison.Ordinal))
                    return NotFound(new { Message = "WIP CSV file not found.", Path = path["NOTFOUND:".Length..] });

                if (!SourceFileEligibility.IncludeFileUtc(System.IO.File.GetLastWriteTimeUtc(path), minUtc))
                    return NotFound(new { Message = "WIP CSV file is older than MinSourceFileLastWriteUtc.", Path = path });

                if (!await MergeWipFileIntoByMillAsync(path, wipByMill, wipByPo, cancellationToken).ConfigureAwait(false))
                    return BadRequest(new { Message = "WIP CSV must include \"Mill Number\" or \"Mill No\", and \"PO_No\", \"PO Number\", or \"PO No\"." });

                sourcePath = path;
            }

            var slitPoByMill = await _activePoPerMill.GetLatestPoByMillAsync(cancellationToken).ConfigureAwait(false);

            var mills = new List<WipByMillRowDto>(4);
            for (var m = 1; m <= 4; m++)
            {
                wipByMill.TryGetValue(m, out var wipRowForMill);
                slitPoByMill.TryGetValue(m, out var slitPo);

                var bundleFilePo = string.IsNullOrWhiteSpace(slitPo)
                    ? await _wipBundleRunningPo.TryGetRunningPoForMillAsync(m, cancellationToken).ConfigureAwait(false)
                    : null;
                var resolvedPo = !string.IsNullOrWhiteSpace(slitPo)
                    ? slitPo
                    : bundleFilePo;

                if (!string.IsNullOrWhiteSpace(resolvedPo))
                {
                    var row = new WipByMillRowDto { MillNo = m, PoNumber = resolvedPo };
                    if (wipByPo.TryGetValue(resolvedPo, out var byPo))
                        CopyWipDetails(row, byPo);
                    else if (wipRowForMill != null && InputSlitCsvParsing.PoEquals(wipRowForMill.PoNumber, resolvedPo))
                        CopyWipDetails(row, wipRowForMill);

                    mills.Add(row);
                }
                else if (wipRowForMill != null && !string.IsNullOrWhiteSpace(wipRowForMill.PoNumber))
                {
                    mills.Add(wipRowForMill);
                }
                else
                {
                    mills.Add(new WipByMillRowDto { MillNo = m, PoNumber = string.Empty });
                }
            }

            var liveOpts = _options.MillSlitLive;
            foreach (var row in mills)
            {
                if (string.IsNullOrWhiteSpace(row.PoNumber) || !string.IsNullOrWhiteSpace(row.PipeSize))
                    continue;
                await WipBundleWipCsvEnricher.TryEnrichRowAsync(row, liveOpts, _logger, cancellationToken).ConfigureAwait(false);
            }

            var pipeSizeByPo = await _pipeSizeProvider.GetPipeSizeByPoAsync(cancellationToken).ConfigureAwait(false);
            foreach (var row in mills)
            {
                if (string.IsNullOrWhiteSpace(row.PoNumber) || !string.IsNullOrWhiteSpace(row.PipeSize))
                    continue;
                if (pipeSizeByPo.TryGetValue(row.PoNumber, out var ps) && !string.IsNullOrWhiteSpace(ps))
                    row.PipeSize = ps.Trim();
            }

            var formation = await _formationChartProvider.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);
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
                liveNdtCount = await _millNdtCountReader.TryReadNdtPipesCountAsync(cancellationToken).ConfigureAwait(false);

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
        catch (OperationCanceledException)
        {
            _logger.LogWarning("GetWipByMills canceled by client/request timeout.");
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

    private static void CopyWipDetails(WipByMillRowDto target, WipByMillRowDto source)
    {
        target.PlannedMonth = source.PlannedMonth;
        target.PipeGrade = source.PipeGrade;
        target.PipeSize = source.PipeSize;
        target.PipeLength = source.PipeLength;
        target.PiecesPerBundle = source.PiecesPerBundle;
        target.TotalPieces = source.TotalPieces;
    }

    /// <summary>Merges data rows from one WIP CSV into <paramref name="byMill"/> (later calls / later rows overwrite same mill).</summary>
    /// <returns><c>false</c> if headers are missing required columns (caller may treat as hard error for single-file mode).</returns>
    private async Task<bool> MergeWipFileIntoByMillAsync(
        string filePath,
        Dictionary<int, WipByMillRowDto> byMill,
        Dictionary<string, WipByMillRowDto> wipByPo,
        CancellationToken cancellationToken)
    {
        await using var stream = System.IO.File.OpenRead(filePath);
        using var reader = new StreamReader(stream);

        var headerLine = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
        if (headerLine is null)
        {
            _logger.LogWarning("WIP file has no header line: {File}", filePath);
            return false;
        }

        var headers = headerLine.Split(',');

        static int HeaderIndex(IReadOnlyList<string> hdrs, params string[] names)
        {
            foreach (var name in names)
            {
                for (var i = 0; i < hdrs.Count; i++)
                {
                    if (string.Equals(hdrs[i].Trim(), name, StringComparison.OrdinalIgnoreCase))
                        return i;
                }
            }

            return -1;
        }

        var poIdx = HeaderIndex(headers, "PO_No", "PO Number", "PO No");
        var millIdx = HeaderIndex(headers, "Mill Number", "Mill No");
        if (millIdx < 0 || poIdx < 0)
        {
            _logger.LogWarning(
                "Skipping WIP file (missing PO or mill column): {File}. Expected Mill Number/Mill No and PO_No/PO Number/PO No.",
                filePath);
            return false;
        }

        int Idx(string columnName) => HeaderIndex(headers, columnName);

        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;
            var cols = line.Split(',');
            if (cols.Length <= millIdx)
                continue;
            var millRaw = cols[millIdx].Trim();
            if (!InputSlitCsvParsing.TryParseMillNo(millRaw, out var millNo))
                continue;

            string Cell(int idx) => idx >= 0 && idx < cols.Length ? cols[idx].Trim() : string.Empty;

            var po = Cell(poIdx);
            if (string.IsNullOrWhiteSpace(po))
                continue;

            var dto = new WipByMillRowDto
            {
                MillNo = millNo,
                PoNumber = po,
                PlannedMonth = Cell(Idx("Planned Month")),
                PipeGrade = Cell(Idx("Pipe Grade")),
                PipeSize = Cell(Idx("Pipe Size")),
                PipeLength = Cell(Idx("Pipe Length")),
                PiecesPerBundle = Cell(Idx("Pieces Per Bundle")),
                TotalPieces = Cell(Idx("Total Pieces")),
            };
            byMill[millNo] = dto;
            wipByPo[InputSlitCsvParsing.NormalizePo(po)] = dto;
        }

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

            foreach (var file in Directory.EnumerateFiles(folder, "*.csv"))
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

                    foreach (var file in Directory.EnumerateFiles(folder, "*.csv"))
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
    /// Lists bundle CSV files generated in the configured OutputBundleFolder.
    /// </summary>
    [HttpGet("bundles")]
    public IActionResult ListBundles()
    {
        var folder = _options.OutputBundleFolder;
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return Ok(Array.Empty<object>());

        var files = Directory.GetFiles(folder, "*.csv")
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

