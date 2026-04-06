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
    private readonly IBundleEngine _bundleEngine;
    private readonly IBundleOutputWriter _outputWriter;
    private readonly INdtBatchStateService _batchState;
    private readonly ICurrentPoPlanService? _currentPoPlanService;
    private readonly INdtBundleTagPrinter _bundleTagPrinter;
    private readonly INetworkPrinterSender _networkPrinterSender;
    private readonly IZplGenerationToggle _zplToggle;
    private readonly IFormationChartProvider _formationChartProvider;
    private readonly ILogger<TestController> _logger;
    private readonly NdtBundleOptions _options;

    public TestController(
        IBundleEngine bundleEngine,
        IBundleOutputWriter outputWriter,
        INdtBatchStateService batchState,
        IOptions<NdtBundleOptions> options,
        ILogger<TestController> logger,
        INdtBundleTagPrinter bundleTagPrinter,
        INetworkPrinterSender networkPrinterSender,
        IZplGenerationToggle zplToggle,
        IFormationChartProvider formationChartProvider,
        ICurrentPoPlanService? currentPoPlanService = null)
    {
        _bundleEngine = bundleEngine;
        _outputWriter = outputWriter;
        _batchState = batchState;
        _bundleTagPrinter = bundleTagPrinter;
        _networkPrinterSender = networkPrinterSender;
        _zplToggle = zplToggle;
        _formationChartProvider = formationChartProvider;
        _currentPoPlanService = currentPoPlanService;
        _logger = logger;
        _options = options.Value;
    }

    public sealed class PoEndRequest
    {
        public string PoNumber { get; set; } = string.Empty;
        public int MillNo { get; set; }
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
    /// Simulate PO-end for a given PO number and mill.
    /// Closes any partial bundle (e.g. 3 pipes when threshold is 10): prints a tag with that count and the next NDT Batch No.,
    /// writes NDT_Bundle_*.csv and output slit CSVs, advances batch state, then advances to the new PO from the TM folder (PoPlanFolder).
    /// Eventually the same logic will be triggered by a PLC PO-end signal.
    /// </summary>
    [HttpPost("po-end")]
    public async Task<IActionResult> SimulatePoEnd([FromBody] PoEndRequest request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.PoNumber))
            return BadRequest(new { Message = "PoNumber is required." });

        _logger.LogInformation("Simulating PO end for PO {PO} Mill {Mill}", request.PoNumber, request.MillNo);

        await _bundleEngine.HandlePoEndAsync(
            request.PoNumber,
            request.MillNo,
            async (contextRecord, batchNo, totalNdtPcs) =>
            {
                await _outputWriter.WriteBundleAsync(contextRecord, batchNo, totalNdtPcs, cancellationToken).ConfigureAwait(false);
                _logger.LogInformation(
                    "SIMULATED PRINT (CSV exported): PO {PO} Mill {Mill} Batch {Batch} NdtPcs {Pcs}",
                    request.PoNumber,
                    request.MillNo,
                    batchNo,
                    totalNdtPcs);
            },
            cancellationToken).ConfigureAwait(false);

        // Advance batch state so the next file gets the next NDT Batch No after PO End.
        await _batchState.IncrementBatchOnPoEndAsync(request.PoNumber, request.MillNo, cancellationToken).ConfigureAwait(false);

        // When PoPlanFolder is set: advance to next PO plan file so the next batch uses the next PO. (Eventually the same logic can be triggered by PLC signal.)
        if (_currentPoPlanService != null)
            await _currentPoPlanService.AdvanceToNextPoAsync(cancellationToken).ConfigureAwait(false);

        return Ok(new { Message = "PO end simulated.", request.PoNumber, request.MillNo });
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
    /// When <see cref="NdtBundleOptions.PoPlanFolder"/> is set, merges all <c>*.csv</c> files in that folder
    /// in oldest-to-newest order (by last write time). For each mill, the latest file that contains a row for that mill wins;
    /// mills not mentioned in newer files keep the value from an older file. This matches plants that drop one WIP file per PO
    /// while several mills run different POs at once.
    /// Otherwise reads the single resolved PO plan CSV (same as <c>wip-info</c>).
    /// </summary>
    [HttpGet("wip-by-mills")]
    public async Task<IActionResult> GetWipByMills(CancellationToken cancellationToken)
    {
        try
        {
            var byMill = new Dictionary<int, WipByMillRowDto>();
            string sourcePath;

            var planFolder = _options.PoPlanFolder?.Trim();
            if (!string.IsNullOrWhiteSpace(planFolder) && Directory.Exists(planFolder))
            {
                var files = Directory.EnumerateFiles(planFolder, "*.csv")
                    .Select(f => new FileInfo(f))
                    .OrderBy(f => f.LastWriteTimeUtc)
                    .ThenBy(f => f.FullName, StringComparer.OrdinalIgnoreCase)
                    .Select(f => f.FullName)
                    .ToArray();

                if (files.Length == 0)
                    return NotFound(new { Message = "PoPlanFolder contains no CSV files.", Path = planFolder });

                foreach (var file in files)
                    await MergeWipFileIntoByMillAsync(file, byMill, cancellationToken).ConfigureAwait(false);

                sourcePath = $"{planFolder} ({files.Length} WIP CSV file(s); newer file overrides per mill)";
            }
            else
            {
                var path = await ResolveWipPlanCsvPathAsync(cancellationToken).ConfigureAwait(false);
                if (string.IsNullOrWhiteSpace(path))
                    return NotFound(new { Message = "WIP CSV path not configured (PoPlanCsvPath or PoPlanFolder)." });
                if (path.StartsWith("NOTFOUND:", StringComparison.Ordinal))
                    return NotFound(new { Message = "WIP CSV file not found.", Path = path["NOTFOUND:".Length..] });

                if (!await MergeWipFileIntoByMillAsync(path, byMill, cancellationToken).ConfigureAwait(false))
                    return BadRequest(new { Message = "WIP CSV must include \"Mill Number\" or \"Mill No\", and \"PO_No\", \"PO Number\", or \"PO No\"." });

                sourcePath = path;
            }

            var mills = new List<WipByMillRowDto>(4);
            for (var m = 1; m <= 4; m++)
            {
                if (byMill.TryGetValue(m, out var row))
                    mills.Add(row);
                else
                    mills.Add(new WipByMillRowDto { MillNo = m, PoNumber = string.Empty });
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

            return Ok(new { Mills = mills, SourcePath = sourcePath });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "GetWipByMills failed.");
            return StatusCode(500, new { Message = "Failed to load WIP by mills.", Error = ex.Message });
        }
    }

    /// <summary>Merges data rows from one WIP CSV into <paramref name="byMill"/> (later calls / later rows overwrite same mill).</summary>
    /// <returns><c>false</c> if headers are missing required columns (caller may treat as hard error for single-file mode).</returns>
    private async Task<bool> MergeWipFileIntoByMillAsync(
        string filePath,
        Dictionary<int, WipByMillRowDto> byMill,
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

            byMill[millNo] = new WipByMillRowDto
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

    /// <summary>Matches <see cref="NdtBundleEngine.ProcessSlitRecordAsync"/> formation lookup (size key, then Default, then minimum 10).</summary>
    private static int ResolveNdtPcsPerBundleFromChart(
        IReadOnlyDictionary<string, FormationChartEntry> formation,
        string pipeSize)
    {
        pipeSize ??= string.Empty;
        formation.TryGetValue(pipeSize, out var formationEntry);
        formationEntry ??= formation.TryGetValue("Default", out var defaultEntry) ? defaultEntry : null;
        var sizeThreshold = formationEntry?.RequiredNdtPcs ?? 0;
        if (sizeThreshold <= 0)
            sizeThreshold = 10;
        return sizeThreshold;
    }

    /// <summary>
    /// Returns total NDT pipes for a PO, optionally filtered to one mill.
    /// Counts come from input slit CSVs in <see cref="NdtBundleOptions.InputSlitFolder"/> (e.g. Mill-1 NDT Files from SAP):
    /// each row's <c>Mill No</c> (1–4) determines which mill the row's <c>NDT Pipes</c> count applies to.
    /// </summary>
    [HttpGet("ndt-summary")]
    public async Task<IActionResult> GetNdtSummary([FromQuery] string poNumber, [FromQuery] int? millNo, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(poNumber))
            return BadRequest(new { Message = "poNumber is required." });

        var folder = _options.InputSlitFolder;
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return Ok(new NdtSummaryDto { PoNumber = poNumber, MillNo = millNo ?? 0, TotalNdtPipes = 0 });

        var files = Directory.EnumerateFiles(folder, "*.csv");
        int total = 0;
        var poRequested = poNumber.Trim();

        foreach (var file in files)
        {
            await using var stream = System.IO.File.OpenRead(file);
            using var reader = new StreamReader(stream);

            var headerLine = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            if (headerLine is null)
                continue;

            headerLine = InputSlitCsvParsing.StripBom(headerLine);
            var headers = InputSlitCsvParsing.SplitCsvFields(headerLine);
            var poIndex = InputSlitCsvParsing.HeaderIndex(headers, "PO Number", "PO_No", "PO No");
            var ndtIndex = InputSlitCsvParsing.HeaderIndex(headers, "NDT Pipes");
            var millIndex = InputSlitCsvParsing.HeaderIndex(headers, "Mill No", "Mill Number");
            if (poIndex < 0 || ndtIndex < 0 || millIndex < 0)
                continue;

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
                    total += ndtVal;
            }
        }

        return Ok(new NdtSummaryDto
        {
            PoNumber = InputSlitCsvParsing.NormalizePo(poRequested),
            MillNo = millNo ?? 0,
            TotalNdtPipes = total
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

            var sent = await _networkPrinterSender.SendAsync(address, _options.NdtTagPrinterPort, zplBytes, cancellationToken).ConfigureAwait(false);

            if (sent)
                return Ok(new { Message = "Dummy ZPL tag sent to printer. The physical label should print now (Honeywell PD45S uses ZPL).", Address = address, Port = _options.NdtTagPrinterPort });
            return StatusCode(500, new { Message = "Failed to send ZPL to printer. Check that the printer is on, reachable, and port " + _options.NdtTagPrinterPort + " is open." });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Print dummy bundle failed.");
            return StatusCode(500, new { Message = "Print failed: " + ex.Message, Error = ex.ToString() });
        }
    }
}

