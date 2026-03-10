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
        ICurrentPoPlanService? currentPoPlanService = null)
    {
        _bundleEngine = bundleEngine;
        _outputWriter = outputWriter;
        _batchState = batchState;
        _bundleTagPrinter = bundleTagPrinter;
        _networkPrinterSender = networkPrinterSender;
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
            string? path;
            if (!string.IsNullOrWhiteSpace(_options.PoPlanFolder) && _currentPoPlanService != null)
                path = await _currentPoPlanService.GetCurrentPoPlanPathAsync(cancellationToken).ConfigureAwait(false);
            else
                path = _options.PoPlanCsvPath;

            if (string.IsNullOrWhiteSpace(path))
                return NotFound(new { Message = "WIP CSV path not configured (PoPlanCsvPath or PoPlanFolder)." });

            if (!System.IO.File.Exists(path))
            {
                // Fallback for development: look for WIP_*.csv in app dir or parent dirs (e.g. when configured path is on D: and not available)
                var dir = new DirectoryInfo(AppContext.BaseDirectory);
                string? fallback = null;
                for (var i = 0; i < 6 && dir != null; i++, dir = dir.Parent)
                {
                    fallback = Directory.EnumerateFiles(dir.FullName, "WIP_*.csv").FirstOrDefault();
                    if (fallback != null) break;
                }
                if (fallback != null)
                {
                    path = fallback;
                    _logger.LogInformation("Using fallback WIP CSV: {Path}", path);
                }
                else
                    return NotFound(new { Message = "WIP CSV file not found.", Path = path });
            }

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
    /// Returns a simple summary of total NDT pipes produced so far for a given PO (and optional mill).
    /// For the demo we compute this by scanning the generated output CSV files in OutputBundleFolder,
    /// because each input slit file results in one output CSV with the same columns plus NDT Batch No.
    /// </summary>
    [HttpGet("ndt-summary")]
    public async Task<IActionResult> GetNdtSummary([FromQuery] string poNumber, [FromQuery] int? millNo, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(poNumber))
            return BadRequest(new { Message = "poNumber is required." });

        var folder = _options.OutputBundleFolder;
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return Ok(new NdtSummaryDto { PoNumber = poNumber, MillNo = millNo ?? 0, TotalNdtPipes = 0 });

        var files = Directory.EnumerateFiles(folder, "*.csv");
        int total = 0;

        foreach (var file in files)
        {
            await using var stream = System.IO.File.OpenRead(file);
            using var reader = new StreamReader(stream);

            var headerLine = await reader.ReadLineAsync();
            if (headerLine is null)
                continue;

            var headers = headerLine.Split(',');

            int IndexOf(string name) =>
                Array.FindIndex(headers, h => h.Trim().Equals(name, StringComparison.OrdinalIgnoreCase));

            int poIndex = IndexOf("PO Number");
            int ndtIndex = IndexOf("NDT Pipes");
            int millIndex = IndexOf("Mill No");

            while (!reader.EndOfStream)
            {
                var line = await reader.ReadLineAsync();
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                var cols = line.Split(',');
                if (cols.Length == 0)
                    continue;

                string Get(string[] c, int i)
                {
                    if (i < 0 || i >= c.Length) return string.Empty;
                    return c[i].Trim();
                }

                var po = Get(cols, poIndex);
                if (!po.Equals(poNumber.Trim(), StringComparison.OrdinalIgnoreCase))
                    continue;

                if (millNo.HasValue)
                {
                    var millRaw = Get(cols, millIndex);
                    if (!int.TryParse(millRaw, out var parsedMill) || parsedMill != millNo.Value)
                        continue;
                }

                var ndtRaw = Get(cols, ndtIndex);
                if (int.TryParse(ndtRaw, out var ndtVal))
                    total += ndtVal;
            }
        }

        return Ok(new NdtSummaryDto
        {
            PoNumber = poNumber.Trim(),
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

