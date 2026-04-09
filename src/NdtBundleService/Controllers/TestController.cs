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
    /// <b>Current PO per mill</b> comes from the latest slit CSV rows in <see cref="NdtBundleOptions.InputSlitFolder"/> and, when set, <see cref="NdtBundleOptions.InputSlitAcceptedFolder"/> (read-only; real-time).
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

                sourcePath = $"{planFolder} ({files.Length} WIP CSV file(s); PO per mill from Input Slit + Input Slit Accepted folders)";
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

            var slitPoByMill = await GetLatestPoPerMillFromSlitFolderAsync(cancellationToken).ConfigureAwait(false);

            var mills = new List<WipByMillRowDto>(4);
            for (var m = 1; m <= 4; m++)
            {
                wipByMill.TryGetValue(m, out var wipRowForMill);
                slitPoByMill.TryGetValue(m, out var slitPo);

                if (!string.IsNullOrWhiteSpace(slitPo))
                {
                    var row = new WipByMillRowDto { MillNo = m, PoNumber = slitPo };
                    if (wipByPo.TryGetValue(slitPo, out var byPo))
                        CopyWipDetails(row, byPo);
                    else if (wipRowForMill != null && InputSlitCsvParsing.PoEquals(wipRowForMill.PoNumber, slitPo))
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

            var slitLocations = string.Join(" | ", GetInputSlitReadFolderPaths().Where(static p => !string.IsNullOrWhiteSpace(p)));
            var detail = $"WIP: {sourcePath}; current PO per mill from slit CSVs in: {slitLocations}";
            return Ok(new { Mills = mills, SourcePath = detail });
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

    private static void CopyWipDetails(WipByMillRowDto target, WipByMillRowDto source)
    {
        target.PlannedMonth = source.PlannedMonth;
        target.PipeGrade = source.PipeGrade;
        target.PipeSize = source.PipeSize;
        target.PipeLength = source.PipeLength;
        target.PiecesPerBundle = source.PiecesPerBundle;
        target.TotalPieces = source.TotalPieces;
    }

    /// <summary>Latest PO per mill from slit inbox + optional accepted folder: files processed oldest→newest, rows top→bottom; last row wins per mill.</summary>
    private async Task<Dictionary<int, string>> GetLatestPoPerMillFromSlitFolderAsync(CancellationToken cancellationToken)
    {
        // Fast path: read newest slit files first and stop when all 4 mills are found.
        var fromLatestFiles = await GetLatestPoPerMillFromLatestFilesAsync(cancellationToken).ConfigureAwait(false);
        if (fromLatestFiles.Count == 4)
            return fromLatestFiles;

        if (UseDatabaseForSummary)
        {
            var fromDb = await GetLatestPoPerMillFromDatabaseAsync(cancellationToken).ConfigureAwait(false);
            if (fromDb.Count > 0)
                return fromDb;
        }

        var result = new Dictionary<int, string>();
        var files = GetEligibleInputSlitCsvFilesOrdered();
        foreach (var fullPath in files)
        {
            await using var stream = System.IO.File.OpenRead(fullPath);
            using var reader = new StreamReader(stream);

            var headerLine = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            if (headerLine is null)
                continue;

            headerLine = InputSlitCsvParsing.StripBom(headerLine);
            var headers = InputSlitCsvParsing.SplitCsvFields(headerLine);
            var poIndex = InputSlitCsvParsing.HeaderIndex(headers, "PO Number", "PO_No", "PO No");
            var millIndex = InputSlitCsvParsing.HeaderIndex(headers, "Mill No", "Mill Number");
            if (poIndex < 0 || millIndex < 0)
                continue;

            while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;
                var cols = InputSlitCsvParsing.SplitCsvFields(line);
                if (cols.Length == 0)
                    continue;
                string Get(int i) => i >= 0 && i < cols.Length ? cols[i].Trim() : string.Empty;
                var millRaw = Get(millIndex);
                if (!InputSlitCsvParsing.TryParseMillNo(millRaw, out var millNo))
                    continue;
                var po = Get(poIndex);
                if (string.IsNullOrWhiteSpace(po))
                    continue;
                result[millNo] = InputSlitCsvParsing.NormalizePo(po);
            }
        }

        return result;
    }

    /// <summary>
    /// Reads newest eligible slit files first (descending LastWriteTimeUtc) and returns first PO seen per mill.
    /// This reflects current running PO better than DB-import order when old files are reprocessed.
    /// </summary>
    private async Task<Dictionary<int, string>> GetLatestPoPerMillFromLatestFilesAsync(CancellationToken cancellationToken)
    {
        var result = new Dictionary<int, string>();
        var minUtc = SourceFileEligibility.ParseMinUtc(_options);
        const int maxFilesToScan = 300;

        var files = new List<FileInfo>();
        foreach (var folder in GetInputSlitReadFolderPaths())
        {
            if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
                continue;
            foreach (var file in Directory.EnumerateFiles(folder, "*.csv"))
            {
                var fi = new FileInfo(file);
                if (SourceFileEligibility.IncludeFileUtc(fi.LastWriteTimeUtc, minUtc))
                    files.Add(fi);
            }
        }

        foreach (var fi in files
                     .OrderByDescending(f => f.LastWriteTimeUtc)
                     .ThenByDescending(f => f.FullName, StringComparer.OrdinalIgnoreCase)
                     .Take(maxFilesToScan))
        {
            if (result.Count == 4)
                break;
            cancellationToken.ThrowIfCancellationRequested();

            string[] lines;
            try
            {
                lines = await System.IO.File.ReadAllLinesAsync(fi.FullName, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                continue;
            }

            if (lines.Length < 2)
                continue;

            var headerLine = InputSlitCsvParsing.StripBom(lines[0]);
            var headers = InputSlitCsvParsing.SplitCsvFields(headerLine);
            var poIndex = InputSlitCsvParsing.HeaderIndex(headers, "PO Number", "PO_No", "PO No");
            var millIndex = InputSlitCsvParsing.HeaderIndex(headers, "Mill No", "Mill Number");
            if (poIndex < 0 || millIndex < 0)
                continue;

            // Bottom-to-top in each file: "last row wins" for that file.
            for (var i = lines.Length - 1; i >= 1; i--)
            {
                if (result.Count == 4)
                    break;
                var line = lines[i];
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                var cols = InputSlitCsvParsing.SplitCsvFields(line);
                if (cols.Length == 0)
                    continue;

                string Get(int idx) => idx >= 0 && idx < cols.Length ? cols[idx].Trim() : string.Empty;
                var millRaw = Get(millIndex);
                if (!InputSlitCsvParsing.TryParseMillNo(millRaw, out var millNo))
                    continue;
                if (millNo < 1 || millNo > 4 || result.ContainsKey(millNo))
                    continue;

                var po = Get(poIndex);
                if (string.IsNullOrWhiteSpace(po))
                    continue;

                result[millNo] = InputSlitCsvParsing.NormalizePo(po);
            }
        }

        return result;
    }

    private async Task<Dictionary<int, string>> GetLatestPoPerMillFromDatabaseAsync(CancellationToken cancellationToken)
    {
        var result = new Dictionary<int, string>();
        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
            return result;

        try
        {
            var minUtc = SourceFileEligibility.ParseMinUtc(_options);
            await using var conn = new SqlConnection(_options.ConnectionString);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

            const string sql = @"
WITH Dedup AS
(
    SELECT
        Mill_No,
        PO_Number,
        ImportedAtUtc,
        Input_Slit_Row_ID,
        ROW_NUMBER() OVER (
            PARTITION BY Source_File, Source_Row_Number
            ORDER BY ImportedAtUtc DESC, Input_Slit_Row_ID DESC
        ) AS src_rn
    FROM dbo.Input_Slit_Row
    WHERE Mill_No BETWEEN 1 AND 4
      AND PO_Number IS NOT NULL
      AND LTRIM(RTRIM(PO_Number)) <> ''
      AND Source_File IS NOT NULL
      AND Source_Row_Number IS NOT NULL
      AND (@MinUtc IS NULL OR ImportedAtUtc >= @MinUtc)
),
LatestByMill AS
(
    SELECT
        Mill_No,
        PO_Number,
        ROW_NUMBER() OVER (PARTITION BY Mill_No ORDER BY ImportedAtUtc DESC, Input_Slit_Row_ID DESC) AS rn
    FROM Dedup
    WHERE src_rn = 1
)
SELECT Mill_No, PO_Number
FROM LatestByMill
WHERE rn = 1;";

            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@MinUtc", minUtc.HasValue ? (object)minUtc.Value : DBNull.Value);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var millNo = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
                var po = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
                if (millNo >= 1 && millNo <= 4 && !string.IsNullOrWhiteSpace(po))
                    result[millNo] = InputSlitCsvParsing.NormalizePo(po);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to read latest PO per mill from Input_Slit_Row; falling back to CSV scan.");
        }

        return result;
    }

    /// <summary>Inbox then accepted TM paths used for read-only slit CSV aggregation (same order as options).</summary>
    private IEnumerable<string> GetInputSlitReadFolderPaths()
    {
        var inbox = _options.InputSlitFolder?.Trim();
        if (!string.IsNullOrEmpty(inbox))
            yield return inbox;
        var accepted = _options.InputSlitAcceptedFolder?.Trim();
        if (!string.IsNullOrEmpty(accepted))
            yield return accepted;
    }

    /// <summary>All eligible <c>*.csv</c> files under inbox + accepted folders, ordered for stable &quot;last row wins&quot; semantics.</summary>
    private List<string> GetEligibleInputSlitCsvFilesOrdered()
    {
        var minUtc = SourceFileEligibility.ParseMinUtc(_options);
        var acc = new List<string>();
        foreach (var folder in GetInputSlitReadFolderPaths())
        {
            if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
                continue;
            foreach (var file in Directory.EnumerateFiles(folder, "*.csv"))
            {
                if (SourceFileEligibility.IncludeFileUtc(System.IO.File.GetLastWriteTimeUtc(file), minUtc))
                    acc.Add(file);
            }
        }

        return acc
            .Select(f => new FileInfo(f))
            .OrderBy(f => f.LastWriteTimeUtc)
            .ThenBy(f => f.FullName, StringComparer.OrdinalIgnoreCase)
            .Select(f => f.FullName)
            .ToList();
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

        var readFolders = GetInputSlitReadFolderPaths()
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
        var latestPoByMill = await GetLatestPoPerMillFromSlitFolderAsync(cancellationToken).ConfigureAwait(false);
        var minUtc = SourceFileEligibility.ParseMinUtc(_options);
        var readFolders = GetInputSlitReadFolderPaths()
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

