using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;

namespace NdtBundleService.Controllers;

/// <summary>
/// List SAP Input Slit inbox files (read-only reference) and create NDT Input Slit
/// <b>output</b> CSVs under <see cref="NdtBundleOptions.OutputBundleFolder"/> (with NDT Batch No)
/// plus matching <c>Output_Slit_Row</c> / SAP-status Pending rows.
/// </summary>
[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
[InstanceRole(InstanceRoleModes.Monolith, InstanceRoleModes.Shared, InstanceRoleModes.Mill)]
public sealed class InputSlitsController : ControllerBase
{
    /// <summary>NDT Input Slit output header (inbox columns + NDT Batch No).</summary>
    public const string ManualNdtOutputCsvHeader =
        "PO Number,Slit No,NDT Pipes,Rejected P,Slit Start Time,Slit Finish Time,Mill No,NDT Short Length Pipe,Rejected Short Length Pipe,NDT Batch No";

    /// <summary>Mandatory CSV wall-clock format for Slit Start / Finish Time.</summary>
    public const string SapSlitDateTimeFormat = "dd.MM.yyyy HH:mm:ss";

    private static readonly Regex SapSlitDateTimePattern = new(
        @"^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}$",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private readonly NdtBundleOptions _options;
    private readonly ITraceabilityRepository _traceability;
    private readonly IOutputSlitSapStatusRepository _sapStatus;
    private readonly ILogger<InputSlitsController> _logger;

    public InputSlitsController(
        IOptions<NdtBundleOptions> options,
        ITraceabilityRepository traceability,
        IOutputSlitSapStatusRepository sapStatus,
        ILogger<InputSlitsController> logger)
    {
        _options = options.Value;
        _traceability = traceability;
        _sapStatus = sapStatus;
        _logger = logger;
    }

    /// <summary>
    /// List slit inbox files in the SAP Input Slit folder (name, lastModified)—read-only reference.
    /// Manual create writes to <see cref="NdtBundleOptions.OutputBundleFolder"/>, not this folder.
    /// </summary>
    [HttpGet("files")]
    public IActionResult ListFiles()
    {
        var folder = _options.InputSlitFolder;
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return Ok(Array.Empty<object>());

        var minUtc = SourceFileEligibility.ParseMinUtc(_options);
        var files = InputSlitInboxEnumeration.EnumerateFiles(folder)
            .Select(path =>
            {
                var fi = new FileInfo(path);
                return new
                {
                    FileName = fi.Name,
                    LastModified = fi.LastWriteTimeUtc,
                    Size = fi.Length
                };
            })
            .Where(f => SourceFileEligibility.IncludeFileUtc(f.LastModified, minUtc))
            .OrderByDescending(f => f.LastModified)
            .ToList();

        return Ok(files);
    }

    /// <summary>
    /// Get parsed content of an input slit file (extensionless or <c>.csv</c>). FileName must be the base name (no path).
    /// </summary>
    [HttpGet("files/{fileName}/content")]
    public async Task<IActionResult> GetFileContent(string fileName, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(fileName) || fileName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
            return BadRequest(new { Message = "Invalid file name." });

        var folder = _options.InputSlitFolder;
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return NotFound(new { Message = "Input slit folder not configured or missing." });

        var path = Path.Combine(folder, Path.GetFileName(fileName));
        if (!System.IO.File.Exists(path))
            return NotFound(new { Message = "File not found." });

        try
        {
            var lines = await System.IO.File.ReadAllLinesAsync(path, cancellationToken).ConfigureAwait(false);
            if (lines.Length == 0)
                return Ok(new { Header = "", Rows = Array.Empty<string[]>() });

            var header = lines[0];
            var headers = header.Split(',');
            var rows = new List<string[]>();
            for (var i = 1; i < lines.Length; i++)
            {
                if (string.IsNullOrWhiteSpace(lines[i]))
                    continue;
                rows.Add(lines[i].Split(','));
            }

            return Ok(new { Header = header, Headers = headers, Rows = rows });
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to read input slit file {File}.", fileName);
            return StatusCode(500, new { Message = "Failed to read file." });
        }
    }

    /// <summary>
    /// Create an NDT Input Slit <b>output</b> CSV in <see cref="NdtBundleOptions.OutputBundleFolder"/>
    /// named <c>{SlitNo}_{yyMMdd}_{PO}</c>, with mandatory SAP times (<c>dd.MM.yyyy HH:mm:ss</c>),
    /// and record <c>Output_Slit_Row</c> (+ SAP Pending when enabled). Does not write to the SAP inbox.
    /// </summary>
    [HttpPost("manual")]
    public async Task<IActionResult> CreateManualFile(
        [FromBody] ManualInputSlitRequest request,
        CancellationToken cancellationToken)
    {
        if (request is null)
            return BadRequest(new { Message = "Request body is required." });

        var po = InputSlitCsvParsing.NormalizePo(request.PoNumber ?? string.Empty);
        if (string.IsNullOrWhiteSpace(po))
            return BadRequest(new { Message = "PO Number is required." });

        if (request.MillNo is < 1 or > 4)
            return BadRequest(new { Message = "Mill No must be 1–4." });

        if (request.NdtPipes < 0)
            return BadRequest(new { Message = "NDT Pipes must be >= 0." });

        if (request.RejectedPipes < 0)
            return BadRequest(new { Message = "Rejected P must be >= 0." });

        var slitNo = (request.SlitNo ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(slitNo))
            return BadRequest(new { Message = "Slit No is required (used in file name SlitNumber_YYMMDD_PONumber)." });

        if (slitNo.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
            return BadRequest(new { Message = "Slit No contains invalid file name characters." });

        var batchNo = (request.NdtBatchNo ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(batchNo))
            return BadRequest(new { Message = "NDT Batch No is required." });

        if (!TryParseSapSlitDateTime(request.SlitStartTime, out var startParsed, out var startText))
            return BadRequest(new
            {
                Message = $"Slit Start Time is required and must be exactly {SapSlitDateTimeFormat} (e.g. 27.09.2026 14:53:31)."
            });

        if (!TryParseSapSlitDateTime(request.SlitFinishTime, out var finishParsed, out var finishText))
            return BadRequest(new
            {
                Message = $"Slit Finish Time is required and must be exactly {SapSlitDateTimeFormat} (e.g. 27.09.2026 14:53:31)."
            });

        var folder = (_options.OutputBundleFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(folder))
            return BadRequest(new { Message = "OutputBundleFolder (NDT Input Slit output) is not configured." });

        try
        {
            Directory.CreateDirectory(folder);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Cannot create NDT Input Slit output folder {Folder}.", folder);
            return StatusCode(500, new { Message = "NDT Input Slit output folder is not writable." });
        }

        var fileName = BuildManualOutputFileName(slitNo, startParsed, po);
        if (fileName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
            return BadRequest(new { Message = "Invalid file name." });

        var path = Path.Combine(folder, fileName);
        if (System.IO.File.Exists(path))
            return Conflict(new { Message = $"File already exists: {fileName}" });

        var row = string.Join(",",
            CsvEscape(po),
            CsvEscape(slitNo),
            request.NdtPipes.ToString(CultureInfo.InvariantCulture),
            request.RejectedPipes.ToString(CultureInfo.InvariantCulture),
            CsvEscape(startText),
            CsvEscape(finishText),
            request.MillNo.ToString(CultureInfo.InvariantCulture),
            CsvEscape(request.NdtShortLengthPipe ?? string.Empty),
            CsvEscape(request.RejectedShortLengthPipe ?? string.Empty),
            CsvEscape(batchNo));

        try
        {
            await System.IO.File.WriteAllLinesAsync(
                    path,
                    new[] { ManualNdtOutputCsvHeader, row },
                    Encoding.UTF8,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to write NDT Input Slit output file {Path}.", path);
            return StatusCode(500, new { Message = "Failed to write NDT output file." });
        }

        var record = new InputSlitRecord
        {
            PoNumber = po,
            SlitNo = slitNo,
            NdtPipes = request.NdtPipes,
            RejectedPipes = request.RejectedPipes,
            SlitStartTime = startParsed,
            SlitFinishTime = finishParsed,
            MillNo = request.MillNo,
            NdtShortLengthPipe = request.NdtShortLengthPipe ?? string.Empty,
            RejectedShortLengthPipe = request.RejectedShortLengthPipe ?? string.Empty
        };

        var sqlRows = new List<(InputSlitRecord Record, string NdtBatchNo, int SourceRowNumber, bool LinkBundleParent)>
        {
            (record, batchNo, 1, true)
        };

        try
        {
            await _traceability
                .RecordOutputSlitRowsAsync(path, sqlRows, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Wrote NDT output {Path} but failed to record Output_Slit_Row.", path);
            return StatusCode(500, new
            {
                Message = "NDT output file was written, but SQL Output_Slit_Row insert failed. Check logs and re-run or fix SQL.",
                FileName = fileName,
                FullPath = path,
                Folder = folder
            });
        }

        if (_sapStatus.Enabled)
        {
            try
            {
                DateTime? lwUtc = null;
                try
                {
                    lwUtc = System.IO.File.GetLastWriteTimeUtc(path);
                }
                catch
                {
                    /* ignore */
                }

                await _sapStatus
                    .RecordOutputFileWrittenAsync(fileName, lwUtc, folder, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "NDT output {File} written and SQL recorded; SAP Pending seed failed.", fileName);
            }
        }

        _logger.LogInformation(
            "Manual NDT Input Slit output created: {File} (PO {PO}, Mill {Mill}, NDT {Ndt}, Batch {Batch}).",
            fileName,
            po,
            request.MillNo,
            request.NdtPipes,
            batchNo);

        return Ok(new
        {
            Message = "NDT Input Slit output CSV created and Output_Slit_Row recorded.",
            FileName = fileName,
            FullPath = path,
            Folder = folder,
            NdtBatchNo = batchNo
        });
    }

    /// <summary>Builds <c>{SlitNo}_{yyMMdd}_{PO}</c> (no extension), matching SAP Input Slit naming.</summary>
    public static string BuildManualOutputFileName(string slitNo, DateTime slitStart, string poNumber)
    {
        var yyMMdd = slitStart.ToString("yyMMdd", CultureInfo.InvariantCulture);
        return $"{slitNo.Trim()}_{yyMMdd}_{InputSlitCsvParsing.NormalizePo(poNumber)}";
    }

    private static bool TryParseSapSlitDateTime(string? raw, out DateTime parsed, out string normalized)
    {
        parsed = default;
        normalized = string.Empty;
        var text = NormalizeSapSlitDateTimeInput(raw);
        if (!SapSlitDateTimePattern.IsMatch(text))
            return false;

        if (!DateTime.TryParseExact(
                text,
                SapSlitDateTimeFormat,
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out parsed))
            return false;

        // Re-format so CSV always gets the canonical string even if spacing differed after trim.
        normalized = parsed.ToString(SapSlitDateTimeFormat, CultureInfo.InvariantCulture);
        return true;
    }

    /// <summary>Strip quotes/NBSP and collapse whitespace so operators can paste SAP times directly.</summary>
    private static string NormalizeSapSlitDateTimeInput(string? raw)
    {
        var text = (raw ?? string.Empty)
            .Replace('\u00a0', ' ')
            .Replace('\t', ' ')
            .Replace('\r', ' ')
            .Replace('\n', ' ')
            .Trim();
        if (text.Length >= 2 &&
            ((text[0] == '"' && text[^1] == '"') || (text[0] == '\'' && text[^1] == '\'')))
            text = text[1..^1].Trim();

        while (text.Contains("  ", StringComparison.Ordinal))
            text = text.Replace("  ", " ", StringComparison.Ordinal);

        return text;
    }

    private static string CsvEscape(string value)
    {
        if (value.IndexOfAny([',', '"', '\r', '\n']) < 0)
            return value;
        return "\"" + value.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
    }
}

/// <summary>Request body for <see cref="InputSlitsController.CreateManualFile"/>.</summary>
public sealed class ManualInputSlitRequest
{
    public string? PoNumber { get; set; }
    public int MillNo { get; set; } = 1;
    /// <summary>Required; used as the SlitNumber segment of <c>SlitNumber_YYMMDD_PONumber</c>.</summary>
    public string? SlitNo { get; set; }
    public int NdtPipes { get; set; }
    public int RejectedPipes { get; set; }
    /// <summary>Required SAP wall time: <c>dd.MM.yyyy HH:mm:ss</c> (e.g. <c>27.09.2026 14:53:31</c>).</summary>
    public string? SlitStartTime { get; set; }
    /// <summary>Required SAP wall time: <c>dd.MM.yyyy HH:mm:ss</c>.</summary>
    public string? SlitFinishTime { get; set; }
    public string? NdtShortLengthPipe { get; set; }
    public string? RejectedShortLengthPipe { get; set; }
    /// <summary>Required NDT Batch No for the output CSV and <c>Output_Slit_Row</c>.</summary>
    public string? NdtBatchNo { get; set; }
}
