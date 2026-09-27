using System.Globalization;
using System.Text;
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
    /// (e.g. <c>Z:\To SAP\TM\NDT\NDT Input Slit\Input Slit</c>) with <c>NDT Batch No</c>, and record
    /// <c>Output_Slit_Row</c> (+ SAP Pending status when SQL is enabled). Does not write to the SAP inbox.
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

        var batchNo = (request.NdtBatchNo ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(batchNo))
            return BadRequest(new { Message = "NDT Batch No is required." });

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

        var fileName = BuildManualOutputFileName(request.FileName, request.MillNo, po);
        if (fileName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0)
            return BadRequest(new { Message = "Invalid file name." });

        var path = Path.Combine(folder, fileName);
        if (System.IO.File.Exists(path))
            return Conflict(new { Message = $"File already exists: {fileName}" });

        var start = FormatOptionalDateTime(request.SlitStartTime);
        var finish = FormatOptionalDateTime(request.SlitFinishTime);
        var row = string.Join(",",
            CsvEscape(po),
            CsvEscape(request.SlitNo ?? string.Empty),
            request.NdtPipes.ToString(CultureInfo.InvariantCulture),
            request.RejectedPipes.ToString(CultureInfo.InvariantCulture),
            CsvEscape(start),
            CsvEscape(finish),
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
            SlitNo = request.SlitNo ?? string.Empty,
            NdtPipes = request.NdtPipes,
            RejectedPipes = request.RejectedPipes,
            SlitStartTime = request.SlitStartTime,
            SlitFinishTime = request.SlitFinishTime,
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

    private static string BuildManualOutputFileName(string? requested, int millNo, string po)
    {
        if (!string.IsNullOrWhiteSpace(requested))
            return Path.GetFileName(requested.Trim());

        var stamp = DateTime.Now.ToString("yyMMdd_HHmmss", CultureInfo.InvariantCulture);
        return $"Manual_{millNo:D2}_{po}_{stamp}.csv";
    }

    private static string FormatOptionalDateTime(DateTime? value) =>
        value.HasValue
            ? value.Value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)
            : string.Empty;

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
    public string? SlitNo { get; set; }
    public int NdtPipes { get; set; }
    public int RejectedPipes { get; set; }
    public DateTime? SlitStartTime { get; set; }
    public DateTime? SlitFinishTime { get; set; }
    public string? NdtShortLengthPipe { get; set; }
    public string? RejectedShortLengthPipe { get; set; }
    /// <summary>Required NDT Batch No for the output CSV and <c>Output_Slit_Row</c>.</summary>
    public string? NdtBatchNo { get; set; }
    /// <summary>Optional basename under OutputBundleFolder; default <c>Manual_{mill}_{po}_{yyMMdd_HHmmss}.csv</c>.</summary>
    public string? FileName { get; set; }
}
