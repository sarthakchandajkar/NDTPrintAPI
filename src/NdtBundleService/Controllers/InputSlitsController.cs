using System.Globalization;
using System.Linq;
using System.Text;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;

namespace NdtBundleService.Controllers;

/// <summary>
/// List and create input slit files (extensionless SAP exports or <c>.csv</c>) in the configured InputSlitFolder.
/// </summary>
[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
[InstanceRole(InstanceRoleModes.Monolith, InstanceRoleModes.Shared, InstanceRoleModes.Mill)]
public sealed class InputSlitsController : ControllerBase
{
    public const string ManualCsvHeader =
        "PO Number,Slit No,NDT Pipes,Rejected P,Slit Start Time,Slit Finish Time,Mill No,NDT Short Length Pipe,Rejected Short Length Pipe";

    private readonly NdtBundleOptions _options;
    private readonly ILogger<InputSlitsController> _logger;

    public InputSlitsController(IOptions<NdtBundleOptions> options, ILogger<InputSlitsController> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>
    /// List slit inbox files in the input slit folder (name, lastModified)—extensionless SAP exports or <c>.csv</c>.
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
    /// Create a manual Input Slit CSV in <see cref="NdtBundleOptions.InputSlitFolder"/> so the
    /// monitoring worker processes it on the next poll (missed / late SAP rows).
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

        var folder = (_options.InputSlitFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(folder))
            return BadRequest(new { Message = "InputSlitFolder is not configured." });

        try
        {
            Directory.CreateDirectory(folder);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Cannot create Input Slit folder {Folder}.", folder);
            return StatusCode(500, new { Message = "Input Slit folder is not writable." });
        }

        var fileName = BuildManualFileName(request.FileName, request.MillNo, po);
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
            CsvEscape(request.RejectedShortLengthPipe ?? string.Empty));

        try
        {
            await System.IO.File.WriteAllLinesAsync(
                    path,
                    new[] { ManualCsvHeader, row },
                    Encoding.UTF8,
                    cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to write manual Input Slit file {Path}.", path);
            return StatusCode(500, new { Message = "Failed to write file." });
        }

        _logger.LogInformation(
            "Manual Input Slit file created: {File} (PO {PO}, Mill {Mill}, NDT {Ndt}).",
            fileName,
            po,
            request.MillNo,
            request.NdtPipes);

        return Ok(new
        {
            Message = "Input Slit file created; it will be processed on the next poll.",
            FileName = fileName,
            FullPath = path,
            Folder = folder
        });
    }

    private static string BuildManualFileName(string? requested, int millNo, string po)
    {
        if (!string.IsNullOrWhiteSpace(requested))
        {
            var name = Path.GetFileName(requested.Trim());
            if (!name.EndsWith(".csv", StringComparison.OrdinalIgnoreCase))
                name += ".csv";
            return name;
        }

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
    /// <summary>Optional basename; default <c>Manual_{mill}_{po}_{yyMMdd_HHmmss}.csv</c>.</summary>
    public string? FileName { get; set; }
}
