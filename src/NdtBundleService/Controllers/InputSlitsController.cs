using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Controllers;

/// <summary>
/// List and read input slit CSV files from the configured InputSlitFolder.
/// </summary>
[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
public sealed class InputSlitsController : ControllerBase
{
    private readonly NdtBundleOptions _options;
    private readonly ILogger<InputSlitsController> _logger;

    public InputSlitsController(IOptions<NdtBundleOptions> options, ILogger<InputSlitsController> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>
    /// List CSV files in the input slit folder (name, lastModified).
    /// </summary>
    [HttpGet("files")]
    public IActionResult ListFiles()
    {
        var folder = _options.InputSlitFolder;
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return Ok(Array.Empty<object>());

        var files = Directory.EnumerateFiles(folder, "*.csv")
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
            .OrderByDescending(f => f.LastModified)
            .ToList();

        return Ok(files);
    }

    /// <summary>
    /// Get parsed content of an input slit CSV file. FileName must be the base name (no path).
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
}
