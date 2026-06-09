using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>Reads the tail of the latest Serilog rolling log file.</summary>
public sealed class AppLogReader
{
    private readonly IOptionsMonitor<FileLoggingOptions> _options;
    private readonly IHostEnvironment _environment;

    public AppLogReader(IOptionsMonitor<FileLoggingOptions> options, IHostEnvironment environment)
    {
        _options = options;
        _environment = environment;
    }

    public string ResolveLogFolder()
    {
        var folder = (_options.CurrentValue.Folder ?? string.Empty).Trim();
        return string.IsNullOrEmpty(folder)
            ? Path.Combine(_environment.ContentRootPath, "Logs")
            : folder;
    }

    public async Task<AppLogTailResult> ReadTailAsync(int maxLines, CancellationToken cancellationToken)
    {
        maxLines = Math.Clamp(maxLines, 1, 5000);
        var folder = ResolveLogFolder();
        if (!Directory.Exists(folder))
        {
            return new AppLogTailResult(folder, null, Array.Empty<string>());
        }

        var prefix = string.IsNullOrWhiteSpace(_options.CurrentValue.FileNamePrefix)
            ? "ndtbundle"
            : _options.CurrentValue.FileNamePrefix.Trim();

        var latest = Directory
            .EnumerateFiles(folder, $"{prefix}*.log", SearchOption.TopDirectoryOnly)
            .Select(p => new FileInfo(p))
            .OrderByDescending(f => f.LastWriteTimeUtc)
            .ThenByDescending(f => f.Name, StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault();

        if (latest is null)
            return new AppLogTailResult(folder, null, Array.Empty<string>());

        var lines = await ReadLastLinesAsync(latest.FullName, maxLines, cancellationToken).ConfigureAwait(false);
        return new AppLogTailResult(folder, latest.Name, lines);
    }

    private static async Task<IReadOnlyList<string>> ReadLastLinesAsync(string path, int maxLines, CancellationToken cancellationToken)
    {
        const int maxBytesToScan = 2 * 1024 * 1024;
        await using var stream = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete);

        var length = stream.Length;
        if (length == 0)
            return Array.Empty<string>();

        var readSize = (int)Math.Min(length, maxBytesToScan);
        stream.Seek(-readSize, SeekOrigin.End);
        var buffer = new byte[readSize];
        var read = await stream.ReadAsync(buffer.AsMemory(0, readSize), cancellationToken).ConfigureAwait(false);
        var text = System.Text.Encoding.UTF8.GetString(buffer, 0, read);
        var allLines = text.Split('\n');
        if (readSize < length && allLines.Length > 0)
            allLines = allLines.Skip(1).ToArray();

        return allLines
            .Select(l => l.TrimEnd('\r'))
            .Where(l => l.Length > 0)
            .TakeLast(maxLines)
            .ToList();
    }
}

public sealed record AppLogTailResult(string Folder, string? FileName, IReadOnlyList<string> Lines);
