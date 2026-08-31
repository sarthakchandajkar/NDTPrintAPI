using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Fails startup if leftover mill-state JSON files are still on the PAS share.
/// There is no migration: delete those files (fresh reset) before this binary can run.
/// </summary>
public sealed class LegacyJsonStateStartupCheck : IHostedService
{
    internal static readonly string[] FileNames =
    [
        "NdtBundleRuntimeState.json",
        "NdtBundleRuntimeState-M1.json",
        "NdtBundleRuntimeState-M2.json",
        "NdtBundleRuntimeState-M3.json",
        "NdtBundleRuntimeState-M4.json",
        "PoLifecycleState.json",
        "PoLifecycleState-M1.json",
        "PoLifecycleState-M2.json",
        "PoLifecycleState-M3.json",
        "PoLifecycleState-M4.json",
        "MillPrinterSettings.json",
        "MillPrinterSettings-M1.json",
        "MillPrinterSettings-M2.json",
        "MillPrinterSettings-M3.json",
        "MillPrinterSettings-M4.json"
    ];

    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly ILogger<LegacyJsonStateStartupCheck> _logger;

    public LegacyJsonStateStartupCheck(
        IOptionsMonitor<NdtBundleOptions> options,
        ILogger<LegacyJsonStateStartupCheck> logger)
    {
        _options = options;
        _logger = logger;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var found = FindLeftoverFiles(_options.CurrentValue.OutputBundleFolder);
        if (found.Count == 0)
            return Task.CompletedTask;

        var list = string.Join("; ", found);
        _logger.LogError(
            "Leftover mill-state JSON found (no migration): {Files}. Delete these files and start with empty SQL tables.",
            list);
        throw new InvalidOperationException(
            "Leftover mill-state JSON found (fresh reset required, no migration): " + list);
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    internal static IReadOnlyList<string> FindLeftoverFiles(string? outputBundleFolder)
    {
        var folders = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var output = (outputBundleFolder ?? string.Empty).Trim();
        if (!string.IsNullOrEmpty(output))
        {
            folders.Add(output);
            var parent = Path.GetDirectoryName(output.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
            if (!string.IsNullOrEmpty(parent))
                folders.Add(parent);
        }

        var found = new List<string>();
        foreach (var folder in folders)
        {
            if (!Directory.Exists(folder))
                continue;

            foreach (var name in FileNames)
            {
                var path = Path.Combine(folder, name);
                if (File.Exists(path))
                    found.Add(path);
            }
        }

        return found;
    }
}
