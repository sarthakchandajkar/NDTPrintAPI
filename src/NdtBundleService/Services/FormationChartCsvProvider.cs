using System.Globalization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Loads the NDT Bundle Formation Chart from a CSV file, or returns the built-in default chart when no file is configured.
/// CSV expected columns: PipeSize (or Size), RequiredNdtPcs (or Pcs/Bundle).
/// </summary>
public sealed class FormationChartCsvProvider : IFormationChartProvider
{
    private static readonly IReadOnlyDictionary<string, FormationChartEntry> BuiltInFormationChart =
        new Dictionary<string, FormationChartEntry>(StringComparer.OrdinalIgnoreCase)
        {
            { "Default", new FormationChartEntry { PipeSize = "Default", RequiredNdtPcs = 20 } },
            { "0.5", new FormationChartEntry { PipeSize = "0.5", RequiredNdtPcs = 2 } },
            { "0.75", new FormationChartEntry { PipeSize = "0.75", RequiredNdtPcs = 180 } },
            { "1", new FormationChartEntry { PipeSize = "1", RequiredNdtPcs = 150 } },
            { "1.25", new FormationChartEntry { PipeSize = "1.25", RequiredNdtPcs = 140 } },
            { "1.5", new FormationChartEntry { PipeSize = "1.5", RequiredNdtPcs = 120 } },
            { "2", new FormationChartEntry { PipeSize = "2", RequiredNdtPcs = 80 } },
            { "2.4", new FormationChartEntry { PipeSize = "2.4", RequiredNdtPcs = 60 } },
            { "2.5", new FormationChartEntry { PipeSize = "2.5", RequiredNdtPcs = 65 } },
            { "3", new FormationChartEntry { PipeSize = "3", RequiredNdtPcs = 45 } },
            { "3.5", new FormationChartEntry { PipeSize = "3.5", RequiredNdtPcs = 40 } },
            { "4", new FormationChartEntry { PipeSize = "4", RequiredNdtPcs = 35 } },
            { "5", new FormationChartEntry { PipeSize = "5", RequiredNdtPcs = 25 } },
            { "6", new FormationChartEntry { PipeSize = "6", RequiredNdtPcs = 20 } },
            { "8", new FormationChartEntry { PipeSize = "8", RequiredNdtPcs = 13 } },
        };

    private readonly NdtBundleOptions _options;
    private readonly ILogger<FormationChartCsvProvider> _logger;
    private readonly object _cacheLock = new();
    private IReadOnlyDictionary<string, FormationChartEntry>? _cached;
    private string? _cacheSignature;

    public FormationChartCsvProvider(IOptions<NdtBundleOptions> options, ILogger<FormationChartCsvProvider> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<IReadOnlyDictionary<string, FormationChartEntry>> GetFormationChartAsync(CancellationToken cancellationToken)
    {
        var path = ResolveFormationChartPath();
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
        {
            _logger.LogInformation(
                "Using built-in NDT Bundle Formation Chart (configured path {Path} is missing or empty).",
                string.IsNullOrWhiteSpace(path) ? "(none)" : path);
            return BuiltInFormationChart;
        }

        var signature = $"{path}|{File.GetLastWriteTimeUtc(path).Ticks}";
        lock (_cacheLock)
        {
            if (_cached is not null && string.Equals(_cacheSignature, signature, StringComparison.Ordinal))
                return _cached;
        }

        var result = await LoadFormationChartAsync(path, cancellationToken).ConfigureAwait(false);
        lock (_cacheLock)
        {
            _cached = result;
            _cacheSignature = signature;
        }

        return result;
    }

    private async Task<IReadOnlyDictionary<string, FormationChartEntry>> LoadFormationChartAsync(
        string path,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var result = CopyBuiltInFormationChart();

        await using var stream = File.OpenRead(path);
        using var reader = new StreamReader(stream);

        string? headerLine = await reader.ReadLineAsync().ConfigureAwait(false);
        if (headerLine is null)
            return result;

        var headers = headerLine.Split(',');
        int sizeIndex = Array.FindIndex(headers, h => h.Trim().Equals("PipeSize", StringComparison.OrdinalIgnoreCase));
        if (sizeIndex < 0)
            sizeIndex = Array.FindIndex(headers, h => h.Trim().Equals("Size", StringComparison.OrdinalIgnoreCase));
        int pcsIndex = Array.FindIndex(headers, h => h.Trim().Equals("RequiredNdtPcs", StringComparison.OrdinalIgnoreCase));
        if (pcsIndex < 0)
            pcsIndex = Array.FindIndex(headers, h => h.Trim().Equals("Pcs/Bundle", StringComparison.OrdinalIgnoreCase));

        if (sizeIndex < 0 || pcsIndex < 0)
        {
            _logger.LogWarning(
                "Formation chart CSV {Path} does not contain expected columns (PipeSize or Size, RequiredNdtPcs or Pcs/Bundle); using built-in chart.",
                path);
            return BuiltInFormationChart;
        }

        while (await reader.ReadLineAsync().ConfigureAwait(false) is { } line)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;

            var cols = line.Split(',');
            if (cols.Length <= Math.Max(sizeIndex, pcsIndex))
                continue;

            var size = cols[sizeIndex].Trim();
            if (string.IsNullOrEmpty(size))
                continue;

            if (!int.TryParse(cols[pcsIndex].Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var required))
                continue;

            result[size] = new FormationChartEntry
            {
                PipeSize = size,
                RequiredNdtPcs = required
            };
        }

        if (!result.ContainsKey("Default"))
            result["Default"] = new FormationChartEntry { PipeSize = "Default", RequiredNdtPcs = 20 };

        if (result.TryGetValue("0.5", out var halfInch))
        {
            _logger.LogInformation(
                "Loaded NDT Bundle Formation Chart from {Path}; pipe size 0.5 threshold is {Threshold} pcs.",
                path,
                halfInch.RequiredNdtPcs);
        }
        else
        {
            _logger.LogInformation("Loaded NDT Bundle Formation Chart from {Path}.", path);
        }

        return result;
    }

    private string? ResolveFormationChartPath()
    {
        var configured = (_options.FormationChartCsvPath ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(configured))
            return null;

        return Path.IsPathRooted(configured)
            ? configured
            : Path.Combine(AppContext.BaseDirectory, configured);
    }

    private static Dictionary<string, FormationChartEntry> CopyBuiltInFormationChart()
    {
        var copy = new Dictionary<string, FormationChartEntry>(StringComparer.OrdinalIgnoreCase);
        foreach (var entry in BuiltInFormationChart)
            copy[entry.Key] = entry.Value;
        return copy;
    }
}

