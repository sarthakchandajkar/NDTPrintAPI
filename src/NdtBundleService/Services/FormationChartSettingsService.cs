using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

public interface IFormationChartSettingsService
{
    Task<IReadOnlyList<FormationChartEntry>> GetEntriesAsync(CancellationToken cancellationToken);

    Task SaveEntriesAsync(IReadOnlyList<FormationChartEntry> entries, CancellationToken cancellationToken);
}

public sealed class FormationChartSettingsService : IFormationChartSettingsService
{
    private readonly IFormationChartProvider _formationChartProvider;
    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly ILogger<FormationChartSettingsService> _logger;

    public FormationChartSettingsService(
        IFormationChartProvider formationChartProvider,
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        ILogger<FormationChartSettingsService> logger)
    {
        _formationChartProvider = formationChartProvider;
        _optionsMonitor = optionsMonitor;
        _logger = logger;
    }

    public async Task<IReadOnlyList<FormationChartEntry>> GetEntriesAsync(CancellationToken cancellationToken)
    {
        var chart = await _formationChartProvider.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);
        return chart.Values
            .OrderBy(e => e.PipeSize, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    public async Task SaveEntriesAsync(IReadOnlyList<FormationChartEntry> entries, CancellationToken cancellationToken)
    {
        if (entries.Count == 0)
            throw new InvalidOperationException("At least one formation chart row is required.");

        var path = FormationChartPathResolver.Resolve(_optionsMonitor.CurrentValue);
        if (string.IsNullOrWhiteSpace(path))
            throw new InvalidOperationException("FormationChartCsvPath is not configured.");

        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        var sb = new StringBuilder();
        sb.AppendLine("PipeSize,RequiredNdtPcs");
        foreach (var e in entries.OrderBy(x => x.PipeSize, StringComparer.OrdinalIgnoreCase))
        {
            var size = (e.PipeSize ?? string.Empty).Trim();
            if (string.IsNullOrEmpty(size))
                continue;
            if (e.RequiredNdtPcs <= 0)
                throw new InvalidOperationException($"Required NDT pcs must be positive for pipe size '{size}'.");
            sb.Append(size).Append(',').Append(e.RequiredNdtPcs.ToString(CultureInfo.InvariantCulture)).AppendLine();
        }

        try
        {
            await File.WriteAllTextAsync(path, sb.ToString(), cancellationToken).ConfigureAwait(false);
        }
        catch (UnauthorizedAccessException ex)
        {
            throw new InvalidOperationException(
                $"Cannot write formation chart to '{path}'. The NdtBundleService Windows account needs Modify permission on that file or folder, " +
                "or set NdtBundle:FormationChartCsvPath to a writable path (e.g. the same PAS share folder as NdtBundleRuntimeStateFile).",
                ex);
        }

        _formationChartProvider.InvalidateCache();
        _logger.LogInformation("Updated formation chart at {Path} ({Count} rows).", path, entries.Count);
    }
}
