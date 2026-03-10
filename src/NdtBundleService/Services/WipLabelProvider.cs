using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Loads WIP label fields (Pipe Grade, Size, Thickness, Length, Weight, Type) from the current PO plan CSV for tag printing.
/// Uses PoPlanFolder/CurrentPoPlanService when set, otherwise PoPlanCsvPath.
/// </summary>
public sealed class WipLabelProvider : IWipLabelProvider
{
    private readonly NdtBundleOptions _options;
    private readonly ICurrentPoPlanService? _currentPoPlanService;
    private readonly ILogger<WipLabelProvider> _logger;

    public WipLabelProvider(
        IOptions<NdtBundleOptions> options,
        ILogger<WipLabelProvider> logger,
        ICurrentPoPlanService? currentPoPlanService = null)
    {
        _options = options.Value;
        _currentPoPlanService = currentPoPlanService;
        _logger = logger;
    }

    public async Task<WipLabelInfo?> GetWipLabelAsync(string poNumber, int millNo, CancellationToken cancellationToken = default)
    {
        string? path;
        if (!string.IsNullOrWhiteSpace(_options.PoPlanFolder) && _currentPoPlanService != null)
            path = await _currentPoPlanService.GetCurrentPoPlanPathAsync(cancellationToken).ConfigureAwait(false);
        else
            path = _options.PoPlanCsvPath;

        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
            return null;

        await using var stream = File.OpenRead(path);
        using var reader = new StreamReader(stream);
        var headerLine = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
        if (headerLine is null)
            return null;

        var headers = headerLine.Split(',');
        int GetIndex(string name)
        {
            for (var i = 0; i < headers.Length; i++)
                if (headers[i].Trim().Equals(name, StringComparison.OrdinalIgnoreCase))
                    return i;
            return -1;
        }

        var poIndex = GetIndex("PO_No") >= 0 ? GetIndex("PO_No") : GetIndex("PO Number");
        var millIndex = GetIndex("Mill Number");
        var gradeIndex = GetIndex("Pipe Grade");
        var sizeIndex = GetIndex("Pipe Size");
        var thicknessIndex = GetIndex("Pipe Thickness");
        var lengthIndex = GetIndex("Pipe Length");
        var weightIndex = GetIndex("Pipe Weight Per Meter");
        var typeIndex = GetIndex("Pipe Type");

        if (poIndex < 0)
            return null;

        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;
            var cols = line.Split(',');
            if (cols.Length <= poIndex)
                continue;
            var po = cols[poIndex].Trim();
            if (!po.Equals(poNumber.Trim(), StringComparison.OrdinalIgnoreCase))
                continue;
            if (millIndex >= 0 && millIndex < cols.Length)
            {
                var millStr = cols[millIndex].Trim();
                if (int.TryParse(millStr, out var m) && m != millNo)
                    continue;
            }

            string Get(int idx) => idx >= 0 && idx < cols.Length ? cols[idx].Trim() : "";
            return new WipLabelInfo
            {
                PipeGrade = Get(gradeIndex),
                PipeSize = Get(sizeIndex),
                PipeThickness = Get(thicknessIndex),
                PipeLength = Get(lengthIndex),
                PipeWeightPerMeter = Get(weightIndex),
                PipeType = Get(typeIndex)
            };
        }

        return null;
    }
}
