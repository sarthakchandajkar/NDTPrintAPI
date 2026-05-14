using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Loads WIP label fields (Pipe Grade, Size, Thickness, Length, Weight, Type) for NDT tag printing.
/// Reads the current PO plan CSV and, when needed, matching <c>WIP_MM_PO_…</c> files under TM bundle folders.
/// </summary>
public sealed class WipLabelProvider : IWipLabelProvider
{
    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly ICurrentPoPlanService? _currentPoPlanService;
    private readonly ILogger<WipLabelProvider> _logger;

    public WipLabelProvider(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        ILogger<WipLabelProvider> logger,
        ICurrentPoPlanService? currentPoPlanService = null)
    {
        _optionsMonitor = optionsMonitor;
        _currentPoPlanService = currentPoPlanService;
        _logger = logger;
    }

    public async Task<WipLabelInfo?> GetWipLabelAsync(string poNumber, int millNo, CancellationToken cancellationToken = default)
    {
        string? currentPoPlanPath = null;
        if (!string.IsNullOrWhiteSpace(_optionsMonitor.CurrentValue.PoPlanFolder) && _currentPoPlanService != null)
            currentPoPlanPath = await _currentPoPlanService.GetCurrentPoPlanPathAsync(cancellationToken).ConfigureAwait(false);

        return await WipCsvLabelLookup.ResolveAsync(
            _optionsMonitor.CurrentValue,
            currentPoPlanPath,
            poNumber,
            millNo,
            _logger,
            cancellationToken).ConfigureAwait(false);
    }
}
