using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Loads WIP label fields (Pipe Grade, Size, Thickness, Length, Weight, Type) for NDT tag printing.
/// Reads PO plan fields from <c>dbo.PO_Plan_WIP</c> when SQL is preferred, then enriches from WIP bundle CSVs if needed.
/// </summary>
public sealed class WipLabelProvider : IWipLabelProvider
{
    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly IPoPlanWipRepository _poPlanWipRepository;
    private readonly ICurrentPoPlanService? _currentPoPlanService;
    private readonly ILogger<WipLabelProvider> _logger;

    public WipLabelProvider(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        IPoPlanWipRepository poPlanWipRepository,
        ILogger<WipLabelProvider> logger,
        ICurrentPoPlanService? currentPoPlanService = null)
    {
        _optionsMonitor = optionsMonitor;
        _poPlanWipRepository = poPlanWipRepository;
        _logger = logger;
        _currentPoPlanService = currentPoPlanService;
    }

    public async Task<WipLabelInfo?> GetWipLabelAsync(string poNumber, int millNo, CancellationToken cancellationToken = default)
    {
        var options = _optionsMonitor.CurrentValue;
        WipLabelInfo? seed = null;
        if (PoPlanWipSql.IsEnabled(options))
        {
            var row = await _poPlanWipRepository.TryGetLatestByPoAsync(poNumber, cancellationToken).ConfigureAwait(false);
            if (row is not null)
            {
                seed = new WipLabelInfo
                {
                    PipeGrade = row.PipeGrade,
                    PipeSize = row.PipeSize,
                    PipeLength = row.PipeLength,
                    PipeType = row.PipeType
                };
            }
        }

        string? currentPoPlanPath = null;
        if (!PoPlanWipSql.IsEnabled(options)
            && !string.IsNullOrWhiteSpace(options.PoPlanFolder)
            && _currentPoPlanService != null)
        {
            currentPoPlanPath = await _currentPoPlanService.GetCurrentPoPlanPathAsync(cancellationToken).ConfigureAwait(false);
        }

        return await WipCsvLabelLookup.ResolveAsync(
            options,
            currentPoPlanPath,
            poNumber,
            millNo,
            _logger,
            cancellationToken,
            seed,
            skipPoPlanFolderScan: PoPlanWipSql.IsEnabled(options) && seed is not null).ConfigureAwait(false);
    }
}
