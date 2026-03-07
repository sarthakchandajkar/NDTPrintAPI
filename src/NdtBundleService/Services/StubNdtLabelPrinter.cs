using Microsoft.Extensions.Logging;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// No-op label printer when Telerik.Reporting is not available (e.g. NuGet feed not configured).
/// To use the Telerik NDT tag design: add the Telerik NuGet feed (https://nuget.telerik.com/v3/index.json),
/// restore NdtBundleService.Reports, add a project reference to it from NdtBundleService, and register
/// TelerikNdtLabelPrinter instead of this stub in Program.cs.
/// </summary>
public sealed class StubNdtLabelPrinter : INdtLabelPrinter
{
    private readonly ILogger<StubNdtLabelPrinter> _logger;

    public StubNdtLabelPrinter(ILogger<StubNdtLabelPrinter> logger)
    {
        _logger = logger;
    }

    public Task<bool> PrintLabelAsync(InputSlitRecord contextRecord, string ndtBatchNoFormatted, int totalNdtPcs, bool isReprint, CancellationToken cancellationToken)
    {
        _logger.LogDebug("NDT tag print skipped (stub). Bundle {BundleNo} Pcs {Pcs}. Configure Telerik and BundleLabelCsvPath to enable.", ndtBatchNoFormatted, totalNdtPcs);
        return Task.FromResult(false);
    }

    public Task<bool> PrintLabelFromDataAsync(NDTBundlePrintData printData, CancellationToken cancellationToken)
    {
        _logger.LogDebug("NDT tag print from data skipped (stub). Bundle {BundleNo}.", printData.BundleNo);
        return Task.FromResult(false);
    }
}
