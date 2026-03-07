using Microsoft.Extensions.Logging;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// POC-style entry point: PrintNDTBundleTag(printData) returns bool.
/// Renders the tag from NDTBundlePrintData (layout matches Rpt_NDTLabel / NDT_Bundle_Printing_POC),
/// sends to configured printer (e.g. port 9100) or saves to OutputBundleFolder. Returns true on success.
/// </summary>
public sealed class NdtBundleTagPrintService : INdtBundleTagPrinter
{
    private readonly INdtLabelPrinter _labelPrinter;
    private readonly ILogger<NdtBundleTagPrintService> _logger;

    public NdtBundleTagPrintService(INdtLabelPrinter labelPrinter, ILogger<NdtBundleTagPrintService> logger)
    {
        _labelPrinter = labelPrinter;
        _logger = logger;
    }

    public async Task<bool> PrintNDTBundleTagAsync(NDTBundlePrintData printData, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(printData.BundleNo))
        {
            _logger.LogWarning("PrintNDTBundleTag skipped: BundleNo is empty.");
            return false;
        }

        try
        {
            var sent = await _labelPrinter.PrintLabelFromDataAsync(printData, cancellationToken).ConfigureAwait(false);
            if (sent)
                _logger.LogInformation("PrintNDTBundleTag sent to printer for bundle {BundleNo}.", printData.BundleNo);
            else
                _logger.LogWarning("PrintNDTBundleTag: PDF saved but not sent to printer for bundle {BundleNo}.", printData.BundleNo);
            return sent;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "PrintNDTBundleTag failed for bundle {BundleNo}.", printData.BundleNo);
            return false;
        }
    }
}
