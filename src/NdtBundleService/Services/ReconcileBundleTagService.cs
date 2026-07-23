using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Shared reprint path for reconcile flows: ZPL with <c>isReprint: true</c> and print-status update.
/// </summary>
public sealed class ReconcileBundleTagService : IReconcileBundleTagService
{
    private readonly INdtBundleRepository _bundleRepository;
    private readonly IWipLabelProvider _wipLabelProvider;
    private readonly INetworkPrinterSender _printerSender;
    private readonly IMillPrinterSettingsService _millPrinters;
    private readonly NdtBundleOptions _options;
    private readonly IZplGenerationToggle _zplToggle;
    private readonly ILogger<ReconcileBundleTagService> _logger;

    public ReconcileBundleTagService(
        INdtBundleRepository bundleRepository,
        IWipLabelProvider wipLabelProvider,
        INetworkPrinterSender printerSender,
        IMillPrinterSettingsService millPrinters,
        IOptions<NdtBundleOptions> options,
        IZplGenerationToggle zplToggle,
        ILogger<ReconcileBundleTagService> logger)
    {
        _bundleRepository = bundleRepository;
        _wipLabelProvider = wipLabelProvider;
        _printerSender = printerSender;
        _millPrinters = millPrinters;
        _options = options.Value;
        _zplToggle = zplToggle;
        _logger = logger;
    }

    public async Task<ReconcileBundleTagPrintResult> ReprintAsync(NdtBundleRecord bundle, CancellationToken cancellationToken)
    {
        if (!_zplToggle.IsEnabled)
        {
            return new ReconcileBundleTagPrintResult
            {
                Success = false,
                Message = "NDT tag ZPL and network print are disabled (runtime toggle)."
            };
        }

        var (address, printerPort, printerConfigured) = _millPrinters.ResolveForMill(bundle.MillNo);
        if (!printerConfigured)
        {
            return new ReconcileBundleTagPrintResult
            {
                Success = false,
                Message = $"Printer not configured for Mill {bundle.MillNo} (Settings → printers)."
            };
        }

        var wip = await _wipLabelProvider.GetWipLabelAsync(bundle.PoNumber, bundle.MillNo, cancellationToken)
            .ConfigureAwait(false);
        var pipeGrade = wip?.PipeGrade;
        var pipeSize = wip?.PipeSize ?? string.Empty;
        var pipeThickness = wip?.PipeThickness ?? string.Empty;
        var pipeLength = wip?.PipeLength ?? string.Empty;
        var bundleWeight = NdtBundleWeightCalculator.FormatBundleWeight(
            wip?.PipeWeightPerMeter,
            pipeLength,
            bundle.TotalNdtPcs);
        var pipeType = wip?.PipeType ?? string.Empty;

        if (string.IsNullOrWhiteSpace(bundleWeight))
        {
            _logger.LogWarning(
                "Reprint tag for bundle {BatchNo} PO {PO}: bundle weight is empty (weight/m={Weight}, length={Length}, pcs={Pcs}).",
                bundle.BundleNo,
                bundle.PoNumber,
                wip?.PipeWeightPerMeter ?? "(missing)",
                pipeLength,
                bundle.TotalNdtPcs);
        }

        var labelDate = bundle.PrintedAt ?? bundle.SlitFinishTime ?? bundle.SlitStartTime ?? DateTime.Now;

        var zplBytes = ZplNdtLabelBuilder.BuildNdtTagZpl(
            bundle.BundleNo,
            bundle.MillNo,
            bundle.PoNumber,
            pipeGrade,
            pipeSize,
            pipeThickness,
            pipeLength,
            bundleWeight,
            pipeType,
            labelDate,
            bundle.TotalNdtPcs,
            isReprint: true);

        _logger.LogInformation(
            "Printing bundle {BatchNo} (Reprint) with {NdtPcs} pcs.",
            bundle.BundleNo,
            bundle.TotalNdtPcs);

        try
        {
            await NdtBundleOutputPaths.TrySaveBundleZplAsync(_options, bundle.BundleNo, zplBytes, cancellationToken)
                .ConfigureAwait(false);

            var sendResult = await _printerSender.SendAsync(address, printerPort, zplBytes, cancellationToken)
                .ConfigureAwait(false);
            if (sendResult.Success)
            {
                await _bundleRepository.UpdateBundlePrintStatusAsync(
                        bundle.BundleNo,
                        BundlePrintStatus.Printed,
                        null,
                        cancellationToken)
                    .ConfigureAwait(false);

                return new ReconcileBundleTagPrintResult
                {
                    Success = true,
                    Message = "Bundle tag (Reprint) sent to printer."
                };
            }

            const string detail =
                "Failed to send to printer. Check NdtTagPrinterAddress/Port and optional NdtTagPrinterLocalBindAddress.";
            await _bundleRepository.UpdateBundlePrintStatusAsync(
                    bundle.BundleNo,
                    BundlePrintStatus.PrintFailed,
                    detail,
                    cancellationToken)
                .ConfigureAwait(false);

            return new ReconcileBundleTagPrintResult
            {
                Success = false,
                Message = detail,
                ErrorDetail = sendResult.ErrorDetail
            };
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Reprint failed for bundle {BatchNo}.", bundle.BundleNo);
            await _bundleRepository.UpdateBundlePrintStatusAsync(
                    bundle.BundleNo,
                    BundlePrintStatus.PrintFailed,
                    ex.Message,
                    cancellationToken)
                .ConfigureAwait(false);

            return new ReconcileBundleTagPrintResult
            {
                Success = false,
                Message = "Print failed: " + ex.Message
            };
        }
    }
}
