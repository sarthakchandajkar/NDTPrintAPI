using System.Globalization;
using System.IO;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Prints NDT bundle tags via ZPL to the configured network printer (e.g. Honeywell PD45S). Used for automatic printing when a bundle is closed or on PO end.
/// </summary>
public sealed class NdtZplTagPrinter : INdtTagPrinter
{
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly IZplGenerationToggle _zplToggle;
    private readonly IWipLabelProvider _wipLabelProvider;
    private readonly INetworkPrinterSender _sender;
    private readonly ILogger<NdtZplTagPrinter> _logger;

    public NdtZplTagPrinter(
        IOptionsMonitor<NdtBundleOptions> options,
        IZplGenerationToggle zplToggle,
        IWipLabelProvider wipLabelProvider,
        INetworkPrinterSender sender,
        ILogger<NdtZplTagPrinter> logger)
    {
        _options = options;
        _zplToggle = zplToggle;
        _wipLabelProvider = wipLabelProvider;
        _sender = sender;
        _logger = logger;
    }

    public async Task<bool> PrintBundleTagAsync(InputSlitRecord record, int batchNumber, int totalNdtPcs, bool isReprint, CancellationToken cancellationToken = default)
    {
        if (!_zplToggle.IsEnabled)
        {
            _logger.LogWarning(
                "NDT tag not sent to printer: ZPL/print is off (NdtBundle:EnableNdtTagZplAndPrint=false or disabled via POST /api/Status/zpl-generation).");
            return false;
        }

        var opt = _options.CurrentValue;
        var address = (opt.NdtTagPrinterAddress ?? "").Trim();
        var useAddress = !string.IsNullOrEmpty(address) && !address.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase);
        if (!useAddress)
        {
            _logger.LogWarning("NDT tag not sent: NdtBundle:NdtTagPrinterAddress is not set (or is 0.0.0.0). Set the label printer IP, e.g. 192.168.0.125.");
            return false;
        }

        var ndtBatchNoFormatted = FormatNdtBatchNo(batchNumber, record.MillNo);
        var wip = await _wipLabelProvider.GetWipLabelAsync(record.PoNumber, record.MillNo, cancellationToken).ConfigureAwait(false);

        var pipeGrade = wip?.PipeGrade;
        var pipeSize = wip?.PipeSize ?? "";
        var pipeThickness = wip?.PipeThickness ?? "";
        var pipeLength = wip?.PipeLength ?? "";
        var pipeWeight = wip?.PipeWeightPerMeter ?? "";
        var pipeType = wip?.PipeType ?? "";

        if (wip is null
            || (string.IsNullOrWhiteSpace(pipeGrade)
                && string.IsNullOrWhiteSpace(pipeSize)
                && string.IsNullOrWhiteSpace(pipeLength)
                && string.IsNullOrWhiteSpace(pipeWeight)))
        {
            _logger.LogWarning(
                "NDT tag for batch {BatchNo} PO {PoNumber} mill {MillNo} is missing WIP label fields (grade/size/length/weight). Check PO plan and WIP bundle CSVs.",
                ndtBatchNoFormatted,
                record.PoNumber,
                record.MillNo);
        }

        var date = record.SlitStartTime ?? DateTime.Now;

        var zplBytes = ZplNdtLabelBuilder.BuildNdtTagZpl(
            ndtBatchNoFormatted,
            record.MillNo,
            record.PoNumber,
            pipeGrade,
            pipeSize,
            pipeThickness,
            pipeLength,
            pipeWeight,
            pipeType,
            date,
            totalNdtPcs,
            isReprint);

        // Always save a ZPL preview file alongside bundle output so the layout
        // can be visualized in an external ZPL viewer without depending on the printer.
        await TrySaveZplPreviewAsync(zplBytes, ndtBatchNoFormatted, cancellationToken).ConfigureAwait(false);

        var sendResult = await _sender.SendAsync(address, opt.NdtTagPrinterPort, zplBytes, cancellationToken).ConfigureAwait(false);
        if (sendResult.Success)
            _logger.LogInformation("Printed NDT tag {BatchNo} ({Pcs} pcs) to {Address}.", ndtBatchNoFormatted, totalNdtPcs, address);
        else
            _logger.LogWarning("Failed to send NDT tag {BatchNo} to printer. {Detail}", ndtBatchNoFormatted, sendResult.ErrorDetail ?? "");
        return sendResult.Success;
    }

    private async Task TrySaveZplPreviewAsync(byte[] zplBytes, string ndtBatchNoFormatted, CancellationToken cancellationToken)
    {
        try
        {
            var folder = (_options.CurrentValue.OutputBundleFolder ?? string.Empty).Trim();
            if (string.IsNullOrEmpty(folder))
                return;

            Directory.CreateDirectory(folder);

            var safeBatch = string.Join("_", (ndtBatchNoFormatted ?? string.Empty)
                .Split(Path.GetInvalidFileNameChars(), StringSplitOptions.RemoveEmptyEntries));

            if (string.IsNullOrEmpty(safeBatch))
                safeBatch = "NDTBatch";

            var fileName = $"NDTTag_{safeBatch}_{DateTime.Now:yyyyMMddHHmmss}.zpl";
            var fullPath = Path.Combine(folder, fileName);

            await File.WriteAllBytesAsync(fullPath, zplBytes, cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("Saved ZPL preview for NDT tag {BatchNo} to {Path}", ndtBatchNoFormatted, fullPath);
        }
        catch (Exception ex)
        {
            // Preview is best-effort; do not fail printing if this throws.
            _logger.LogDebug(ex, "Failed to save ZPL preview for NDT tag {BatchNo}.", ndtBatchNoFormatted);
        }
    }

    private static string FormatNdtBatchNo(int sequenceNumber, int millNo)
    {
        var yy = (DateTime.Now.Year % 100).ToString("D2", CultureInfo.InvariantCulture);
        var millDigit = (millNo >= 1 && millNo <= 4) ? millNo.ToString(CultureInfo.InvariantCulture) : "1";
        var seq = sequenceNumber.ToString("D5", CultureInfo.InvariantCulture);
        return "12" + yy + millDigit + seq;
    }
}
