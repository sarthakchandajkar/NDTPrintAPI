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
    private readonly NdtBundleOptions _options;
    private readonly IZplGenerationToggle _zplToggle;
    private readonly IWipLabelProvider _wipLabelProvider;
    private readonly INetworkPrinterSender _sender;
    private readonly ILogger<NdtZplTagPrinter> _logger;

    public NdtZplTagPrinter(
        IOptions<NdtBundleOptions> options,
        IZplGenerationToggle zplToggle,
        IWipLabelProvider wipLabelProvider,
        INetworkPrinterSender sender,
        ILogger<NdtZplTagPrinter> logger)
    {
        _options = options.Value;
        _zplToggle = zplToggle;
        _wipLabelProvider = wipLabelProvider;
        _sender = sender;
        _logger = logger;
    }

    public async Task<bool> PrintBundleTagAsync(InputSlitRecord record, int batchNumber, int totalNdtPcs, bool isReprint, CancellationToken cancellationToken = default)
    {
        if (!_zplToggle.IsEnabled)
        {
            _logger.LogDebug("NDT tag ZPL and network print are disabled (runtime toggle).");
            return false;
        }

        var address = (_options.NdtTagPrinterAddress ?? "").Trim();
        var useAddress = !string.IsNullOrEmpty(address) && !address.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase);
        if (!useAddress)
        {
            _logger.LogDebug("NdtTagPrinterAddress not configured; skipping ZPL print.");
            return false;
        }

        var ndtBatchNoFormatted = FormatNdtBatchNo(batchNumber);
        var wip = await _wipLabelProvider.GetWipLabelAsync(record.PoNumber, record.MillNo, cancellationToken).ConfigureAwait(false);

        var pipeGrade = wip?.PipeGrade;
        var pipeSize = wip?.PipeSize ?? "";
        var pipeThickness = wip?.PipeThickness ?? "";
        var pipeLength = wip?.PipeLength ?? "";
        var pipeWeight = wip?.PipeWeightPerMeter ?? "";
        var pipeType = wip?.PipeType ?? "";

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

        var sent = await _sender.SendAsync(address, _options.NdtTagPrinterPort, zplBytes, cancellationToken).ConfigureAwait(false);
        if (sent)
            _logger.LogInformation("Printed NDT tag {BatchNo} ({Pcs} pcs) to {Address}.", ndtBatchNoFormatted, totalNdtPcs, address);
        else
            _logger.LogWarning("Failed to send NDT tag {BatchNo} to printer.", ndtBatchNoFormatted);
        return sent;
    }

    private async Task TrySaveZplPreviewAsync(byte[] zplBytes, string ndtBatchNoFormatted, CancellationToken cancellationToken)
    {
        try
        {
            var folder = (_options.OutputBundleFolder ?? string.Empty).Trim();
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

    private string FormatNdtBatchNo(int sequenceNumber)
    {
        var yy = (DateTime.Now.Year % 100).ToString("D2", CultureInfo.InvariantCulture);
        var raw = (_options.ShopId ?? "01").Trim();
        var shopId = raw.Length >= 2 ? raw[..2].PadLeft(2, '0') : raw.PadLeft(2, '0');
        var seq = sequenceNumber.ToString("D5", CultureInfo.InvariantCulture);
        return "9" + yy + shopId + seq;
    }
}
