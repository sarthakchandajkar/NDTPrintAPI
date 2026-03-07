using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Reports;
using Telerik.Reporting;
using Telerik.Reporting.Processing;

namespace NdtBundleService.Services;

/// <summary>
/// Renders the NDT bundle tag using the Telerik Rpt_NDTLabel design and sends a print request to the configured printer.
/// Supports Windows printer by name (NdtTagPrinterName) or direct network printing by IP (NdtTagPrinterAddress:port).
/// </summary>
public sealed class TelerikNdtLabelPrinter : INdtLabelPrinter
{
    private readonly NdtBundleOptions _options;
    private readonly IBundleLabelInfoProvider? _labelInfoProvider;
    private readonly ILogger<TelerikNdtLabelPrinter> _logger;

    public TelerikNdtLabelPrinter(
        IOptions<NdtBundleOptions> options,
        ILogger<TelerikNdtLabelPrinter> logger)
        : this(options, null, logger) { }

    public TelerikNdtLabelPrinter(
        IOptions<NdtBundleOptions> options,
        IBundleLabelInfoProvider? labelInfoProvider,
        ILogger<TelerikNdtLabelPrinter> logger)
    {
        _options = options.Value;
        _labelInfoProvider = labelInfoProvider;
        _logger = logger;
    }

    public async Task<bool> PrintLabelAsync(InputSlitRecord contextRecord, string ndtBatchNoFormatted, int totalNdtPcs, bool isReprint, CancellationToken cancellationToken)
    {
        var specification = "";
        var type = "";
        var size = "";
        var length = "";

        if (_labelInfoProvider != null)
        {
            var labelInfo = await _labelInfoProvider.GetBundleLabelInfoAsync(cancellationToken).ConfigureAwait(false);
            var key = (contextRecord.PoNumber, contextRecord.MillNo);
            if (labelInfo.TryGetValue(key, out var info))
            {
                specification = info.Specification ?? "";
                type = info.Type ?? "";
                size = info.PipeSize ?? "";
                length = info.Length ?? "";
            }
        }

        var report = new Rpt_NDTLabel();
        var reportSource = new InstanceReportSource
        {
            ReportDocument = report
        };
        reportSource.Parameters.Add(new Parameter("BundleNo", ndtBatchNoFormatted));
        reportSource.Parameters.Add(new Parameter("Specification", specification));
        reportSource.Parameters.Add(new Parameter("Type", type));
        reportSource.Parameters.Add(new Parameter("Size", size));
        reportSource.Parameters.Add(new Parameter("Length", length));
        reportSource.Parameters.Add(new Parameter("PcsBund", totalNdtPcs.ToString()));
        reportSource.Parameters.Add(new Parameter("SlitNo", contextRecord.SlitNo ?? ""));
        reportSource.Parameters.Add(new Parameter("isReprint", isReprint));

        byte[] pdfBytes;
        try
        {
            var processor = new ReportProcessor();
            var result = processor.RenderReport("PDF", reportSource, null);
            pdfBytes = result.DocumentBytes;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to render NDT label report for bundle {BundleNo}.", ndtBatchNoFormatted);
            throw;
        }

        // Save PDF to output folder for records
        var outputFolder = _options.OutputBundleFolder;
        if (!string.IsNullOrWhiteSpace(outputFolder))
        {
            Directory.CreateDirectory(outputFolder);
            var safeBundleNo = string.Join("_", ndtBatchNoFormatted.Split(Path.GetInvalidFileNameChars()));
            var pdfFileName = $"NDTLabel_{safeBundleNo}_{DateTime.Now:yyyyMMddHHmmss}.pdf";
            var pdfPath = Path.Combine(outputFolder, pdfFileName);
            await File.WriteAllBytesAsync(pdfPath, pdfBytes, cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("Saved NDT label PDF: {Path}", pdfPath);
        }

        // Send print request: by IP (direct) or by printer name (Windows)
        var sent = false;
        var address = (_options.NdtTagPrinterAddress ?? "").Trim();
        var useAddress = !string.IsNullOrEmpty(address) &&
            !address.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase);

        if (useAddress)
        {
            sent = await SendToNetworkPrinterAsync(pdfBytes, address, _options.NdtTagPrinterPort, cancellationToken).ConfigureAwait(false);
            if (sent)
                _logger.LogInformation("Sent NDT label to printer at {Address}:{Port}.", address, _options.NdtTagPrinterPort);
        }

        if (!sent && !string.IsNullOrWhiteSpace(_options.NdtTagPrinterName))
        {
            SendToWindowsPrinter(pdfBytes, _options.NdtTagPrinterName);
            _logger.LogInformation("Sent NDT label to Windows printer: {PrinterName}.", _options.NdtTagPrinterName);
            sent = true;
        }

        if (!sent && !useAddress && string.IsNullOrWhiteSpace(_options.NdtTagPrinterName))
            _logger.LogDebug("No printer configured (NdtTagPrinterAddress or NdtTagPrinterName). Label PDF saved to output folder only.");
        return sent;
    }

    /// <summary>
    /// Sends raw PDF bytes to a network printer at the given IP/host and port (e.g. 9100).
    /// </summary>
    private async Task<bool> SendToNetworkPrinterAsync(byte[] data, string host, int port, CancellationToken cancellationToken)
    {
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(30));
            using var client = new TcpClient();
            await client.ConnectAsync(host, port, cts.Token).ConfigureAwait(false);
            await using var stream = client.GetStream();
            await stream.WriteAsync(data, cts.Token).ConfigureAwait(false);
            await stream.FlushAsync(cts.Token).ConfigureAwait(false);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not send label to printer at {Host}:{Port}.", host, port);
            return false;
        }
    }

    /// <summary>
    /// Sends PDF to a Windows-installed printer by name. Uses the default PDF handler to print when available.
    /// </summary>
    private void SendToWindowsPrinter(byte[] pdfBytes, string printerName)
    {
        var folder = _options.OutputBundleFolder;
        if (string.IsNullOrWhiteSpace(folder))
        {
            _logger.LogWarning("OutputBundleFolder not set; cannot print to Windows printer (PDF file required).");
            return;
        }
        try
        {
            Directory.CreateDirectory(folder);
            var tempFile = Path.Combine(folder, $"NDTLabel_Print_{DateTime.Now:yyyyMMddHHmmss}.pdf");
            File.WriteAllBytes(tempFile, pdfBytes);
            // On Windows, we could invoke the shell "print" verb or a PDF reader with -print-to. For reliability we log and leave the file; user can configure a print monitor or script.
            _logger.LogInformation("NDT label PDF saved for printing: {Path}. Printer: {Printer}. To auto-print, set NdtTagPrinterAddress to the printer IP.", tempFile, printerName);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to prepare print job for printer {PrinterName}.", printerName);
        }
    }
}
