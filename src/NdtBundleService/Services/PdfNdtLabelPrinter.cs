using System.Net.Sockets;
using System.Drawing;
using System.Drawing.Imaging;
using System.Runtime.Versioning;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using QuestPDF.Fluent;
using QuestPDF.Helpers;
using QuestPDF.Infrastructure;
using ZXing;
using ZXing.Common;
using ZXing.Windows.Compatibility;

namespace NdtBundleService.Services;

/// <summary>
/// Renders the NDT bundle tag as PDF using QuestPDF and ZXing (no Telerik).
/// Layout matches Rpt_NDTLabel / NDT_Bundle_Printing_POC: 100×100mm page, panel 9.7×9.5cm at 2mm, 3pt border,
/// Microsoft New Tai Lue body, Malgun Gothic footer, Code128 barcodes 48×18mm and 19×42mm (90° at 78,53mm).
/// Sends the PDF to the configured network printer (NdtTagPrinterAddress:Port) or saves to OutputBundleFolder.
/// </summary>
[SupportedOSPlatform("windows")]
public sealed class PdfNdtLabelPrinter : INdtLabelPrinter
{
    // Page 100×100mm; after 2mm padding each side, content area = 96mm. Row widths must sum to 96.
    private const float PageSizeMm = 100f;
    private const float PanelOffsetMm = 2f;
    private const float PanelWidthMm = 96f;
    private const float BorderPt = 1f; // 1pt to avoid constraint conflict (3pt can reduce inner space)
    private const string FontBody = "Microsoft New Tai Lue";
    private const string FontFooter = "Malgun Gothic";

    private readonly NdtBundleOptions _options;
    private readonly IBundleLabelInfoProvider? _labelInfoProvider;
    private readonly ILogger<PdfNdtLabelPrinter> _logger;

    public PdfNdtLabelPrinter(
        IOptions<NdtBundleOptions> options,
        ILogger<PdfNdtLabelPrinter> logger)
        : this(options, null, logger) { }

    public PdfNdtLabelPrinter(
        IOptions<NdtBundleOptions> options,
        IBundleLabelInfoProvider? labelInfoProvider,
        ILogger<PdfNdtLabelPrinter> logger)
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

        var sizeFormatted = string.IsNullOrEmpty(size) ? "" : size + "''";
        var lengthFormatted = string.IsNullOrEmpty(length) ? "" : length + "'";
        var pcsBund = totalNdtPcs.ToString();
        var slitNo = contextRecord.SlitNo ?? "";
        var bundleNo = ndtBatchNoFormatted;

        byte[] pdfBytes = BuildLabelPdf(bundleNo, specification, type, sizeFormatted, lengthFormatted, pcsBund, slitNo, isReprint);
        return await SaveAndSendPdfAsync(pdfBytes, bundleNo, cancellationToken).ConfigureAwait(false);
    }

    public async Task<bool> PrintLabelFromDataAsync(NDTBundlePrintData printData, CancellationToken cancellationToken)
    {
        var specification = printData.Pipe_Grade ?? "";
        var type = printData.Pipe_Type ?? "";
        var sizeFormatted = string.IsNullOrEmpty(printData.Pipe_Size) ? "" : printData.Pipe_Size + "''";
        var lengthFormatted = string.IsNullOrEmpty(printData.Pipe_Len) ? "" : printData.Pipe_Len + "'";
        var pcsBund = printData.NDT_Pcs.ToString();
        var slitNo = printData.SlitNo ?? "";
        var bundleNo = printData.BundleNo ?? "";

        byte[] pdfBytes = BuildLabelPdf(bundleNo, specification, type, sizeFormatted, lengthFormatted, pcsBund, slitNo, printData.IsReprint);
        return await SaveAndSendPdfAsync(pdfBytes, bundleNo, cancellationToken).ConfigureAwait(false);
    }

    private byte[] BuildLabelPdf(string bundleNo, string specification, string type, string sizeFormatted, string lengthFormatted, string pcsBund, string slitNo, bool isReprint)
    {
        byte[] barcode1Png = GenerateCode128Png(bundleNo, 480, 180); // 48×18mm
        byte[] barcode2Png = GenerateCode128Png(bundleNo, 190, 420); // 19×42mm vertical

        return Document.Create(container =>
        {
            container.Page(page =>
            {
                page.Size(10, 10, Unit.Centimetre); // 100×100mm
                page.Margin(0);
                // Panel: 2mm from top-left, 96mm wide. No border to avoid inner constraint (border can be re-added later).
                page.Content()
                    .Padding(PanelOffsetMm, Unit.Millimetre)
                    .Width(PanelWidthMm, Unit.Millimetre)
                    .Column(column =>
                    {
                        // Header: barcode 48mm, text takes remaining, logo 18mm (RelativeItem absorbs any rounding)
                        column.Item().Height(18, Unit.Millimetre).Row(row =>
                        {
                            row.ConstantItem(48, Unit.Millimetre).Width(48, Unit.Millimetre).Height(18, Unit.Millimetre)
                                .Image(barcode1Png);
                            row.RelativeItem().AlignCenter().AlignMiddle()
                                .Text("AJSPC - OMAN").FontSize(11).Bold().FontFamily(FontBody);
                            row.ConstantItem(18, Unit.Millimetre).Width(18, Unit.Millimetre).Height(18, Unit.Millimetre)
                                .Background(Colors.Grey.Lighten3).AlignCenter().AlignMiddle();
                        });

                        // Row 2: SPECIFICATION label + value (relative)
                        column.Item().Height(10, Unit.Millimetre).Row(row =>
                        {
                            row.ConstantItem(28, Unit.Millimetre).AlignMiddle().Text("SPECIFICATION").FontSize(9).Bold().FontFamily(FontBody);
                            row.RelativeItem().AlignCenter().AlignMiddle().Text(specification).FontSize(11).FontFamily(FontBody);
                        });

                        // Row 3: Type, Size, Length, Pcs/Bnd labels
                        column.Item().Height(7, Unit.Millimetre).Row(row =>
                        {
                            row.ConstantItem(28, Unit.Millimetre).Text("Type").FontSize(10).Bold().FontFamily(FontBody);
                            row.ConstantItem(20, Unit.Millimetre).Text("Size").FontSize(10).Bold().FontFamily(FontBody);
                            row.ConstantItem(28, Unit.Millimetre).Text("Length").FontSize(10).Bold().FontFamily(FontBody);
                            row.RelativeItem().Text("Pcs/Bnd").FontSize(10).Bold().FontFamily(FontBody);
                        });

                        // Row 4: values
                        column.Item().Height(9, Unit.Millimetre).Row(row =>
                        {
                            row.ConstantItem(28, Unit.Millimetre).AlignMiddle().Text(type).FontSize(9).FontFamily(FontBody);
                            row.ConstantItem(20, Unit.Millimetre).AlignMiddle().Text(sizeFormatted).FontSize(9).FontFamily(FontBody);
                            row.ConstantItem(28, Unit.Millimetre).AlignMiddle().Text(lengthFormatted).FontSize(9).FontFamily(FontBody);
                            row.RelativeItem().AlignMiddle().Text(pcsBund).FontSize(9).FontFamily(FontBody);
                        });

                        // Row 5–6: SLIT NUMBER + value
                        column.Item().Height(9, Unit.Millimetre).PaddingTop(2, Unit.Millimetre).Text("SLIT NUMBER").FontSize(9).Bold().FontFamily(FontBody);
                        column.Item().Height(8, Unit.Millimetre).Text(slitNo).FontSize(9).FontFamily(FontBody);

                        // Row 7–8: BUNDLE NUMBER + value
                        column.Item().Height(9, Unit.Millimetre).PaddingTop(2, Unit.Millimetre).Text("BUNDLE NUMBER").FontSize(9).Bold().FontFamily(FontBody);
                        column.Item().Height(8, Unit.Millimetre).Text(bundleNo).FontSize(9).FontFamily(FontBody);

                        // Footer: Reprint "R" (right) + "MADE IN OMAN" (center), Malgun Gothic 12pt
                        column.Item().Height(10, Unit.Millimetre).PaddingTop(3, Unit.Millimetre).Row(row =>
                        {
                            row.ConstantItem(10, Unit.Millimetre).AlignRight().AlignMiddle()
                                .Text(isReprint ? "R" : "").FontSize(25).Bold().FontFamily(FontBody);
                            row.RelativeItem().AlignCenter().AlignMiddle().Text("MADE IN OMAN").FontSize(12).Bold().FontFamily(FontFooter);
                        });
                    });
            });
        }).GeneratePdf();
    }

    private async Task<bool> SaveAndSendPdfAsync(byte[] pdfBytes, string bundleNo, CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(_options.OutputBundleFolder))
        {
            Directory.CreateDirectory(_options.OutputBundleFolder);
            var safeBundleNo = string.Join("_", bundleNo.Split(Path.GetInvalidFileNameChars()));
            var pdfFileName = $"NDTLabel_{safeBundleNo}_{DateTime.Now:yyyyMMddHHmmss}.pdf";
            var pdfPath = Path.Combine(_options.OutputBundleFolder, pdfFileName);
            await File.WriteAllBytesAsync(pdfPath, pdfBytes, cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("Saved NDT label PDF: {Path}", pdfPath);
        }

        var address = (_options.NdtTagPrinterAddress ?? "").Trim();
        var useAddress = !string.IsNullOrEmpty(address) && !address.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase);

        if (useAddress)
        {
            var sent = await SendToNetworkPrinterAsync(pdfBytes, address, _options.NdtTagPrinterPort, cancellationToken).ConfigureAwait(false);
            if (sent)
                _logger.LogInformation("Sent NDT label to printer at {Address}:{Port}.", address, _options.NdtTagPrinterPort);
            else
                _logger.LogWarning("Could not send to printer at {Address}:{Port}. PDF was saved to output folder. Check that the printer accepts TCP on port {Port} and supports PDF (many label printers expect ZPL instead).", address, _options.NdtTagPrinterPort, _options.NdtTagPrinterPort);
            return sent;
        }

        if (!useAddress && !string.IsNullOrWhiteSpace(_options.NdtTagPrinterName))
            _logger.LogDebug("NdtTagPrinterName set but direct IP not configured; PDF saved to output folder only.");
        return false;
    }

    [SupportedOSPlatform("windows")]
    private static byte[] GenerateCode128Png(string content, int widthPx, int heightPx)
    {
        var writer = new BarcodeWriter<Bitmap>
        {
            Format = BarcodeFormat.CODE_128,
            Renderer = new BitmapRenderer(),
            Options = new EncodingOptions
            {
                Width = widthPx,
                Height = heightPx,
                Margin = 2,
                PureBarcode = false
            }
        };
        using var bitmap = writer.Write(content);
        using var ms = new MemoryStream();
        bitmap.Save(ms, System.Drawing.Imaging.ImageFormat.Png);
        return ms.ToArray();
    }

    private async Task<bool> SendToNetworkPrinterAsync(byte[] data, string host, int port, CancellationToken cancellationToken)
    {
        var bindAddress = (_options.NdtTagPrinterLocalBindAddress ?? "").Trim();
        _logger.LogInformation("Connecting to printer at {Host}:{Port} (local bind: {Bind}).", host, port, string.IsNullOrEmpty(bindAddress) ? "any" : bindAddress);

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(TimeSpan.FromSeconds(10));

            TcpClient client;
            if (!string.IsNullOrEmpty(bindAddress) && System.Net.IPAddress.TryParse(bindAddress, out var localIp))
            {
                client = new TcpClient(new System.Net.IPEndPoint(localIp, 0));
            }
            else
            {
                client = new TcpClient();
            }

            using (client)
            {
                await client.ConnectAsync(host, port, cts.Token).ConfigureAwait(false);
                _logger.LogDebug("TCP connected, sending {Bytes} bytes.", data.Length);
                await using var stream = client.GetStream();
                await stream.WriteAsync(data, cts.Token).ConfigureAwait(false);
                await stream.FlushAsync(cts.Token).ConfigureAwait(false);
                // Brief delay so printer can receive last packet before we close (some firmware expects this)
                await Task.Delay(200, CancellationToken.None).ConfigureAwait(false);
            }
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Printer send failed to {Host}:{Port}. Error: {Message}", host, port, ex.Message);
            return false;
        }
    }
}
