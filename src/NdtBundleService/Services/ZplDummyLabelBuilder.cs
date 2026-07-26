using System.Text;

namespace NdtBundleService.Services;

/// <summary>
/// Builds a simple ZPL (Zebra Programming Language) label for connection testing.
/// Honeywell PD45S and similar label printers expect ZPL on port 9100, not PDF.
/// </summary>
public static class ZplDummyLabelBuilder
{
    /// <summary>203 dpi Honeywell PD45S: ~8 dots/mm.</summary>
    public const int DotsPerMm = 8;

    private const int DefaultWidthMm = 100;
    private const int DefaultLengthMm = 100;

    /// <summary>
    /// Builds ZPL for a dummy NDT test tag. Returns UTF-8 bytes ready to send to the printer.
    /// </summary>
    public static byte[] BuildDummyLabelZpl(
        string bundleNo = "DUMMY-001",
        string specification = "SPEC-DUMMY",
        string pipeType = "TypeA",
        string pipeSize = "6",
        string pipeLen = "40",
        int pcsPerBundle = 10,
        string slitNo = "SLIT-01",
        int labelWidthMm = DefaultWidthMm,
        int labelLengthMm = DefaultLengthMm)
    {
        var widthDots = Math.Max(DotsPerMm, labelWidthMm * DotsPerMm);
        var lengthDots = Math.Max(DotsPerMm, labelLengthMm * DotsPerMm);
        var isSquare = labelLengthMm >= 100;

        return isSquare
            ? BuildSquareDummyZpl(bundleNo, specification, pipeType, pipeSize, pipeLen, pcsPerBundle, slitNo, widthDots, lengthDots)
            : BuildCompactDummyZpl(bundleNo, specification, pipeType, pipeSize, pipeLen, pcsPerBundle, slitNo, widthDots, lengthDots);
    }

    private static byte[] BuildSquareDummyZpl(
        string bundleNo,
        string specification,
        string pipeType,
        string pipeSize,
        string pipeLen,
        int pcsPerBundle,
        string slitNo,
        int widthDots,
        int lengthDots)
    {
        const int margin = 40;
        const int left = margin;
        var fieldWidth = widthDots - (margin * 2);
        const int topBarcodeHeight = 130;
        const int bottomBarcodeHeight = 110;
        const int barcodeHumanReadable = 34;
        const int bottomGap = 16;

        var bottomY2 = lengthDots - margin - bottomBarcodeHeight - barcodeHumanReadable;
        var bottomY1 = bottomY2 - bottomGap - bottomBarcodeHeight - barcodeHumanReadable;
        var textStartY = margin + topBarcodeHeight + barcodeHumanReadable + 12;
        var textEndY = bottomY1 - 12;
        const int textLineCount = 4;
        var lineStep = Math.Max(44, (textEndY - textStartY) / textLineCount);

        var escapedBundle = EscapeZplField(bundleNo);
        var escapedSpec = EscapeZplField(specification);
        var escapedType = EscapeZplField(pipeType);
        var escapedSize = EscapeZplField(pipeSize);
        var escapedLen = EscapeZplField(pipeLen);
        var escapedSlit = EscapeZplField(slitNo);

        var zpl = new StringBuilder();
        zpl.Append("^XA");
        zpl.AppendFormat("^PW{0}^LL{1}^LH0,0", widthDots, lengthDots);

        var y = margin;
        zpl.Append("^CF0,40");
        zpl.AppendFormat("^FO{0},{1}^FB{2},1,0,C,0^FDNDT DUMMY TAG - CONNECTION TEST^FS", left, y, fieldWidth);
        y += 52;

        zpl.AppendFormat("^FO{0},{1}^BY3^BCN,{2},Y,N,N^FD{3}^FS", left, y, topBarcodeHeight, escapedBundle);
        y = textStartY;

        zpl.Append("^CF0,36");
        zpl.AppendFormat("^FO{0},{1}^FB{2},1,0,C,0^FDBundle: {3}^FS", left, y, fieldWidth, escapedBundle);
        y += lineStep;

        zpl.Append("^CF0,32");
        zpl.AppendFormat(
            "^FO{0},{1}^FB{2},1,0,C,0^FD{3} | {4} | {5}\" | {6}'^FS",
            left,
            y,
            fieldWidth,
            escapedSpec,
            escapedType,
            escapedSize,
            escapedLen);
        y += lineStep;

        zpl.AppendFormat(
            "^FO{0},{1}^FB{2},1,0,C,0^FDPcs/Bnd: {3}   Slit: {4}^FS",
            left,
            y,
            fieldWidth,
            pcsPerBundle,
            escapedSlit);
        y += lineStep;

        zpl.Append("^CF0,34");
        zpl.AppendFormat("^FO{0},{1}^FB{2},1,0,C,0^FDTEST PRINT - VERIFY PRINTER PATH^FS", left, y, fieldWidth);

        zpl.AppendFormat("^FO{0},{1}^BY3^BCN,{2},Y,N,N^FD{3}^FS", left, bottomY1, bottomBarcodeHeight, escapedBundle);
        zpl.AppendFormat("^FO{0},{1}^BY3^BCN,{2},Y,N,N^FD{3}^FS", left, bottomY2, bottomBarcodeHeight, escapedBundle);

        zpl.Append("^CF0,30");
        zpl.AppendFormat(
            "^FO{0},{1}^FB{2},1,0,C,0^FDMADE IN OMAN - TEST PRINT^FS",
            left,
            lengthDots - margin - 28,
            fieldWidth);

        zpl.Append("^XZ");
        return Encoding.UTF8.GetBytes(zpl.ToString());
    }

    private static byte[] BuildCompactDummyZpl(
        string bundleNo,
        string specification,
        string pipeType,
        string pipeSize,
        string pipeLen,
        int pcsPerBundle,
        string slitNo,
        int widthDots,
        int lengthDots)
    {
        var zpl = new StringBuilder();
        zpl.Append("^XA");
        zpl.AppendFormat("^PW{0}^LL{1}^LH0,0", widthDots, lengthDots);
        zpl.Append("^CF0,28");
        zpl.Append("^FO50,30^FDNDT DUMMY TAG - CONNECTION TEST^FS");
        zpl.Append("^FO50,65^FDBundle: ");
        zpl.Append(EscapeZplField(bundleNo));
        zpl.Append("^FS");
        zpl.Append("^FO50,100^FD");
        zpl.Append(EscapeZplField(specification));
        zpl.Append(" | ");
        zpl.Append(EscapeZplField(pipeType));
        zpl.Append(" | ");
        zpl.Append(EscapeZplField(pipeSize));
        zpl.Append("\" | ");
        zpl.Append(EscapeZplField(pipeLen));
        zpl.Append("'^FS");
        zpl.Append("^FO50,135^FDPcs/Bnd: ");
        zpl.Append(pcsPerBundle);
        zpl.Append("   Slit: ");
        zpl.Append(EscapeZplField(slitNo));
        zpl.Append("^FS");
        zpl.Append("^FO50,170^BY2^BCN,70,Y,N,N^FD");
        zpl.Append(EscapeZplField(bundleNo));
        zpl.Append("^FS");
        zpl.Append("^CF0,22^FO50,300^FDMADE IN OMAN - TEST PRINT^FS");
        zpl.Append("^XZ");
        return Encoding.UTF8.GetBytes(zpl.ToString());
    }

    private static string EscapeZplField(string value)
    {
        if (string.IsNullOrEmpty(value)) return "";
        return value
            .Replace("\\", "\\\\")
            .Replace("^", "\\^")
            .Replace("~", "\\~");
    }
}
