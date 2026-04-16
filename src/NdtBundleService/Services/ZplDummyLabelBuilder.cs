using System.Text;

namespace NdtBundleService.Services;

/// <summary>
/// Builds a simple ZPL (Zebra Programming Language) label for connection testing.
/// Honeywell PD45S and similar label printers expect ZPL on port 9100, not PDF.
/// </summary>
public static class ZplDummyLabelBuilder
{
    /// <summary>
    /// Label width in dots (203 dpi: ~8 dots/mm). 100mm ≈ 800 dots.
    /// </summary>
    private const int LabelWidthDots = 800;

    /// <summary>
    /// Label length in dots.
    /// </summary>
    private const int LabelLengthDots = 800;

    /// <summary>
    /// Builds ZPL for a dummy NDT test tag. Returns UTF-8 bytes ready to send to the printer.
    /// </summary>
    public static byte[] BuildDummyLabelZpl(string bundleNo = "DUMMY-001", string specification = "SPEC-DUMMY",
        string pipeType = "TypeA", string pipeSize = "6", string pipeLen = "40", int pcsPerBundle = 10, string slitNo = "SLIT-01")
    {
        var zpl = new StringBuilder();
        // Start label
        zpl.Append("^XA");
        // Label width and length (100mm x 100mm at 203 dpi)
        zpl.AppendFormat("^PW{0}^LL{1}", LabelWidthDots, LabelLengthDots);
        zpl.Append("^LH0,0");
        // Default font
        zpl.Append("^CF0,28");
        // Title
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
        // Code 128 barcode (batch number + human-readable line from printer)
        zpl.Append("^FO50,170^BY2^BCN,70,Y,N,N^FD");
        zpl.Append(EscapeZplField(bundleNo));
        zpl.Append("^FS");
        // Footer
        zpl.Append("^CF0,22^FO50,300^FDMADE IN OMAN - TEST PRINT^FS");
        zpl.Append("^XZ");

        return Encoding.UTF8.GetBytes(zpl.ToString());
    }

    private static string EscapeZplField(string value)
    {
        if (string.IsNullOrEmpty(value)) return "";
        // In ZPL ^FD...^FS, replace special chars: \ -> \\, ^ -> \^, ~ -> \~
        return value
            .Replace("\\", "\\\\")
            .Replace("^", "\\^")
            .Replace("~", "\\~");
    }
}
