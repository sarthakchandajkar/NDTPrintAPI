using System.Text;

namespace NdtBundleService.Services;

/// <summary>
/// Builds ZPL for the full NDT bundle tag (Honeywell PD45S).
/// Layout matches the physical NDT tag:
/// - Top: Code 128 barcode with NDT Batch Number (human-readable line printed by the barcode command)
/// - Middle content (4–5 lines):
///   Mill, PO Number, NDT Batch Number on one line
///   Grade, Pipe Size, Pipe Thickness on one line
///   Pipe Length and bundle total weight (kg) on one line
///   Date, Number of NDT pipes, Pipe type/WIP/FG and optional "Reprint" on one line
/// - Bottom: two stacked Code 128 barcodes with the same NDT Batch Number.
/// </summary>
public static class ZplNdtLabelBuilder
{
    private const int LabelWidthDots = 800;
    private const int LabelLengthDots = 1100;

    public static byte[] BuildNdtTagZpl(
        string ndtBatchNo,
        int millNo,
        string poNumber,
        string? pipeGrade,
        string pipeSize,
        string pipeThickness,
        string pipeLength,
        string bundleWeight,
        string pipeType,
        DateTime date,
        int pcsInBundle,
        bool isReprint,
        string? stationText = null)
    {
        var zpl = new StringBuilder();
        zpl.Append("^XA");
        zpl.AppendFormat("^PW{0}^LL{1}^LH0,0", LabelWidthDots, LabelLengthDots);
        // Larger default font for readability
        zpl.Append("^CF0,32");

        // Common formatting helpers
        var escapedBatch = Escape(ndtBatchNo);
        var escapedPo = Escape(poNumber);
        var escapedGrade = Escape(pipeGrade);
        var escapedSize = Escape(pipeSize);
        var escapedThickness = Escape(pipeThickness);
        var escapedLength = Escape(pipeLength);
        var escapedWeight = Escape(bundleWeight);
        var escapedType = Escape(pipeType);
        var escapedStation = Escape(stationText);

        var y = 40;
        var lineHeight = 32;
        var detailFont = "^CF0,28";

        // Top: Code 128 (^BC). ^BY = module width; ^BCN,h,f,g,e = normal orientation, bar height, interpretation line below/above, UCC mode.
        zpl.AppendFormat("^FO80,{0}^BY3^BCN,100,Y,N,N^FD{1}^FS", y, escapedBatch);
        y += 130;

        // Middle content – line 1: Mill, PO, Bund (NDT Batch No), centered
        zpl.Append(detailFont);
        zpl.AppendFormat("^FO80,{0}^FB640,1,0,C,0^FDMill- {1}  PO: {2}  Bund: {3}^FS", y, millNo, escapedPo, escapedBatch);
        y += lineHeight;

        // Product details on two lines so weight is not clipped/overlapped on long pipe sizes.
        var gradePart = string.IsNullOrEmpty(escapedGrade) ? "Gr- -" : $"Gr- {escapedGrade}";
        var sizePart = string.IsNullOrEmpty(escapedSize) ? "-" : escapedSize;
        var thkPart = string.IsNullOrEmpty(escapedThickness) ? "-" : escapedThickness;
        var lenPart = string.IsNullOrEmpty(escapedLength) ? "-" : escapedLength;
        var wtPart = string.IsNullOrEmpty(escapedWeight) ? "-" : escapedWeight;

        zpl.AppendFormat("^FO80,{0}^FB640,1,0,C,0^FD{1}  Size: {2}  Thk: {3}^FS", y, gradePart, sizePart, thkPart);
        y += lineHeight;
        zpl.AppendFormat("^FO80,{0}^FB640,1,0,C,0^FDLen: {1}  Wt: {2}^FS", y, lenPart, wtPart);
        y += lineHeight;

        // Optional station line (Visual / Hydrotesting / Revisual; for hydro includes Four Head vs Big).
        if (!string.IsNullOrWhiteSpace(escapedStation))
        {
            zpl.AppendFormat("^FO80,{0}^FB640,1,0,C,0^FDStation: {1}^FS", y, escapedStation);
            y += lineHeight;
        }

        // Date, pieces, type/WIP/FG and optional Reprint
        zpl.Append("^CF0,32");
        var dateText = date.ToString("dd/MM/yy");
        var typeText = string.IsNullOrEmpty(escapedType) ? "" : $"  {escapedType}";
        var reprintText = isReprint ? "  Reprint" : "";
        zpl.AppendFormat("^FO80,{0}^FB640,1,0,C,0^FDDate: {1}  Pcs. {2}{3}{4}^FS",
            y,
            dateText,
            pcsInBundle,
            typeText,
            reprintText);

        // Bottom: two stacked Code 128 barcodes with the same NDT Batch Number.
        var bottomY1 = LabelLengthDots - 280;
        var bottomY2 = bottomY1 + 120;

        zpl.AppendFormat("^FO80,{0}^BY2^BCN,80,Y,N,N^FD{1}^FS", bottomY1, escapedBatch);
        zpl.AppendFormat("^FO80,{0}^BY2^BCN,80,Y,N,N^FD{1}^FS", bottomY2, escapedBatch);

        zpl.Append("^XZ");
        return Encoding.UTF8.GetBytes(zpl.ToString());
    }

    private static string Escape(string? value)
    {
        if (string.IsNullOrEmpty(value)) return "";
        return value
            .Replace("\\", "\\\\")
            .Replace("^", "\\^")
            .Replace("~", "\\~");
    }
}
