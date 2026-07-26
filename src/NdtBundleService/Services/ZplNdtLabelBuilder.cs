using System.Text;
using NdtBundleService.Configuration;

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
/// - Bottom: one or two stacked Code 128 barcodes with the same NDT Batch Number.
/// </summary>
public static class ZplNdtLabelBuilder
{
    /// <summary>203 dpi Honeywell PD45S: ~8 dots/mm.</summary>
    public const int DotsPerMm = 8;

    public readonly record struct NdtTagLabelSize(int WidthMm, int LengthMm)
    {
        public int WidthDots => Math.Max(DotsPerMm, WidthMm * DotsPerMm);
        public int LengthDots => Math.Max(DotsPerMm, LengthMm * DotsPerMm);

        public bool IsSquare => LengthMm >= 100;

        public static NdtTagLabelSize FromOptions(NdtBundleOptions options, int millNo = 0)
        {
            var widthMm = options.NdtTagLabelWidthMm;
            var lengthMm = options.NdtTagLabelLengthMm;
            if (millNo >= 1
                && millNo <= 4
                && options.NdtTagLabelLengthMmByMill.TryGetValue(millNo.ToString(), out var overrideLength)
                && overrideLength > 0)
            {
                lengthMm = overrideLength;
            }

            return new NdtTagLabelSize(widthMm, lengthMm);
        }
    }

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
        string? stationText = null,
        NdtTagLabelSize? labelSize = null)
    {
        var size = labelSize ?? NdtTagLabelSize.FromOptions(new NdtBundleOptions());
        return size.IsSquare
            ? BuildSquareTagZpl(
                ndtBatchNo,
                millNo,
                poNumber,
                pipeGrade,
                pipeSize,
                pipeThickness,
                pipeLength,
                bundleWeight,
                pipeType,
                date,
                pcsInBundle,
                isReprint,
                stationText,
                size)
            : BuildCompactTagZpl(
                ndtBatchNo,
                millNo,
                poNumber,
                pipeGrade,
                pipeSize,
                pipeThickness,
                pipeLength,
                bundleWeight,
                pipeType,
                date,
                pcsInBundle,
                isReprint,
                stationText,
                size);
    }

    private static byte[] BuildCompactTagZpl(
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
        string? stationText,
        NdtTagLabelSize size)
    {
        var zpl = new StringBuilder();
        zpl.Append("^XA");
        zpl.AppendFormat("^PW{0}^LL{1}^LH0,0", size.WidthDots, size.LengthDots);

        var escapedBatch = Escape(ndtBatchNo);
        var escapedPo = Escape(poNumber);
        var escapedGrade = Escape(pipeGrade);
        var escapedSize = Escape(pipeSize);
        var escapedThickness = Escape(pipeThickness);
        var escapedLength = Escape(pipeLength);
        var escapedWeight = Escape(bundleWeight);
        var escapedType = Escape(pipeType);
        var escapedStation = Escape(stationText);

        const int left = 60;
        var fieldWidth = size.WidthDots - (left * 2);

        var y = 12;
        zpl.AppendFormat("^FO{0},{1}^BY2^BCN,55,Y,N,N^FD{2}^FS", left, y, escapedBatch);
        y += 72;

        zpl.Append("^CF0,22");
        zpl.AppendFormat("^FO{0},{1}^FB{2},1,0,C,0^FDMill- {3}  PO: {4}  Bund: {5}^FS",
            left, y, fieldWidth, millNo, escapedPo, escapedBatch);
        y += 24;

        var gradePart = string.IsNullOrEmpty(escapedGrade) ? "Gr- -" : $"Gr- {escapedGrade}";
        var sizePart = string.IsNullOrEmpty(escapedSize) ? "-" : escapedSize;
        var thkPart = string.IsNullOrEmpty(escapedThickness) ? "-" : escapedThickness;
        var lenPart = string.IsNullOrEmpty(escapedLength) ? "-" : escapedLength;
        var wtPart = string.IsNullOrEmpty(escapedWeight) ? "-" : escapedWeight;

        zpl.AppendFormat("^FO{0},{1}^FB{2},1,0,C,0^FD{3}  Size: {4}  Thk: {5}^FS",
            left, y, fieldWidth, gradePart, sizePart, thkPart);
        y += 24;
        zpl.AppendFormat("^FO{0},{1}^FB{2},1,0,C,0^FDLen: {3}  Wt: {4}^FS", left, y, fieldWidth, lenPart, wtPart);
        y += 24;

        if (!string.IsNullOrWhiteSpace(escapedStation))
        {
            zpl.AppendFormat("^FO{0},{1}^FB{2},1,0,C,0^FDStation: {3}^FS", left, y, fieldWidth, escapedStation);
            y += 24;
        }

        var dateText = date.ToString("dd/MM/yy");
        var typeText = string.IsNullOrEmpty(escapedType) ? "" : $"  {escapedType}";
        var reprintText = isReprint ? "  Reprint" : "";
        zpl.AppendFormat("^FO{0},{1}^FB{2},1,0,C,0^FDDate: {3}  Pcs. {4}{5}{6}^FS",
            left, y, fieldWidth, dateText, pcsInBundle, typeText, reprintText);

        var bottomY = size.LengthDots - 78;
        zpl.AppendFormat("^FO{0},{1}^BY2^BCN,55,Y,N,N^FD{2}^FS", left, bottomY, escapedBatch);

        zpl.Append("^XZ");
        return Encoding.UTF8.GetBytes(zpl.ToString());
    }

    private static byte[] BuildSquareTagZpl(
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
        string? stationText,
        NdtTagLabelSize size)
    {
        var zpl = new StringBuilder();
        zpl.Append("^XA");
        zpl.AppendFormat("^PW{0}^LL{1}^LH0,0", size.WidthDots, size.LengthDots);
        zpl.Append("^CF0,34");

        var escapedBatch = Escape(ndtBatchNo);
        var escapedPo = Escape(poNumber);
        var escapedGrade = Escape(pipeGrade);
        var escapedSize = Escape(pipeSize);
        var escapedThickness = Escape(pipeThickness);
        var escapedLength = Escape(pipeLength);
        var escapedWeight = Escape(bundleWeight);
        var escapedType = Escape(pipeType);
        var escapedStation = Escape(stationText);

        const int left = 60;
        var fieldWidth = size.WidthDots - (left * 2);

        var y = 36;
        zpl.AppendFormat("^FO{0},{1}^BY3^BCN,110,Y,N,N^FD{2}^FS", left, y, escapedBatch);
        y += 148;

        zpl.Append("^CF0,30");
        zpl.AppendFormat("^FO{0},{1}^FB{2},1,0,C,0^FDMill- {3}  PO: {4}  Bund: {5}^FS",
            left, y, fieldWidth, millNo, escapedPo, escapedBatch);
        y += 38;

        var gradePart = string.IsNullOrEmpty(escapedGrade) ? "Gr- -" : $"Gr- {escapedGrade}";
        var sizePart = string.IsNullOrEmpty(escapedSize) ? "-" : escapedSize;
        var thkPart = string.IsNullOrEmpty(escapedThickness) ? "-" : escapedThickness;
        var lenPart = string.IsNullOrEmpty(escapedLength) ? "-" : escapedLength;
        var wtPart = string.IsNullOrEmpty(escapedWeight) ? "-" : escapedWeight;

        zpl.Append("^CF0,28");
        zpl.AppendFormat("^FO{0},{1}^FB{2},1,0,C,0^FD{3}  Size: {4}  Thk: {5}^FS",
            left, y, fieldWidth, gradePart, sizePart, thkPart);
        y += 36;
        zpl.AppendFormat("^FO{0},{1}^FB{2},1,0,C,0^FDLen: {3}  Wt: {4}^FS", left, y, fieldWidth, lenPart, wtPart);
        y += 36;

        if (!string.IsNullOrWhiteSpace(escapedStation))
        {
            zpl.AppendFormat("^FO{0},{1}^FB{2},1,0,C,0^FDStation: {3}^FS", left, y, fieldWidth, escapedStation);
            y += 36;
        }

        zpl.Append("^CF0,32");
        var dateText = date.ToString("dd/MM/yy");
        var typeText = string.IsNullOrEmpty(escapedType) ? "" : $"  {escapedType}";
        var reprintText = isReprint ? "  Reprint" : "";
        zpl.AppendFormat("^FO{0},{1}^FB{2},1,0,C,0^FDDate: {3}  Pcs. {4}{5}{6}^FS",
            left, y, fieldWidth, dateText, pcsInBundle, typeText, reprintText);

        var bottomY1 = size.LengthDots - 250;
        var bottomY2 = bottomY1 + 118;
        zpl.AppendFormat("^FO{0},{1}^BY2^BCN,88,Y,N,N^FD{2}^FS", left, bottomY1, escapedBatch);
        zpl.AppendFormat("^FO{0},{1}^BY2^BCN,88,Y,N,N^FD{2}^FS", left, bottomY2, escapedBatch);

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
