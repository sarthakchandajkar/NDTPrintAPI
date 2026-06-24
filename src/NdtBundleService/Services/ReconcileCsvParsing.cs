namespace NdtBundleService.Services;

/// <summary>Shared CSV line parsing for reconcile and traceability sync.</summary>
public static class ReconcileCsvParsing
{
    public sealed class OutputCsvColumnIndices
    {
        public int PoNumber { get; init; } = 0;
        public int SlitNo { get; init; } = 1;
        public int NdtPipes { get; init; } = 2;
        public int NdtBatchNo { get; init; } = 9;
        public int MinColumns { get; init; } = 10;

        public bool TryGetField(IReadOnlyList<string> cols, int index, out string value)
        {
            if (index < 0 || index >= cols.Count)
            {
                value = string.Empty;
                return false;
            }

            value = cols[index].Trim();
            return true;
        }
    }

    public static OutputCsvColumnIndices ResolveOutputCsvColumns(string headerLine)
    {
        var headers = InputSlitCsvParsing.SplitCsvFields(InputSlitCsvParsing.StripBom(headerLine));
        var idxBatch = InputSlitCsvParsing.HeaderIndex(headers, "NDT Batch No");
        var idxNd = InputSlitCsvParsing.HeaderIndex(headers, "NDT Pipes");
        var idxPo = InputSlitCsvParsing.HeaderIndex(headers, "PO Number", "PO_No", "PO No");
        var idxSlit = InputSlitCsvParsing.HeaderIndex(headers, "Slit No");
        if (idxBatch < 0 || idxNd < 0)
        {
            return new OutputCsvColumnIndices();
        }

        return new OutputCsvColumnIndices
        {
            PoNumber = idxPo >= 0 ? idxPo : 0,
            SlitNo = idxSlit >= 0 ? idxSlit : 1,
            NdtPipes = idxNd,
            NdtBatchNo = idxBatch,
            MinColumns = Math.Max(10, idxBatch + 1)
        };
    }

    public static string NormalizeSlitKey(string? raw) =>
        string.IsNullOrWhiteSpace(raw) ? "—" : raw.Trim();

    public static bool SlitKeysMatch(string? cell, string? targetSlit) =>
        string.Equals(NormalizeSlitKey(cell), NormalizeSlitKey(targetSlit), StringComparison.OrdinalIgnoreCase);

    public static string ReplaceFieldAtIndex(string line, int fieldIndex, string newCellValue)
    {
        if (string.IsNullOrEmpty(line) || fieldIndex < 0)
            return line;

        var fields = SplitCsvLine(line);
        if (fieldIndex >= fields.Count)
            return line;

        fields[fieldIndex] = newCellValue ?? string.Empty;
        return JoinCsvLine(fields);
    }

    public static List<string> SplitCsvLine(string line)
    {
        var result = new List<string>();
        var current = new System.Text.StringBuilder();
        var inQuotes = false;
        for (var i = 0; i < line.Length; i++)
        {
            var c = line[i];
            if (c == '"')
            {
                inQuotes = !inQuotes;
                continue;
            }

            if (c == ',' && !inQuotes)
            {
                result.Add(current.ToString().Trim());
                current.Clear();
                continue;
            }

            current.Append(c);
        }

        result.Add(current.ToString().Trim());
        return result;
    }

    private static string JoinCsvLine(IReadOnlyList<string> fields)
    {
        if (fields.Count == 0)
            return string.Empty;

        var parts = new string[fields.Count];
        for (var i = 0; i < fields.Count; i++)
            parts[i] = EscapeCsvField(fields[i] ?? string.Empty);
        return string.Join(",", parts);
    }

    private static string EscapeCsvField(string s)
    {
        if (s.Length == 0)
            return s;
        if (s.IndexOfAny(new[] { ',', '"', '\r', '\n' }) < 0)
            return s;
        return "\"" + s.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
    }
}
