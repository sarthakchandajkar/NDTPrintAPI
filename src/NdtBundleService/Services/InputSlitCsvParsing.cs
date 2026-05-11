using System.Globalization;
using System.Text;

namespace NdtBundleService.Services;

/// <summary>
/// Shared parsing for SAP input slit CSV lines (quoted fields, BOM, column aliases).
/// </summary>
public static class InputSlitCsvParsing
{
    public static string StripBom(string line)
    {
        if (string.IsNullOrEmpty(line))
            return line;
        return line[0] == '\ufeff' ? line[1..] : line;
    }

    /// <summary>Splits a line on commas outside of double quotes; supports <c>""</c> escapes inside quotes.</summary>
    public static string[] SplitCsvFields(string line)
    {
        var result = new List<string>();
        var sb = new StringBuilder();
        var inQuotes = false;
        for (var i = 0; i < line.Length; i++)
        {
            var c = line[i];
            if (c == '"')
            {
                if (inQuotes && i + 1 < line.Length && line[i + 1] == '"')
                {
                    sb.Append('"');
                    i++;
                    continue;
                }

                inQuotes = !inQuotes;
                continue;
            }

            if (c == ',' && !inQuotes)
            {
                result.Add(sb.ToString().Trim());
                sb.Clear();
                continue;
            }

            sb.Append(c);
        }

        result.Add(sb.ToString().Trim());
        return result.ToArray();
    }

    public static int HeaderIndex(IReadOnlyList<string> headers, params string[] names)
    {
        foreach (var name in names)
        {
            for (var i = 0; i < headers.Count; i++)
            {
                var h = StripBom(headers[i]).Trim();
                if (string.Equals(h, name, StringComparison.OrdinalIgnoreCase))
                    return i;
            }
        }

        return -1;
    }

    /// <summary>Mill from WIP or slit: 1–4, 01, Mill-2, or <c>2.0</c>.</summary>
    public static bool TryParseMillNo(string raw, out int millNo)
    {
        millNo = 0;
        if (string.IsNullOrWhiteSpace(raw))
            return false;
        raw = raw.Trim();
        if (raw.StartsWith("Mill-", StringComparison.OrdinalIgnoreCase))
        {
            var rest = raw.AsSpan("Mill-".Length).Trim();
            return int.TryParse(rest, NumberStyles.Integer, CultureInfo.InvariantCulture, out millNo)
                   && millNo is >= 1 and <= 4;
        }

        if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out millNo) && millNo is >= 1 and <= 4)
            return true;

        if (double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
        {
            var r = (int)Math.Round(d);
            if (Math.Abs(d - r) < 1e-6 && r is >= 1 and <= 4)
            {
                millNo = r;
                return true;
            }
        }

        return false;
    }

    /// <summary>Compares PO cells that may differ by formatting (Excel <c>1000057028.0</c> vs <c>1000057028</c>).</summary>
    public static bool PoEquals(string rowPo, string requestedPo)
    {
        var a = NormalizePo(rowPo);
        var b = NormalizePo(requestedPo);
        return string.Equals(a, b, StringComparison.OrdinalIgnoreCase);
    }

    public static string NormalizePo(string s)
    {
        s = s.Trim();
        if (s.Length == 0)
            return s;

        if (decimal.TryParse(s, NumberStyles.Number, CultureInfo.InvariantCulture, out var d))
        {
            if (d == Math.Truncate(d) && d is >= 1 and <= 99999999999m)
                return ((long)d).ToString(CultureInfo.InvariantCulture);
        }

        return s;
    }

    public static bool TryParseIntFlexible(string raw, out int value)
    {
        raw = raw.Trim();
        if (int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out value))
            return true;
        if (double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var dbl))
        {
            var r = (int)Math.Round(dbl);
            if (Math.Abs(dbl - r) < 1e-6)
            {
                value = r;
                return true;
            }
        }

        return false;
    }

    /// <summary>Replaces one field (0-based) and re-encodes the line as CSV.</summary>
    public static string ReplaceFieldAtIndex(string line, int fieldIndex, string newCellValue)
    {
        if (string.IsNullOrEmpty(line) || fieldIndex < 0)
            return line;
        var fields = SplitCsvFields(line);
        if (fieldIndex >= fields.Length)
            return line;
        fields[fieldIndex] = newCellValue ?? string.Empty;
        return JoinCsvLine(fields);
    }

    /// <summary>Joins fields with commas; quotes fields that contain delimiters.</summary>
    public static string JoinCsvLine(IReadOnlyList<string> fields)
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
