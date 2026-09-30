using System.Text.Json;

namespace NdtBundleService.Models;

/// <summary>One exact Excel CSV data row preserved for audit (all columns).</summary>
public sealed class CsvFolderAuditRow
{
    public int SourceRowNumber { get; init; }
    public string HeaderLine { get; init; } = string.Empty;
    public string RowLine { get; init; } = string.Empty;
    public string ColumnsJson { get; init; } = string.Empty;
}

/// <summary>Parse outcome: full audit replicas plus optional normalized lookup rows.</summary>
public sealed class CsvFolderFileParseResult<TLookup>
{
    public string HeaderLine { get; init; } = string.Empty;
    public IReadOnlyList<CsvFolderAuditRow> AuditRows { get; init; } = Array.Empty<CsvFolderAuditRow>();
    public IReadOnlyList<TLookup> LookupRows { get; init; } = Array.Empty<TLookup>();
}

internal static class CsvFolderAuditRowFactory
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = false
    };

    public static CsvFolderAuditRow Create(string headerLine, string[] headers, string rowLine, string[] cols, int sourceRowNumber)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < headers.Length; i++)
        {
            var name = (headers[i] ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(name))
                name = $"Column_{i + 1}";

            // Preserve duplicate Excel headers by suffixing.
            var key = name;
            var n = 2;
            while (map.ContainsKey(key))
            {
                key = $"{name}__{n}";
                n++;
            }

            map[key] = i < cols.Length ? (cols[i] ?? string.Empty).Trim() : string.Empty;
        }

        return new CsvFolderAuditRow
        {
            SourceRowNumber = sourceRowNumber,
            HeaderLine = headerLine,
            RowLine = rowLine,
            ColumnsJson = JsonSerializer.Serialize(map, JsonOptions)
        };
    }
}
