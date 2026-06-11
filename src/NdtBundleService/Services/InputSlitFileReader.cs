using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>Reads input slit CSV files (shared by live worker and rebuild).</summary>
public static class InputSlitFileReader
{
    public sealed record SlitFileContent(
        string SourcePath,
        DateTime SourceLastWriteUtc,
        string HeaderLine,
        IReadOnlyList<(string RawLine, InputSlitRecord? Record)> Rows,
        int NdtColumnIndex);

    public static async Task<SlitFileContent?> ReadAsync(string path, CancellationToken cancellationToken)
    {
        if (!File.Exists(path))
            return null;

        var rows = new List<(string RawLine, InputSlitRecord?)>();
        DateTime lwUtc;
        try
        {
            lwUtc = File.GetLastWriteTimeUtc(path);
        }
        catch
        {
            lwUtc = DateTime.UtcNow;
        }

        await using var stream = File.OpenRead(path);
        using var reader = new StreamReader(stream);

        var headerRaw = await reader.ReadLineAsync(cancellationToken);
        if (headerRaw is null)
            return null;

        var headerLine = InputSlitCsvParsing.StripBom(headerRaw);
        var headers = InputSlitCsvParsing.SplitCsvFields(headerLine);

        var poIndex = InputSlitCsvParsing.HeaderIndex(headers, "PO Number", "PO_No", "PO No");
        var slitIndex = InputSlitCsvParsing.HeaderIndex(headers, "Slit No");
        var ndtIndex = InputSlitCsvParsing.HeaderIndex(headers, "NDT Pipes");
        var rejectedIndex = InputSlitCsvParsing.HeaderIndex(headers, "Rejected P");
        var startIndex = InputSlitCsvParsing.HeaderIndex(headers, "Slit Start Time");
        var finishIndex = InputSlitCsvParsing.HeaderIndex(headers, "Slit Finish Time");
        var millIndex = InputSlitCsvParsing.HeaderIndex(headers, "Mill No", "Mill Number");
        var shortIndex = InputSlitCsvParsing.HeaderIndex(headers, "NDT Short Length Pipe");
        var rejShortIndex = InputSlitCsvParsing.HeaderIndex(headers, "Rejected Short Length Pipe");

        while (!reader.EndOfStream)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var line = await reader.ReadLineAsync(cancellationToken);
            if (string.IsNullOrWhiteSpace(line))
                continue;

            var cols = InputSlitCsvParsing.SplitCsvFields(line);
            if (cols.Length == 0)
                continue;

            var poRaw = GetString(cols, poIndex);
            var poNumber = string.IsNullOrWhiteSpace(poRaw) ? string.Empty : InputSlitCsvParsing.NormalizePo(poRaw);
            var millParsed = GetMillNo(cols, millIndex);

            var record = new InputSlitRecord
            {
                PoNumber = poNumber,
                SlitNo = GetString(cols, slitIndex),
                NdtPipes = GetIntFlexible(cols, ndtIndex),
                RejectedPipes = GetIntFlexible(cols, rejectedIndex),
                SlitStartTime = GetDateTime(cols, startIndex),
                SlitFinishTime = GetDateTime(cols, finishIndex),
                MillNo = millParsed,
                NdtShortLengthPipe = GetString(cols, shortIndex),
                RejectedShortLengthPipe = GetString(cols, rejShortIndex)
            };

            rows.Add((line, record));
        }

        return new SlitFileContent(path, lwUtc, headerLine, rows, ndtIndex);
    }

    public static DateTime? GetEarliestSlitTime(SlitFileContent file)
    {
        DateTime? earliest = null;
        foreach (var (_, record) in file.Rows)
        {
            if (record is null)
                continue;
            var t = record.SlitFinishTime ?? record.SlitStartTime;
            if (!t.HasValue)
                continue;
            if (!earliest.HasValue || t.Value < earliest.Value)
                earliest = t.Value;
        }

        return earliest ?? file.SourceLastWriteUtc;
    }

    private static string GetString(string[] cols, int index) =>
        index >= 0 && index < cols.Length ? cols[index].Trim() : string.Empty;

    private static int GetIntFlexible(string[] cols, int index)
    {
        if (index < 0 || index >= cols.Length)
            return 0;
        return InputSlitCsvParsing.TryParseIntFlexible(cols[index], out var v) ? v : 0;
    }

    private static int GetMillNo(string[] cols, int index)
    {
        if (index < 0 || index >= cols.Length)
            return 1;
        return InputSlitCsvParsing.TryParseMillNo(cols[index], out var m) ? m : 1;
    }

    private static DateTime? GetDateTime(string[] cols, int index)
    {
        if (index < 0 || index >= cols.Length)
            return null;
        var raw = cols[index].Trim();
        if (string.IsNullOrEmpty(raw))
            return null;
        return InputSlitCsvParsing.TryParseSlitDateTime(raw, out var dt) ? dt : null;
    }
}
