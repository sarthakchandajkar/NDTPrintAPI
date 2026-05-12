using System.Linq;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <inheritdoc />
public sealed class ActivePoPerMillService : IActivePoPerMillService
{
    private readonly NdtBundleOptions _options;
    private readonly ILogger<ActivePoPerMillService> _logger;

    public ActivePoPerMillService(IOptions<NdtBundleOptions> options, ILogger<ActivePoPerMillService> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public IReadOnlyList<string> GetInputSlitReadFolderPaths()
    {
        var list = new List<string>();
        var inbox = _options.InputSlitFolder?.Trim();
        if (!string.IsNullOrEmpty(inbox))
            list.Add(inbox);
        var accepted = _options.InputSlitAcceptedFolder?.Trim();
        if (!string.IsNullOrEmpty(accepted))
            list.Add(accepted);
        return list;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<int, string>> GetLatestPoByMillAsync(CancellationToken cancellationToken)
    {
        var fromLatestFiles = await GetLatestPoPerMillFromLatestFilesAsync(cancellationToken).ConfigureAwait(false);
        if (fromLatestFiles.Count == 4)
            return fromLatestFiles;

        if (UseDatabaseForSummary)
        {
            var fromDb = await GetLatestPoPerMillFromDatabaseAsync(cancellationToken).ConfigureAwait(false);
            if (fromDb.Count > 0)
                return fromDb;
        }

        var result = new Dictionary<int, string>();
        var files = GetEligibleInputSlitCsvFilesOrdered();
        foreach (var fullPath in files)
        {
            await using var stream = File.OpenRead(fullPath);
            using var reader = new StreamReader(stream);

            var headerLine = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            if (headerLine is null)
                continue;

            headerLine = InputSlitCsvParsing.StripBom(headerLine);
            var headers = InputSlitCsvParsing.SplitCsvFields(headerLine);
            var poIndex = InputSlitCsvParsing.HeaderIndex(headers, "PO Number", "PO_No", "PO No");
            var millIndex = InputSlitCsvParsing.HeaderIndex(headers, "Mill No", "Mill Number");
            if (poIndex < 0 || millIndex < 0)
                continue;

            while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
            {
                if (string.IsNullOrWhiteSpace(line))
                    continue;
                var cols = InputSlitCsvParsing.SplitCsvFields(line);
                if (cols.Length == 0)
                    continue;
                string Get(int i) => i >= 0 && i < cols.Length ? cols[i].Trim() : string.Empty;
                var millRaw = Get(millIndex);
                if (!InputSlitCsvParsing.TryParseMillNo(millRaw, out var millNo))
                    continue;
                var po = Get(poIndex);
                if (string.IsNullOrWhiteSpace(po))
                    continue;
                result[millNo] = InputSlitCsvParsing.NormalizePo(po);
            }
        }

        return result;
    }

    private bool UseDatabaseForSummary =>
        _options.UseSqlServerForBundles && !string.IsNullOrWhiteSpace(_options.ConnectionString);

    private async Task<Dictionary<int, string>> GetLatestPoPerMillFromLatestFilesAsync(CancellationToken cancellationToken)
    {
        var result = new Dictionary<int, string>();
        var minUtc = SourceFileEligibility.ParseMinUtc(_options);
        const int maxFilesToScan = 300;

        var files = new List<FileInfo>();
        foreach (var folder in GetInputSlitReadFolderPaths())
        {
            if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
                continue;
            foreach (var file in InputSlitInboxEnumeration.EnumerateFiles(folder))
            {
                var fi = new FileInfo(file);
                if (SourceFileEligibility.IncludeFileUtc(fi.LastWriteTimeUtc, minUtc))
                    files.Add(fi);
            }
        }

        foreach (var fi in files
                     .OrderByDescending(f => f.LastWriteTimeUtc)
                     .ThenByDescending(f => f.FullName, StringComparer.OrdinalIgnoreCase)
                     .Take(maxFilesToScan))
        {
            if (result.Count == 4)
                break;
            cancellationToken.ThrowIfCancellationRequested();

            string[] lines;
            try
            {
                lines = await File.ReadAllLinesAsync(fi.FullName, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                continue;
            }

            if (lines.Length < 2)
                continue;

            var headerLine = InputSlitCsvParsing.StripBom(lines[0]);
            var headers = InputSlitCsvParsing.SplitCsvFields(headerLine);
            var poIndex = InputSlitCsvParsing.HeaderIndex(headers, "PO Number", "PO_No", "PO No");
            var millIndex = InputSlitCsvParsing.HeaderIndex(headers, "Mill No", "Mill Number");
            if (poIndex < 0 || millIndex < 0)
                continue;

            for (var i = lines.Length - 1; i >= 1; i--)
            {
                if (result.Count == 4)
                    break;
                var line = lines[i];
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                var cols = InputSlitCsvParsing.SplitCsvFields(line);
                if (cols.Length == 0)
                    continue;

                string Get(int idx) => idx >= 0 && idx < cols.Length ? cols[idx].Trim() : string.Empty;
                var millRaw = Get(millIndex);
                if (!InputSlitCsvParsing.TryParseMillNo(millRaw, out var millNo))
                    continue;
                if (millNo is < 1 or > 4 || result.ContainsKey(millNo))
                    continue;

                var po = Get(poIndex);
                if (string.IsNullOrWhiteSpace(po))
                    continue;

                result[millNo] = InputSlitCsvParsing.NormalizePo(po);
            }
        }

        return result;
    }

    private async Task<Dictionary<int, string>> GetLatestPoPerMillFromDatabaseAsync(CancellationToken cancellationToken)
    {
        var result = new Dictionary<int, string>();
        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
            return result;

        try
        {
            var minUtc = SourceFileEligibility.ParseMinUtc(_options);
            await using var conn = new SqlConnection(_options.ConnectionString);
            await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

            const string sql = @"
WITH Dedup AS
(
    SELECT
        Mill_No,
        PO_Number,
        ImportedAtUtc,
        Input_Slit_Row_ID,
        ROW_NUMBER() OVER (
            PARTITION BY Source_File, Source_Row_Number
            ORDER BY ImportedAtUtc DESC, Input_Slit_Row_ID DESC
        ) AS src_rn
    FROM dbo.Input_Slit_Row
    WHERE Mill_No BETWEEN 1 AND 4
      AND PO_Number IS NOT NULL
      AND LTRIM(RTRIM(PO_Number)) <> ''
      AND Source_File IS NOT NULL
      AND Source_Row_Number IS NOT NULL
      AND (@MinUtc IS NULL OR ImportedAtUtc >= @MinUtc)
),
LatestByMill AS
(
    SELECT
        Mill_No,
        PO_Number,
        ROW_NUMBER() OVER (PARTITION BY Mill_No ORDER BY ImportedAtUtc DESC, Input_Slit_Row_ID DESC) AS rn
    FROM Dedup
    WHERE src_rn = 1
)
SELECT Mill_No, PO_Number
FROM LatestByMill
WHERE rn = 1;";

            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@MinUtc", minUtc.HasValue ? (object)minUtc.Value : DBNull.Value);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var millNo = reader.IsDBNull(0) ? 0 : reader.GetInt32(0);
                var po = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
                if (millNo is >= 1 and <= 4 && !string.IsNullOrWhiteSpace(po))
                    result[millNo] = InputSlitCsvParsing.NormalizePo(po);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to read latest PO per mill from Input_Slit_Row; falling back to CSV scan.");
        }

        return result;
    }

    private List<string> GetEligibleInputSlitCsvFilesOrdered()
    {
        var minUtc = SourceFileEligibility.ParseMinUtc(_options);
        var acc = new List<string>();
        foreach (var folder in GetInputSlitReadFolderPaths())
        {
            if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
                continue;
            foreach (var file in InputSlitInboxEnumeration.EnumerateFiles(folder))
            {
                if (SourceFileEligibility.IncludeFileUtc(File.GetLastWriteTimeUtc(file), minUtc))
                    acc.Add(file);
            }
        }

        return acc
            .Select(f => new FileInfo(f))
            .OrderBy(f => f.LastWriteTimeUtc)
            .ThenBy(f => f.FullName, StringComparer.OrdinalIgnoreCase)
            .Select(f => f.FullName)
            .ToList();
    }
}
