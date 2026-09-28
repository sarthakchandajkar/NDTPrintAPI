using System.Linq;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services.MillInstanceStatus;

namespace NdtBundleService.Services;

/// <inheritdoc />
public sealed class ActivePoPerMillService : IActivePoPerMillService
{
    private readonly NdtBundleOptions _options;
    private readonly IWipBundleRunningPoProvider _wipRunningPo;
    private readonly IMillInstanceStatusStore _millStatus;
    private readonly ILogger<ActivePoPerMillService> _logger;

    public ActivePoPerMillService(
        IOptions<NdtBundleOptions> options,
        IWipBundleRunningPoProvider wipRunningPo,
        IMillInstanceStatusStore millStatus,
        ILogger<ActivePoPerMillService> logger)
    {
        _options = options.Value;
        _wipRunningPo = wipRunningPo;
        _millStatus = millStatus;
        _logger = logger;
    }

    public IReadOnlyList<string> GetInputSlitReadFolderPaths()
    {
        var list = new List<string>();
        var inbox = _options.InputSlitFolder?.Trim();
        if (!string.IsNullOrEmpty(inbox))
            list.Add(inbox);
        // Accepted is not used for ActivePo / Summary PO resolution (UNC archive hangs Shared).
        return list;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyDictionary<int, string>> GetLatestPoByMillAsync(CancellationToken cancellationToken)
    {
        // Prefer mill-published Running_Po from SQL first so Shared dashboard does not wait on a
        // UNC scan of Input Slit Accepted (that archive often hangs / times out Next.js → "Internal Server Error").
        var result = new Dictionary<int, string>();
        var waitingMills = OverlayMillPublishedRunningPo(result);

        var needSlitFallback = false;
        for (var mill = 1; mill <= 4; mill++)
        {
            if (waitingMills.Contains(mill))
                continue;
            if (!result.ContainsKey(mill))
            {
                needSlitFallback = true;
                break;
            }
        }

        if (needSlitFallback)
        {
            var fromSlit = await BuildSlitPoMapAsync(cancellationToken).ConfigureAwait(false);
            foreach (var kv in fromSlit)
            {
                if (waitingMills.Contains(kv.Key))
                    continue;
                if (!result.ContainsKey(kv.Key))
                    result[kv.Key] = kv.Value;
            }

            // Re-apply so fresh Running_Po / Waiting still beat any slit fill.
            OverlayMillPublishedRunningPo(result);
        }

        return await MergeRunningPoFromWipAsync(result, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Resolves PO per mill from Input Slit sources only (files and/or SQL). WIP bundle filenames are not used here.
    /// </summary>
    private async Task<Dictionary<int, string>> BuildSlitPoMapAsync(CancellationToken cancellationToken)
    {
        if (_options.PreferInputSlitFilesForRunningPo)
        {
            var fromFiles = await GetLatestPoPerMillFromLatestFilesAsync(cancellationToken).ConfigureAwait(false);
            if (fromFiles.Count > 0)
            {
                if (!UseDatabaseForSummary || fromFiles.Count == 4)
                    return fromFiles;

                var merged = new Dictionary<int, string>(fromFiles);
                var fromDb = await GetLatestPoPerMillFromDatabaseAsync(cancellationToken).ConfigureAwait(false);
                foreach (var kv in fromDb)
                {
                    if (!merged.ContainsKey(kv.Key))
                        merged[kv.Key] = kv.Value;
                }

                if (merged.Count > 0)
                    return merged;
            }
        }
        else if (UseDatabaseForSummary)
        {
            var fromDb = await GetLatestPoPerMillFromDatabaseAsync(cancellationToken).ConfigureAwait(false);
            if (fromDb.Count > 0)
                return fromDb;
        }

        var fromLatestFiles = await GetLatestPoPerMillFromLatestFilesAsync(cancellationToken).ConfigureAwait(false);
        if (fromLatestFiles.Count > 0)
            return fromLatestFiles;

        return await GetLatestPoPerMillFromAllFilesForwardScanAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Shared dashboard: prefer each mill's published <c>Running_Po</c> / waiting flag from
    /// <c>Mill_Instance_Status</c> (fresh telemetry only) over slit CSV / SQL.
    /// Returns mill numbers currently WaitingForNewWip so callers do not refill them from slit files.
    /// </summary>
    private HashSet<int> OverlayMillPublishedRunningPo(Dictionary<int, string> result)
    {
        var waiting = new HashSet<int>();
        if (!UseDatabaseForSummary)
            return waiting;

        try
        {
            var now = DateTimeOffset.UtcNow;
            foreach (var row in _millStatus.LoadAll())
            {
                if (row.MillNo is < 1 or > 4)
                    continue;
                if (row.LastUpdateUtc + MillInstanceStatusFreshness.StaleAfter < now)
                    continue;

                if (row.WaitingForNewWip)
                {
                    waiting.Add(row.MillNo);
                    result.Remove(row.MillNo);
                    continue;
                }

                if (!string.IsNullOrWhiteSpace(row.RunningPoNumber))
                    result[row.MillNo] = InputSlitCsvParsing.NormalizePo(row.RunningPoNumber);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to overlay mill-published Running_Po from Mill_Instance_Status.");
        }

        return waiting;
    }

    /// <summary>
    /// Prefer local WIP running PO over slit / SQL when this process has one
    /// (mill instances). Shared has no owned WIP state, so this is a no-op there and
    /// mill-published <c>Running_Po</c> overlay above remains the Shared source of truth.
    /// </summary>
    private async Task<IReadOnlyDictionary<int, string>> MergeRunningPoFromWipAsync(
        Dictionary<int, string> result,
        CancellationToken cancellationToken)
    {
        for (var millNo = 1; millNo <= 4; millNo++)
        {
            if (_wipRunningPo.IsWaitingForNewWipAfterPoEnd(millNo))
            {
                result.Remove(millNo);
                continue;
            }

            var wipPo = await _wipRunningPo.TryGetRunningPoForMillAsync(millNo, CancellationToken.None).ConfigureAwait(false);
            if (!string.IsNullOrWhiteSpace(wipPo))
                result[millNo] = InputSlitCsvParsing.NormalizePo(wipPo);
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

        // Live inbox only. Do not enumerate Input Slit Accepted — that archive is large on UNC and
        // blocked Shared Summary polls (Next.js rewrite → plain "Internal Server Error").
        var inbox = (_options.InputSlitFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(inbox) || !Directory.Exists(inbox))
            return result;

        var files = InputSlitInboxEnumeration
            .EnumerateFiles(inbox)
            .Select(p => new FileInfo(p))
            .Where(fi => SourceFileEligibility.IncludeFileUtc(fi.LastWriteTimeUtc, minUtc))
            .ToList();

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

    private async Task<Dictionary<int, string>> GetLatestPoPerMillFromAllFilesForwardScanAsync(CancellationToken cancellationToken)
    {
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
        var inbox = (_options.InputSlitFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(inbox) || !Directory.Exists(inbox))
            return new List<string>();

        // Inbox only — same reason as GetLatestPoPerMillFromLatestFilesAsync (Accepted UNC hangs).
        return InputSlitInboxEnumeration
            .EnumerateFiles(inbox)
            .Where(f => SourceFileEligibility.IncludeFileUtc(File.GetLastWriteTimeUtc(f), minUtc))
            .Select(f => new FileInfo(f))
            .OrderBy(f => f.LastWriteTimeUtc)
            .ThenBy(f => f.FullName, StringComparer.OrdinalIgnoreCase)
            .Select(f => f.FullName)
            .ToList();
    }
}
