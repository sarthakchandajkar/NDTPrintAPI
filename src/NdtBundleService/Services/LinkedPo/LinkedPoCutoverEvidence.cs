using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services.LinkedPo;

public sealed record LinkedPoEvidence(string PriorPo, string NewPo, string BaseSlitNo, string Detail);

/// <summary>Detects linked-PO cutovers from SQL slit history and Input Slit filenames.</summary>
public sealed class LinkedPoCutoverEvidence
{
    private readonly IOptions<NdtBundleOptions> _options;
    private readonly ILogger<LinkedPoCutoverEvidence> _logger;

    public LinkedPoCutoverEvidence(
        IOptions<NdtBundleOptions> options,
        ILogger<LinkedPoCutoverEvidence> logger)
    {
        _options = options;
        _logger = logger;
    }

    public async Task<LinkedPoEvidence?> TryFindAsync(
        int millNo,
        string endedPo,
        string newPo,
        string? slitNoHint,
        CancellationToken cancellationToken)
    {
        var ended = InputSlitCsvParsing.NormalizePo(endedPo);
        var next = InputSlitCsvParsing.NormalizePo(newPo);
        if (millNo is < 1 or > 4 || string.IsNullOrWhiteSpace(ended) || string.IsNullOrWhiteSpace(next))
            return null;
        if (InputSlitCsvParsing.PoEquals(ended, next))
            return null;

        if (LinkedPoSlitIdentity.HasLinkedSuffix(slitNoHint))
        {
            return new LinkedPoEvidence(
                ended,
                next,
                LinkedPoSlitIdentity.ToBaseSlitNo(slitNoHint),
                "slit hint has _91 linked suffix");
        }

        var fromSql = await TryFindFromSqlAsync(millNo, ended, next, slitNoHint, cancellationToken)
            .ConfigureAwait(false);
        if (fromSql is not null)
            return fromSql;

        return TryFindFromInputSlitFolder(millNo, ended, next, slitNoHint);
    }

    private async Task<LinkedPoEvidence?> TryFindFromSqlAsync(
        int millNo,
        string endedPo,
        string newPo,
        string? slitNoHint,
        CancellationToken cancellationToken)
    {
        if (!_options.Value.UseSqlServerForBundles)
            return null;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(_options.Value);
            await SqlTraceabilityConnection
                .OpenAsync(conn, _logger, "LinkedPo evidence", cancellationToken)
                .ConfigureAwait(false);

            var baseHint = LinkedPoSlitIdentity.ToBaseSlitNo(slitNoHint);
            await using var cmd = new SqlCommand(@"
SELECT TOP 1
    LTRIM(RTRIM(prior.Slit_No)) AS PriorSlit,
    LTRIM(RTRIM(prior.PO_Number)) AS PriorPo
FROM dbo.Output_Slit_Row prior
WHERE prior.Mill_No = @Mill
  AND (prior.PO_Number = @EndedPo OR prior.PO_Number = @EndedNorm)
  AND (
        @BaseHint = N''
        OR LTRIM(RTRIM(prior.Slit_No)) = @BaseHint
        OR LTRIM(RTRIM(prior.Slit_No)) = @BaseHint + N'_91'
        OR LTRIM(RTRIM(prior.Slit_No)) LIKE @BaseHint + N'[_]91'
      )
  AND EXISTS (
        SELECT 1
        FROM dbo.Output_Slit_Row nxt
        WHERE nxt.Mill_No = @Mill
          AND (nxt.PO_Number = @NewPo OR nxt.PO_Number = @NewNorm)
          AND (
                REPLACE(LTRIM(RTRIM(nxt.Slit_No)), N'_91', N'') = REPLACE(LTRIM(RTRIM(prior.Slit_No)), N'_91', N'')
             OR LTRIM(RTRIM(nxt.Slit_No)) = LTRIM(RTRIM(prior.Slit_No))
             OR LTRIM(RTRIM(nxt.Slit_No)) = LTRIM(RTRIM(prior.Slit_No)) + N'_91'
             OR LTRIM(RTRIM(prior.Slit_No)) = LTRIM(RTRIM(nxt.Slit_No)) + N'_91'
          )
      )
ORDER BY prior.Slit_Finish_Time DESC;", conn);
            cmd.Parameters.AddWithValue("@Mill", millNo);
            cmd.Parameters.AddWithValue("@EndedPo", endedPo);
            cmd.Parameters.AddWithValue("@EndedNorm", endedPo);
            cmd.Parameters.AddWithValue("@NewPo", newPo);
            cmd.Parameters.AddWithValue("@NewNorm", newPo);
            cmd.Parameters.AddWithValue("@BaseHint", string.IsNullOrWhiteSpace(baseHint) ? string.Empty : baseHint);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var slit = reader.IsDBNull(0) ? string.Empty : reader.GetString(0).Trim();
                return new LinkedPoEvidence(
                    endedPo,
                    newPo,
                    LinkedPoSlitIdentity.ToBaseSlitNo(slit),
                    "Output_Slit_Row same base slit under both POs");
            }

            // Prior half on ended PO + new-PO row in Input_Slit_Row (second half may not be stamped yet).
            await using var cmd2 = new SqlCommand(@"
SELECT TOP 1
    LTRIM(RTRIM(prior.Slit_No)) AS PriorSlit
FROM dbo.Output_Slit_Row prior
WHERE prior.Mill_No = @Mill
  AND (prior.PO_Number = @EndedPo OR prior.PO_Number = @EndedNorm)
  AND (
        @BaseHint = N''
        OR LTRIM(RTRIM(prior.Slit_No)) = @BaseHint
        OR LTRIM(RTRIM(prior.Slit_No)) = @BaseHint + N'_91'
      )
  AND EXISTS (
        SELECT 1
        FROM dbo.Input_Slit_Row nxt
        WHERE nxt.Mill_No = @Mill
          AND (nxt.PO_Number = @NewPo OR nxt.PO_Number = @NewNorm)
          AND REPLACE(LTRIM(RTRIM(nxt.Slit_No)), N'_91', N'') = REPLACE(LTRIM(RTRIM(prior.Slit_No)), N'_91', N'')
      )
ORDER BY prior.Slit_Finish_Time DESC;", conn);
            cmd2.Parameters.AddWithValue("@Mill", millNo);
            cmd2.Parameters.AddWithValue("@EndedPo", endedPo);
            cmd2.Parameters.AddWithValue("@EndedNorm", endedPo);
            cmd2.Parameters.AddWithValue("@NewPo", newPo);
            cmd2.Parameters.AddWithValue("@NewNorm", newPo);
            cmd2.Parameters.AddWithValue("@BaseHint", string.IsNullOrWhiteSpace(baseHint) ? string.Empty : baseHint);

            var scalar = await cmd2.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (scalar is string s && !string.IsNullOrWhiteSpace(s))
            {
                return new LinkedPoEvidence(
                    endedPo,
                    newPo,
                    LinkedPoSlitIdentity.ToBaseSlitNo(s),
                    "Output_Slit_Row prior + Input_Slit_Row new PO same base slit");
            }

            // Open incomplete fill on ended PO + any _91 / dual-file hint already known.
            if (!string.IsNullOrWhiteSpace(baseHint))
            {
                await using var cmd3 = new SqlCommand(@"
SELECT TOP 1 LTRIM(RTRIM(Slit_No))
FROM dbo.Output_Slit_Row
WHERE Mill_No = @Mill
  AND (PO_Number = @EndedPo OR PO_Number = @EndedNorm)
  AND (
        LTRIM(RTRIM(Slit_No)) = @BaseHint
     OR LTRIM(RTRIM(Slit_No)) = @BaseHint + N'_91'
     OR LTRIM(RTRIM(Slit_No)) LIKE @BaseHint + N'[_]91'
  )
ORDER BY Slit_Finish_Time DESC;", conn);
                cmd3.Parameters.AddWithValue("@Mill", millNo);
                cmd3.Parameters.AddWithValue("@EndedPo", endedPo);
                cmd3.Parameters.AddWithValue("@EndedNorm", endedPo);
                cmd3.Parameters.AddWithValue("@BaseHint", baseHint);
                var priorSlit = await cmd3.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                if (priorSlit is string ps && !string.IsNullOrWhiteSpace(ps))
                {
                    return new LinkedPoEvidence(
                        endedPo,
                        newPo,
                        LinkedPoSlitIdentity.ToBaseSlitNo(ps),
                        "prior Output_Slit_Row matches slit hint base (second-half ingest)");
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Linked-PO SQL evidence lookup failed for Mill {Mill}.", millNo);
        }

        return null;
    }

    private LinkedPoEvidence? TryFindFromInputSlitFolder(
        int millNo,
        string endedPo,
        string newPo,
        string? slitNoHint)
    {
        var o = _options.Value;
        var folders = new[]
        {
            (o.InputSlitFolder ?? string.Empty).Trim(),
            (o.InputSlitAcceptedFolder ?? string.Empty).Trim()
        };

        var baseHint = LinkedPoSlitIdentity.ToBaseSlitNo(slitNoHint);
        var endedBases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var newBases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var folder in folders)
        {
            if (folder.Length == 0 || !Directory.Exists(folder))
                continue;

            string[] files;
            try
            {
                files = Directory.GetFiles(folder, "*.csv");
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Linked-PO folder scan failed for {Folder}.", folder);
                continue;
            }

            foreach (var path in files)
            {
                if (!LinkedPoSlitIdentity.TryParseInputSlitFileName(path, out var slit, out _, out var po))
                    continue;

                var bas = LinkedPoSlitIdentity.ToBaseSlitNo(slit);
                if (bas.Length == 0)
                    continue;
                if (!string.IsNullOrWhiteSpace(baseHint) &&
                    !string.Equals(bas, baseHint, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (InputSlitCsvParsing.PoEquals(po, endedPo))
                    endedBases.Add(bas);
                else if (InputSlitCsvParsing.PoEquals(po, newPo))
                    newBases.Add(bas);
            }
        }

        foreach (var b in endedBases)
        {
            if (newBases.Contains(b))
            {
                return new LinkedPoEvidence(
                    endedPo,
                    newPo,
                    b,
                    "Input Slit folder dual files same base slit different PO");
            }
        }

        return null;
    }

    /// <summary>
    /// When an inbound slit for <paramref name="newPo"/> shares a base id with an Output_Slit_Row
    /// under a different PO on the same mill, returns that prior PO (linked second half).
    /// </summary>
    public async Task<LinkedPoEvidence?> TryFindPriorPoForIncomingSlitAsync(
        int millNo,
        string newPo,
        string? slitNo,
        CancellationToken cancellationToken)
    {
        var next = InputSlitCsvParsing.NormalizePo(newPo);
        var baseSlit = LinkedPoSlitIdentity.ToBaseSlitNo(slitNo);
        if (millNo is < 1 or > 4 || string.IsNullOrWhiteSpace(next) || baseSlit.Length == 0)
            return null;

        if (!_options.Value.UseSqlServerForBundles)
            return null;

        try
        {
            await using var conn = SqlTraceabilityConnection.Create(_options.Value);
            await SqlTraceabilityConnection
                .OpenAsync(conn, _logger, "LinkedPo prior slit", cancellationToken)
                .ConfigureAwait(false);

            await using var cmd = new SqlCommand(@"
SELECT TOP 1 LTRIM(RTRIM(PO_Number)) AS PriorPo, LTRIM(RTRIM(Slit_No)) AS Slit
FROM dbo.Output_Slit_Row
WHERE Mill_No = @Mill
  AND PO_Number <> @NewPo AND PO_Number <> @NewNorm
  AND (
        LTRIM(RTRIM(Slit_No)) = @Base
     OR LTRIM(RTRIM(Slit_No)) = @Base + N'_91'
     OR REPLACE(LTRIM(RTRIM(Slit_No)), N'_91', N'') = @Base
  )
ORDER BY Slit_Finish_Time DESC;", conn);
            cmd.Parameters.AddWithValue("@Mill", millNo);
            cmd.Parameters.AddWithValue("@NewPo", next);
            cmd.Parameters.AddWithValue("@NewNorm", next);
            cmd.Parameters.AddWithValue("@Base", baseSlit);

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                return null;

            var priorPo = reader.IsDBNull(0) ? string.Empty : InputSlitCsvParsing.NormalizePo(reader.GetString(0));
            var slit = reader.IsDBNull(1) ? baseSlit : reader.GetString(1).Trim();
            if (string.IsNullOrWhiteSpace(priorPo))
                return null;

            return new LinkedPoEvidence(
                priorPo,
                next,
                LinkedPoSlitIdentity.ToBaseSlitNo(slit),
                LinkedPoSlitIdentity.HasLinkedSuffix(slitNo)
                    ? "inbound _91 slit matches prior Output_Slit_Row"
                    : "inbound slit base matches prior Output_Slit_Row under other PO");
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Linked-PO prior-slit lookup failed for Mill {Mill}.", millNo);
            return null;
        }
    }
}
