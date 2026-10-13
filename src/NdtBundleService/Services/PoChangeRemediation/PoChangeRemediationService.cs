using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services.PoChangeRemediation;

public interface IPoChangeRemediationService
{
    Task<PoChangeRemediationMillStatusResult> GetMillStatusAsync(
        int millNo,
        CancellationToken cancellationToken);

    Task<PoChangeRemediationPreviewResult> PreviewAsync(
        PoChangeRemediationRequest request,
        CancellationToken cancellationToken);

    Task<PoChangeRemediationCommitResult> CommitAsync(
        PoChangeRemediationRequest request,
        CancellationToken cancellationToken);
}

public sealed class PoChangeRemediationMillStatusResult
{
    public bool Ok { get; set; }
    public List<string> Errors { get; set; } = new();
    public List<string> Warnings { get; set; } = new();
    public int MillNo { get; set; }
    public int CurrentMillSequence { get; set; }
    public List<PoChangeRemediationFormingCandidate> FormingCandidates { get; set; } = new();
    public string? SuggestedKeepBundleNo { get; set; }
}

public sealed class PoChangeRemediationFormingCandidate
{
    public string BundleNo { get; set; } = string.Empty;
    public string PoNumber { get; set; } = string.Empty;
    public string? CsvFillState { get; set; }
    public string? PrintStatus { get; set; }
    public string? CloseSource { get; set; }
    public int TargetNdtPcs { get; set; }
    public int CsvFilled { get; set; }
    public int SlitSum { get; set; }
    public List<PoChangeRemediationSlitRow> Slits { get; set; } = new();
}

public sealed class PoChangeRemediationPreviewResult
{
    public bool Ok { get; set; }
    public List<string> Errors { get; set; } = new();
    public List<string> Warnings { get; set; } = new();
    public int MillNo { get; set; }
    public int CurrentMillSequence { get; set; }
    public int ProposedMillSequence { get; set; }
    public List<PoChangeRemediationBundlePlan> FinishedPlans { get; set; } = new();
    public PoChangeRemediationBundlePlan? FormingPlan { get; set; }
    public List<PoChangeRemediationOrphanPlan> OrphansToVoid { get; set; } = new();
    public List<string> CsvFiles { get; set; } = new();
}

public sealed class PoChangeRemediationBundlePlan
{
    public string BundleNo { get; set; } = string.Empty;
    public string PoNumber { get; set; } = string.Empty;
    public string Action { get; set; } = string.Empty;
    public int TotalOrFilled { get; set; }
    public int? TargetNdtPcs { get; set; }
    public int SlitCount { get; set; }
    public bool Reprint { get; set; }
    public bool Exists { get; set; }
    public bool CurrentlyVoided { get; set; }
}

public sealed class PoChangeRemediationOrphanPlan
{
    public string BundleNo { get; set; } = string.Empty;
    public string PoNumber { get; set; } = string.Empty;
    public string? CsvFillState { get; set; }
    public string? PrintStatus { get; set; }
    public string? CloseSource { get; set; }
    public int CsvFilled { get; set; }
    public int SlitCount { get; set; }
    public int SlitSum { get; set; }
    public List<string> SlitNos { get; set; } = new();
}

public sealed class PoChangeRemediationCommitResult
{
    public bool Ok { get; set; }
    public List<string> Errors { get; set; } = new();
    public List<string> Warnings { get; set; } = new();
    public int MillNo { get; set; }
    public int MillSequence { get; set; }
    public List<string> VoidedBundleNos { get; set; } = new();
    public List<string> CsvFilesWritten { get; set; } = new();
    public List<PoChangeRemediationReprintResult> Reprints { get; set; } = new();
    public string Message { get; set; } = string.Empty;
}

public sealed class PoChangeRemediationReprintResult
{
    public string BundleNo { get; set; } = string.Empty;
    public bool Success { get; set; }
    public string? Message { get; set; }
}

public sealed class PoChangeRemediationService : IPoChangeRemediationService
{
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly IFormationChartProvider _formation;
    private readonly IPipeSizeProvider _pipeSize;
    private readonly IReconcileBundleTagService? _reprint;
    private readonly IOutputSlitSapStatusRepository _sapStatus;
    private readonly ILogger<PoChangeRemediationService> _logger;

    public PoChangeRemediationService(
        IOptionsMonitor<NdtBundleOptions> options,
        IFormationChartProvider formation,
        IPipeSizeProvider pipeSize,
        IOutputSlitSapStatusRepository sapStatus,
        ILogger<PoChangeRemediationService> logger,
        IReconcileBundleTagService? reprint = null)
    {
        _options = options;
        _formation = formation;
        _pipeSize = pipeSize;
        _sapStatus = sapStatus;
        _logger = logger;
        _reprint = reprint;
    }

    private NdtBundleOptions Opt => _options.CurrentValue;

    public async Task<PoChangeRemediationMillStatusResult> GetMillStatusAsync(
        int millNo,
        CancellationToken cancellationToken)
    {
        var result = new PoChangeRemediationMillStatusResult { MillNo = millNo };
        if (millNo is < 1 or > 4)
        {
            result.Errors.Add("Mill No must be 1–4.");
            return result;
        }

        if (!SqlTraceabilityConnection.IsSqlEnabled(Opt))
        {
            result.Errors.Add("SQL mode is required for PO-change remediation.");
            return result;
        }

        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await SqlTraceabilityConnection
            .OpenAsync(conn, _logger, "PO-change remediation mill-status", cancellationToken)
            .ConfigureAwait(false);

        result.CurrentMillSequence = await ReadMillSequenceAsync(conn, millNo, cancellationToken)
            .ConfigureAwait(false);

        var candidates = new List<PoChangeRemediationFormingCandidate>();
        await using (var cmd = new SqlCommand(@"
SELECT Bundle_No, PO_Number, Csv_Fill_State, Print_Status, Close_Source,
       COALESCE(Target_Ndt_Pcs, Total_NDT_Pcs, 0), COALESCE(Csv_Filled, 0)
FROM dbo.NDT_Bundle
WHERE Mill_No = @Mill AND ISNULL(Voided, 0) = 0
ORDER BY Bundle_No;", conn))
        {
            cmd.Parameters.AddWithValue("@Mill", millNo);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var bundleNo = reader.IsDBNull(0) ? string.Empty : reader.GetString(0).Trim();
                var fill = reader.IsDBNull(2) ? null : reader.GetString(2);
                var print = reader.IsDBNull(3) ? null : reader.GetString(3);
                var close = reader.IsDBNull(4) ? null : reader.GetString(4);
                if (!PoChangeRemediationValidation.IsFormingCandidate(fill, print, voided: false, close))
                    continue;
                candidates.Add(new PoChangeRemediationFormingCandidate
                {
                    BundleNo = bundleNo,
                    PoNumber = reader.IsDBNull(1) ? string.Empty : reader.GetString(1).Trim(),
                    CsvFillState = fill,
                    PrintStatus = print,
                    CloseSource = close,
                    TargetNdtPcs = reader.IsDBNull(5)
                        ? 0
                        : Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture),
                    CsvFilled = reader.IsDBNull(6)
                        ? 0
                        : Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture)
                });
            }
        }

        foreach (var c in candidates)
        {
            c.Slits = await ReadSlitsForBundleAsync(conn, c.BundleNo, cancellationToken).ConfigureAwait(false);
            c.SlitSum = c.Slits.Sum(s => s.NdtPipes);
        }

        result.FormingCandidates = candidates;
        if (candidates.Count > 1)
        {
            result.Warnings.Add(
                $"Mill {millNo} has {candidates.Count} forming/phantom stamps. Keep one, consolidate same-PO slits into it, and void the rest.");
        }

        // Prefer oldest candidate that already has slits; else oldest overall.
        result.SuggestedKeepBundleNo = candidates
            .OrderByDescending(c => c.SlitSum > 0)
            .ThenBy(c => c.BundleNo, StringComparer.OrdinalIgnoreCase)
            .Select(c => c.BundleNo)
            .FirstOrDefault();

        result.Ok = result.Errors.Count == 0;
        return result;
    }

    public async Task<PoChangeRemediationPreviewResult> PreviewAsync(
        PoChangeRemediationRequest request,
        CancellationToken cancellationToken)
    {
        var defaultTarget = await TryResolveDefaultTargetAsync(request, cancellationToken).ConfigureAwait(false);
        var validated = PoChangeRemediationValidation.Validate(request, defaultTarget);
        var preview = new PoChangeRemediationPreviewResult
        {
            MillNo = validated.MillNo,
            ProposedMillSequence = validated.ProposedMillSequence,
            Errors = validated.Errors.ToList(),
            Warnings = validated.Warnings.ToList()
        };

        if (!SqlTraceabilityConnection.IsSqlEnabled(Opt))
        {
            preview.Errors.Add("SQL mode is required for PO-change remediation.");
            preview.Ok = false;
            return preview;
        }

        if (!validated.Ok)
        {
            preview.Ok = false;
            return preview;
        }

        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await SqlTraceabilityConnection
            .OpenAsync(conn, _logger, "PO-change remediation preview", cancellationToken)
            .ConfigureAwait(false);

        preview.CurrentMillSequence = await ReadMillSequenceAsync(conn, validated.MillNo, cancellationToken)
            .ConfigureAwait(false);

        foreach (var fin in validated.Finished)
        {
            var (exists, voided) = await ReadBundleFlagsAsync(conn, fin.BundleNo, cancellationToken)
                .ConfigureAwait(false);
            preview.FinishedPlans.Add(new PoChangeRemediationBundlePlan
            {
                BundleNo = fin.BundleNo,
                PoNumber = fin.PoNumber,
                Action = exists ? (voided ? "Revive+Finish" : "Update+Finish") : "Insert+Finish",
                TotalOrFilled = fin.TotalNdtPcs,
                SlitCount = fin.Slits.Count,
                Reprint = fin.Reprint,
                Exists = exists,
                CurrentlyVoided = voided
            });
            foreach (var s in fin.Slits)
                preview.CsvFiles.Add(s.FileName);
        }

        if (validated.Forming is { } forming)
        {
            var (exists, voided) = await ReadBundleFlagsAsync(conn, forming.BundleNo, cancellationToken)
                .ConfigureAwait(false);
            preview.FormingPlan = new PoChangeRemediationBundlePlan
            {
                BundleNo = forming.BundleNo,
                PoNumber = forming.PoNumber,
                Action = exists ? (voided ? "Revive+Forming" : "Update+Forming") : "Insert+Forming",
                TotalOrFilled = forming.CsvFilled,
                TargetNdtPcs = forming.TargetNdtPcs,
                SlitCount = forming.Slits.Count,
                Exists = exists,
                CurrentlyVoided = voided
            };
            foreach (var s in forming.Slits)
                preview.CsvFiles.Add(s.FileName);
        }

        var keep = validated.Forming?.BundleNo;
        preview.OrphansToVoid = await ListOrphansToVoidAsync(conn, validated.MillNo, keep, cancellationToken)
            .ConfigureAwait(false);

        if (preview.OrphansToVoid.Count > 0)
        {
            preview.Warnings.Add(
                $"Will void {preview.OrphansToVoid.Count} other forming/phantom stamp(s) on mill {validated.MillNo} so only one forming remains.");
            var slitOrphans = preview.OrphansToVoid.Where(o => o.SlitCount > 0).ToList();
            if (slitOrphans.Count > 0)
            {
                var keepPo = validated.Forming?.PoNumber;
                var samePo = slitOrphans
                    .Where(o => string.Equals(o.PoNumber, keepPo, StringComparison.OrdinalIgnoreCase))
                    .ToList();
                var otherPo = slitOrphans.Except(samePo).ToList();
                if (samePo.Count > 0)
                {
                    var nos = samePo.SelectMany(o => o.SlitNos).Distinct(StringComparer.OrdinalIgnoreCase);
                    preview.Warnings.Add(
                        "Include these same-PO slits on the keep forming batch before commit (or they are deleted with the void): "
                        + string.Join(", ", nos) + ".");
                }

                if (otherPo.Count > 0)
                {
                    preview.Warnings.Add(
                        "Voiding other-PO forming with slits (rows deleted, not moved): "
                        + string.Join(", ", otherPo.Select(o => $"{o.BundleNo}({o.SlitCount} slits)")));
                }
            }
        }

        preview.Warnings.Add(
            "Prefer committing when the mill instance is quiet — live CSV fill may race with rewritten NDT output files. "
            + "For forming cleanup leave Target empty so the provisional chart size is used.");

        preview.Ok = preview.Errors.Count == 0;
        return preview;
    }

    public async Task<PoChangeRemediationCommitResult> CommitAsync(
        PoChangeRemediationRequest request,
        CancellationToken cancellationToken)
    {
        var result = new PoChangeRemediationCommitResult();
        if (request is null || !request.ConfirmCommit)
        {
            result.Errors.Add("ConfirmCommit must be true.");
            return result;
        }

        var preview = await PreviewAsync(request, cancellationToken).ConfigureAwait(false);
        result.Errors = preview.Errors.ToList();
        result.Warnings = preview.Warnings.ToList();
        result.MillNo = preview.MillNo;
        if (!preview.Ok)
            return result;

        var defaultTarget = await TryResolveDefaultTargetAsync(request, cancellationToken).ConfigureAwait(false);
        var validated = PoChangeRemediationValidation.Validate(request, defaultTarget);
        if (!validated.Ok)
        {
            result.Errors = validated.Errors.ToList();
            return result;
        }

        var folder = (Opt.OutputBundleFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(folder))
        {
            result.Errors.Add("OutputBundleFolder is not configured.");
            return result;
        }

        Directory.CreateDirectory(folder);

        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await SqlTraceabilityConnection
            .OpenAsync(conn, _logger, "PO-change remediation commit", cancellationToken)
            .ConfigureAwait(false);
        await using var tx = (SqlTransaction)await conn.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            foreach (var fin in validated.Finished)
            {
                await UpsertFinishedBundleAsync(conn, tx, validated.MillNo, fin, validated.Reason, validated.ReconciledBy, cancellationToken)
                    .ConfigureAwait(false);
                await ReplaceSlitsAsync(conn, tx, validated.MillNo, fin.BundleNo, fin.PoNumber, fin.Slits, folder, cancellationToken)
                    .ConfigureAwait(false);
                await ClearAccumulationAsync(conn, tx, validated.MillNo, fin.PoNumber, cancellationToken)
                    .ConfigureAwait(false);
            }

            // Clear slits on phantom/extra stamps first so same-PO slits can move onto keep forming.
            var orphans = await ListOrphansToVoidAsync(
                    conn, validated.MillNo, validated.Forming?.BundleNo, cancellationToken, tx)
                .ConfigureAwait(false);
            foreach (var orphan in orphans)
            {
                await DeleteSlitsForBundleAsync(conn, tx, orphan.BundleNo, cancellationToken)
                    .ConfigureAwait(false);
            }

            if (validated.Forming is { } forming)
            {
                await UpsertFormingBundleAsync(conn, tx, validated.MillNo, forming, cancellationToken)
                    .ConfigureAwait(false);
                await ReplaceSlitsAsync(conn, tx, validated.MillNo, forming.BundleNo, forming.PoNumber, forming.Slits, folder, cancellationToken)
                    .ConfigureAwait(false);
                await UpsertAccumulationAsync(conn, tx, validated.MillNo, forming.PoNumber, forming.CsvFilled, cancellationToken)
                    .ConfigureAwait(false);
            }

            var voided = await VoidExtraFormingAsync(
                    conn, tx, validated.MillNo, validated.Forming?.BundleNo, validated.Reason, cancellationToken)
                .ConfigureAwait(false);
            result.VoidedBundleNos = voided;

            await SetMillSequenceInTxAsync(
                    conn, tx, validated.MillNo, validated.ProposedMillSequence,
                    validated.ReconciledBy, validated.Reason, cancellationToken)
                .ConfigureAwait(false);

            await tx.CommitAsync(cancellationToken).ConfigureAwait(false);
            result.MillSequence = validated.ProposedMillSequence;
        }
        catch (Exception ex)
        {
            try { await tx.RollbackAsync(cancellationToken).ConfigureAwait(false); } catch { /* ignore */ }
            _logger.LogError(ex, "PO-change remediation SQL commit failed for mill {Mill}.", validated.MillNo);
            result.Errors.Add("SQL commit failed: " + ex.Message);
            return result;
        }

        // CSVs after SQL commit (overwrite allowed)
        foreach (var fin in validated.Finished)
        {
            foreach (var slit in fin.Slits)
            {
                var path = await WriteCsvAsync(folder, fin.BundleNo, fin.PoNumber, validated.MillNo, slit, cancellationToken)
                    .ConfigureAwait(false);
                result.CsvFilesWritten.Add(Path.GetFileName(path));
            }
        }

        if (validated.Forming is { } form)
        {
            foreach (var slit in form.Slits)
            {
                var path = await WriteCsvAsync(folder, form.BundleNo, form.PoNumber, validated.MillNo, slit, cancellationToken)
                    .ConfigureAwait(false);
                result.CsvFilesWritten.Add(Path.GetFileName(path));
            }
        }

        if (_reprint is not null)
        {
            foreach (var fin in validated.Finished.Where(f => f.Reprint))
            {
                try
                {
                    var bundle = await LoadBundleAsync(fin.BundleNo, cancellationToken).ConfigureAwait(false);
                    if (bundle is null)
                    {
                        result.Reprints.Add(new PoChangeRemediationReprintResult
                        {
                            BundleNo = fin.BundleNo,
                            Success = false,
                            Message = "Bundle not found after commit."
                        });
                        continue;
                    }

                    var print = await _reprint.ReprintAsync(bundle, cancellationToken).ConfigureAwait(false);
                    result.Reprints.Add(new PoChangeRemediationReprintResult
                    {
                        BundleNo = fin.BundleNo,
                        Success = print.Success,
                        Message = print.Message
                    });
                }
                catch (Exception ex)
                {
                    result.Reprints.Add(new PoChangeRemediationReprintResult
                    {
                        BundleNo = fin.BundleNo,
                        Success = false,
                        Message = ex.Message
                    });
                }
            }
        }

        result.Ok = true;
        result.Message =
            $"Remediated mill {validated.MillNo}: {validated.Finished.Count} finished, "
            + (validated.Forming is null ? "no forming" : $"forming {validated.Forming.BundleNo}")
            + $", voided {result.VoidedBundleNos.Count}, sequence={result.MillSequence}.";
        return result;
    }

    private async Task<int?> TryResolveDefaultTargetAsync(
        PoChangeRemediationRequest request,
        CancellationToken cancellationToken)
    {
        try
        {
            var po = InputSlitCsvParsing.NormalizePo(request.Forming?.PoNumber);
            if (string.IsNullOrWhiteSpace(po))
                return null;
            string? pipeSize = null;
            try
            {
                pipeSize = await _pipeSize.TryGetPipeSizeForPoAsync(po, cancellationToken).ConfigureAwait(false);
            }
            catch { /* ignore */ }

            var chart = await _formation.GetFormationChartAsync(cancellationToken).ConfigureAwait(false);
            return Math.Max(1, FormationChartLookup.ResolveThreshold(chart, pipeSize));
        }
        catch
        {
            return null;
        }
    }

    private static async Task<int> ReadMillSequenceAsync(SqlConnection conn, int millNo, CancellationToken ct)
    {
        await using var cmd = new SqlCommand(
            "SELECT Current_Sequence FROM dbo.Mill_Sequence WHERE Mill_No = @Mill;", conn);
        cmd.Parameters.AddWithValue("@Mill", millNo);
        var scalar = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return scalar is null or DBNull ? 0 : Convert.ToInt32(scalar, CultureInfo.InvariantCulture);
    }

    private static async Task<(bool Exists, bool Voided)> ReadBundleFlagsAsync(
        SqlConnection conn, string bundleNo, CancellationToken ct)
    {
        await using var cmd = new SqlCommand(@"
SELECT ISNULL(Voided, 0) FROM dbo.NDT_Bundle WHERE Bundle_No = @B;", conn);
        cmd.Parameters.AddWithValue("@B", bundleNo);
        var scalar = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        if (scalar is null or DBNull)
            return (false, false);
        return (true, Convert.ToInt32(scalar, CultureInfo.InvariantCulture) != 0);
    }

    private static async Task<List<PoChangeRemediationOrphanPlan>> ListOrphansToVoidAsync(
        SqlConnection conn,
        int millNo,
        string? keepBundleNo,
        CancellationToken ct,
        SqlTransaction? tx = null)
    {
        await using var cmd = new SqlCommand(@"
SELECT Bundle_No, PO_Number, Csv_Fill_State, Print_Status, Close_Source, COALESCE(Csv_Filled, 0)
FROM dbo.NDT_Bundle
WHERE Mill_No = @Mill AND ISNULL(Voided, 0) = 0;", conn, tx);
        cmd.Parameters.AddWithValue("@Mill", millNo);
        var list = new List<PoChangeRemediationOrphanPlan>();
        await using (var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var bundleNo = reader.IsDBNull(0) ? string.Empty : reader.GetString(0).Trim();
                var fill = reader.IsDBNull(2) ? null : reader.GetString(2);
                var print = reader.IsDBNull(3) ? null : reader.GetString(3);
                var close = reader.IsDBNull(4) ? null : reader.GetString(4);
                if (!PoChangeRemediationValidation.ShouldVoidAsExtraForming(
                        bundleNo, fill, print, voided: false, keepBundleNo, close))
                    continue;
                list.Add(new PoChangeRemediationOrphanPlan
                {
                    BundleNo = bundleNo,
                    PoNumber = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                    CsvFillState = fill,
                    PrintStatus = print,
                    CloseSource = close,
                    CsvFilled = reader.IsDBNull(5)
                        ? 0
                        : Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture)
                });
            }
        }

        foreach (var orphan in list)
        {
            var slits = await ReadSlitsForBundleAsync(conn, orphan.BundleNo, ct, tx).ConfigureAwait(false);
            orphan.SlitCount = slits.Count;
            orphan.SlitSum = slits.Sum(s => s.NdtPipes);
            orphan.SlitNos = slits
                .Select(s => s.SlitNo ?? string.Empty)
                .Where(s => s.Length > 0)
                .ToList();
        }

        return list;
    }

    private static async Task<List<PoChangeRemediationSlitRow>> ReadSlitsForBundleAsync(
        SqlConnection conn,
        string bundleNo,
        CancellationToken ct,
        SqlTransaction? tx = null)
    {
        await using var cmd = new SqlCommand(@"
SELECT Slit_No, NDT_Pipes, Rejected_P, Slit_Start_Time, Slit_Finish_Time,
       NDT_Short_Length_Pipe, Rejected_Short_Length_Pipe, Source_File
FROM dbo.Output_Slit_Row
WHERE NDT_Batch_No = @B
ORDER BY Slit_Start_Time, Slit_No;", conn, tx);
        cmd.Parameters.AddWithValue("@B", bundleNo);
        var list = new List<PoChangeRemediationSlitRow>();
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var start = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3);
            var finish = reader.IsDBNull(4) ? (DateTime?)null : reader.GetDateTime(4);
            var source = reader.IsDBNull(7) ? null : reader.GetString(7);
            list.Add(new PoChangeRemediationSlitRow
            {
                SlitNo = reader.IsDBNull(0) ? string.Empty : reader.GetString(0).Trim(),
                NdtPipes = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture),
                RejectedPipes = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture),
                SlitStartTime = start?.ToString(
                    PoChangeRemediationValidation.SapDateFormat, CultureInfo.InvariantCulture) ?? string.Empty,
                SlitFinishTime = finish?.ToString(
                    PoChangeRemediationValidation.SapDateFormat, CultureInfo.InvariantCulture) ?? string.Empty,
                NdtShortLengthPipe = reader.IsDBNull(5) ? null : reader.GetString(5),
                RejectedShortLengthPipe = reader.IsDBNull(6) ? null : reader.GetString(6),
                SourceFileName = string.IsNullOrWhiteSpace(source) ? null : Path.GetFileName(source.Trim())
            });
        }

        return list;
    }

    private static async Task DeleteSlitsForBundleAsync(
        SqlConnection conn,
        SqlTransaction tx,
        string bundleNo,
        CancellationToken ct)
    {
        await using var del = new SqlCommand(
            "DELETE FROM dbo.Output_Slit_Row WHERE NDT_Batch_No = @B;", conn, tx);
        del.Parameters.AddWithValue("@B", bundleNo);
        await del.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static async Task UpsertFinishedBundleAsync(
        SqlConnection conn,
        SqlTransaction tx,
        int millNo,
        PoChangeRemediationValidatedFinished fin,
        string reason,
        string by,
        CancellationToken ct)
    {
        var contextSlit = fin.Slits.Count > 0 ? fin.Slits[^1].SlitNo : null;
        var start = fin.Slits.Count > 0 ? fin.Slits[0].Start : (DateTime?)null;
        var finish = fin.Slits.Count > 0 ? fin.Slits[^1].Finish : (DateTime?)null;
        var rejected = fin.Slits.Sum(s => s.RejectedPipes);
        var shortLen = fin.Slits.Select(s => s.NdtShortLengthPipe).FirstOrDefault(s => !string.IsNullOrWhiteSpace(s));

        await using var cmd = new SqlCommand(@"
IF EXISTS (SELECT 1 FROM dbo.NDT_Bundle WITH (UPDLOCK, ROWLOCK) WHERE Bundle_No = @BundleNo)
BEGIN
  UPDATE dbo.NDT_Bundle
  SET PO_Number = @Po,
      Mill_No = @Mill,
      Total_NDT_Pcs = @Total,
      Target_Ndt_Pcs = @Total,
      Csv_Filled = @Total,
      Csv_Fill_State = N'CsvComplete',
      Close_Source = COALESCE(NULLIF(Close_Source, N''), N'CsvAdvance'),
      Print_Status = N'Printed',
      PrintedAt = COALESCE(PrintedAt, SYSDATETIME()),
      Print_Attempted_At = COALESCE(Print_Attempted_At, SYSDATETIME()),
      Print_Error = NULL,
      Manual_Recon = 1,
      Manual_Recon_By = @By,
      Manual_Recon_At = SYSUTCDATETIME(),
      Manual_Recon_Reason = @Reason,
      Manual_Recon_Original_Total = COALESCE(Manual_Recon_Original_Total, @Total),
      Awaiting_Csv_Recon = 0,
      Count_Discrepancy = 0,
      Manual_Review = 0,
      Voided = 0,
      Voided_AtUtc = NULL,
      Voided_Reason = NULL,
      Context_Slit_No = @ContextSlit,
      Slit_Start_Time = @Start,
      Slit_Finish_Time = @Finish,
      Rejected_P = @Rejected,
      NDT_Short_Length_Pipe = @ShortLen,
      Csv_Last_Row_AtUtc = SYSUTCDATETIME()
  WHERE Bundle_No = @BundleNo;
END
ELSE
BEGIN
  INSERT INTO dbo.NDT_Bundle
    (PO_Number, Mill_No, Bundle_No, Total_NDT_Pcs, Target_Ndt_Pcs, Csv_Filled, Csv_Fill_State,
     Context_Slit_No, Slit_Start_Time, Slit_Finish_Time, Rejected_P, NDT_Short_Length_Pipe,
     IsReprint, Print_Status, Print_Attempted_At, PrintedAt, Close_Source,
     Awaiting_Csv_Recon, Count_Discrepancy, Manual_Recon, Manual_Recon_By, Manual_Recon_At, Manual_Recon_Reason,
     Manual_Recon_Original_Total, Voided, Csv_Last_Row_AtUtc)
  VALUES
    (@Po, @Mill, @BundleNo, @Total, @Total, @Total, N'CsvComplete',
     @ContextSlit, @Start, @Finish, @Rejected, @ShortLen,
     0, N'Printed', SYSDATETIME(), SYSDATETIME(), N'CsvAdvance',
     0, 0, 1, @By, SYSUTCDATETIME(), @Reason,
     @Total, 0, SYSUTCDATETIME());
END", conn, tx);
        cmd.Parameters.AddWithValue("@BundleNo", fin.BundleNo);
        cmd.Parameters.AddWithValue("@Po", fin.PoNumber);
        cmd.Parameters.AddWithValue("@Mill", millNo);
        cmd.Parameters.AddWithValue("@Total", fin.TotalNdtPcs);
        cmd.Parameters.AddWithValue("@By", Truncate(by, 128));
        cmd.Parameters.AddWithValue("@Reason", Truncate(reason, 512));
        cmd.Parameters.AddWithValue("@ContextSlit", (object?)contextSlit ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Start", (object?)start ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Finish", (object?)finish ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Rejected", rejected);
        cmd.Parameters.AddWithValue("@ShortLen", (object?)shortLen ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static async Task UpsertFormingBundleAsync(
        SqlConnection conn,
        SqlTransaction tx,
        int millNo,
        PoChangeRemediationValidatedForming forming,
        CancellationToken ct)
    {
        var contextSlit = forming.Slits.Count > 0 ? forming.Slits[^1].SlitNo : null;
        var start = forming.Slits.Count > 0 ? forming.Slits[0].Start : (DateTime?)null;
        var finish = forming.Slits.Count > 0 ? forming.Slits[^1].Finish : (DateTime?)null;
        var rejected = forming.Slits.Sum(s => s.RejectedPipes);
        var fillState = forming.CsvFilled > 0 ? CsvFillState.CsvFilling : CsvFillState.PlcClosed;

        await using var cmd = new SqlCommand(@"
IF EXISTS (SELECT 1 FROM dbo.NDT_Bundle WITH (UPDLOCK, ROWLOCK) WHERE Bundle_No = @BundleNo)
BEGIN
  UPDATE dbo.NDT_Bundle
  SET PO_Number = @Po,
      Mill_No = @Mill,
      Total_NDT_Pcs = @Target,
      Target_Ndt_Pcs = @Target,
      Csv_Filled = @Filled,
      Csv_Fill_State = @FillState,
      Close_Source = N'CsvAdvance',
      Print_Status = N'Pending',
      PrintedAt = NULL,
      Print_Attempted_At = SYSUTCDATETIME(),
      Print_Error = NULL,
      Manual_Recon = 0,
      Manual_Recon_By = NULL,
      Manual_Recon_At = NULL,
      Manual_Recon_Reason = NULL,
      Manual_Recon_Original_Total = NULL,
      Awaiting_Csv_Recon = 0,
      Count_Discrepancy = 0,
      Manual_Review = 0,
      Voided = 0,
      Voided_AtUtc = NULL,
      Voided_Reason = NULL,
      Context_Slit_No = @ContextSlit,
      Slit_Start_Time = @Start,
      Slit_Finish_Time = @Finish,
      Rejected_P = @Rejected,
      Csv_Last_Row_AtUtc = SYSUTCDATETIME()
  WHERE Bundle_No = @BundleNo;
END
ELSE
BEGIN
  INSERT INTO dbo.NDT_Bundle
    (PO_Number, Mill_No, Bundle_No, Total_NDT_Pcs, Target_Ndt_Pcs, Csv_Filled, Csv_Fill_State,
     Context_Slit_No, Slit_Start_Time, Slit_Finish_Time, Rejected_P,
     IsReprint, Print_Status, Print_Attempted_At, PrintedAt, Close_Source,
     Awaiting_Csv_Recon, Count_Discrepancy, Manual_Recon, Voided, Csv_Last_Row_AtUtc)
  VALUES
    (@Po, @Mill, @BundleNo, @Target, @Target, @Filled, @FillState,
     @ContextSlit, @Start, @Finish, @Rejected,
     0, N'Pending', SYSUTCDATETIME(), NULL, N'CsvAdvance',
     0, 0, 0, 0, SYSUTCDATETIME());
END", conn, tx);
        cmd.Parameters.AddWithValue("@BundleNo", forming.BundleNo);
        cmd.Parameters.AddWithValue("@Po", forming.PoNumber);
        cmd.Parameters.AddWithValue("@Mill", millNo);
        cmd.Parameters.AddWithValue("@Target", forming.TargetNdtPcs);
        cmd.Parameters.AddWithValue("@Filled", forming.CsvFilled);
        cmd.Parameters.AddWithValue("@FillState", fillState);
        cmd.Parameters.AddWithValue("@ContextSlit", (object?)contextSlit ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Start", (object?)start ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Finish", (object?)finish ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@Rejected", rejected);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static async Task ReplaceSlitsAsync(
        SqlConnection conn,
        SqlTransaction tx,
        int millNo,
        string bundleNo,
        string poNumber,
        IReadOnlyList<PoChangeRemediationValidatedSlit> slits,
        string folder,
        CancellationToken ct)
    {
        await using (var del = new SqlCommand(
            "DELETE FROM dbo.Output_Slit_Row WHERE NDT_Batch_No = @B;", conn, tx))
        {
            del.Parameters.AddWithValue("@B", bundleNo);
            await del.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        foreach (var slit in slits)
        {
            var sourceFile = Path.Combine(folder, slit.FileName);
            await using var ins = new SqlCommand(@"
INSERT INTO dbo.Output_Slit_Row
  (PO_Number, Slit_No, NDT_Pipes, Rejected_P, Slit_Start_Time, Slit_Finish_Time, Mill_No,
   NDT_Short_Length_Pipe, Rejected_Short_Length_Pipe, NDT_Batch_No, Source_File, Source_Row_Number)
VALUES
  (@Po, @Slit, @Ndt, @Rej, @Start, @Finish, @Mill, @Short, @RejShort, @Batch, @Source, 2);", conn, tx);
            ins.Parameters.AddWithValue("@Po", poNumber);
            ins.Parameters.AddWithValue("@Slit", slit.SlitNo);
            ins.Parameters.AddWithValue("@Ndt", slit.NdtPipes);
            ins.Parameters.AddWithValue("@Rej", slit.RejectedPipes);
            ins.Parameters.AddWithValue("@Start", slit.Start);
            ins.Parameters.AddWithValue("@Finish", slit.Finish);
            ins.Parameters.AddWithValue("@Mill", millNo);
            ins.Parameters.AddWithValue("@Short", (object?)slit.NdtShortLengthPipe ?? DBNull.Value);
            ins.Parameters.AddWithValue("@RejShort", (object?)slit.RejectedShortLengthPipe ?? DBNull.Value);
            ins.Parameters.AddWithValue("@Batch", bundleNo);
            ins.Parameters.AddWithValue("@Source", sourceFile);
            await ins.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    private static async Task<List<string>> VoidExtraFormingAsync(
        SqlConnection conn,
        SqlTransaction tx,
        int millNo,
        string? keepBundleNo,
        string reason,
        CancellationToken ct)
    {
        await using var find = new SqlCommand(@"
SELECT Bundle_No, Csv_Fill_State, Print_Status, Close_Source
FROM dbo.NDT_Bundle WITH (UPDLOCK, ROWLOCK)
WHERE Mill_No = @Mill AND ISNULL(Voided, 0) = 0;", conn, tx);
        find.Parameters.AddWithValue("@Mill", millNo);
        var toVoid = new List<string>();
        await using (var reader = await find.ExecuteReaderAsync(ct).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var bundleNo = reader.IsDBNull(0) ? string.Empty : reader.GetString(0).Trim();
                var fill = reader.IsDBNull(1) ? null : reader.GetString(1);
                var print = reader.IsDBNull(2) ? null : reader.GetString(2);
                var close = reader.IsDBNull(3) ? null : reader.GetString(3);
                if (PoChangeRemediationValidation.ShouldVoidAsExtraForming(
                        bundleNo, fill, print, false, keepBundleNo, close))
                    toVoid.Add(bundleNo);
            }
        }

        var voidReason = Truncate(
            "PO-change remediation: only one forming allowed. " + reason, 512);
        foreach (var bundleNo in toVoid)
        {
            await using var voidCmd = new SqlCommand(@"
UPDATE dbo.NDT_Bundle
SET Voided = 1,
    Voided_AtUtc = SYSUTCDATETIME(),
    Voided_Reason = @Reason,
    Total_NDT_Pcs = 0,
    Target_Ndt_Pcs = 0,
    Csv_Filled = 0,
    Csv_Fill_State = N'Voided'
WHERE Bundle_No = @B AND ISNULL(Voided, 0) = 0;", conn, tx);
            voidCmd.Parameters.AddWithValue("@B", bundleNo);
            voidCmd.Parameters.AddWithValue("@Reason", voidReason);
            await voidCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        return toVoid;
    }

    private static async Task SetMillSequenceInTxAsync(
        SqlConnection conn,
        SqlTransaction tx,
        int millNo,
        int sequence,
        string by,
        string reason,
        CancellationToken ct)
    {
        await using var upd = new SqlCommand(@"
UPDATE dbo.Mill_Sequence
SET Current_Sequence = @Seq,
    Updated_AtUtc = SYSUTCDATETIME(),
    Updated_By = @By,
    Reason = @Reason
WHERE Mill_No = @Mill;", conn, tx);
        upd.Parameters.AddWithValue("@Seq", sequence);
        upd.Parameters.AddWithValue("@By", Truncate(by, 128));
        upd.Parameters.AddWithValue("@Reason", Truncate(reason, 512));
        upd.Parameters.AddWithValue("@Mill", millNo);
        var n = await upd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        if (n == 0)
            throw new InvalidOperationException($"Mill_Sequence row missing for mill {millNo}.");
    }

    private static async Task ClearAccumulationAsync(
        SqlConnection conn, SqlTransaction tx, int millNo, string po, CancellationToken ct)
    {
        await using var cmd = new SqlCommand(
            "DELETE FROM dbo.Bundle_Accumulation WHERE Mill_No = @Mill AND Po_Number = @Po;", conn, tx);
        cmd.Parameters.AddWithValue("@Mill", millNo);
        cmd.Parameters.AddWithValue("@Po", po);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static async Task UpsertAccumulationAsync(
        SqlConnection conn, SqlTransaction tx, int millNo, string po, int pcs, CancellationToken ct)
    {
        await using var cmd = new SqlCommand(@"
MERGE dbo.Bundle_Accumulation AS t
USING (SELECT @Mill AS Mill_No, @Po AS Po_Number, N'2.5' AS Size_Key, @Pcs AS Pcs) AS s
ON t.Mill_No = s.Mill_No AND t.Po_Number = s.Po_Number AND t.Size_Key = s.Size_Key
WHEN MATCHED THEN UPDATE SET Pcs = s.Pcs, Last_Activity_Utc = SYSUTCDATETIME()
WHEN NOT MATCHED THEN INSERT (Mill_No, Po_Number, Size_Key, Pcs, Last_Activity_Utc)
VALUES (s.Mill_No, s.Po_Number, s.Size_Key, s.Pcs, SYSUTCDATETIME());", conn, tx);
        cmd.Parameters.AddWithValue("@Mill", millNo);
        cmd.Parameters.AddWithValue("@Po", po);
        cmd.Parameters.AddWithValue("@Pcs", pcs);
        await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task<string> WriteCsvAsync(
        string folder,
        string bundleNo,
        string po,
        int millNo,
        PoChangeRemediationValidatedSlit slit,
        CancellationToken ct)
    {
        var record = new InputSlitRecord
        {
            PoNumber = po,
            SlitNo = slit.SlitNo,
            NdtPipes = slit.NdtPipes,
            RejectedPipes = slit.RejectedPipes,
            SlitStartTime = slit.Start,
            SlitFinishTime = slit.Finish,
            MillNo = millNo,
            NdtShortLengthPipe = slit.NdtShortLengthPipe ?? string.Empty,
            RejectedShortLengthPipe = slit.RejectedShortLengthPipe ?? string.Empty
        };
        var text = NdtInputSlitOutputCsv.ToCsvText(new[] { (record, bundleNo) });
        var path = Path.Combine(folder, slit.FileName);
        await File.WriteAllTextAsync(path, text, new UTF8Encoding(encoderShouldEmitUTF8Identifier: true), ct)
            .ConfigureAwait(false);

        try
        {
            var lw = File.GetLastWriteTimeUtc(path);
            await _sapStatus
                .RecordOutputFileWrittenAsync(slit.FileName, lw, folder, ct)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "SAP status seed skipped for {File}.", slit.FileName);
        }

        return path;
    }

    private async Task<NdtBundleRecord?> LoadBundleAsync(string bundleNo, CancellationToken ct)
    {
        // Lightweight reload via SQL for reprint
        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await conn.OpenAsync(ct).ConfigureAwait(false);
        await using var cmd = new SqlCommand(@"
SELECT Bundle_No, PO_Number, Mill_No, Total_NDT_Pcs, Target_Ndt_Pcs, Csv_Filled, Csv_Fill_State,
       Print_Status, Close_Source, Context_Slit_No, Rejected_P, NDT_Short_Length_Pipe
FROM dbo.NDT_Bundle WHERE Bundle_No = @B AND ISNULL(Voided, 0) = 0;", conn);
        cmd.Parameters.AddWithValue("@B", bundleNo);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;
        return new NdtBundleRecord
        {
            BundleNo = reader.GetString(0),
            PoNumber = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
            MillNo = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
            TotalNdtPcs = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
            TargetNdtPcs = reader.IsDBNull(4) ? null : reader.GetInt32(4),
            CsvFilled = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
            CsvFillState = reader.IsDBNull(6) ? null : reader.GetString(6),
            PrintStatus = reader.IsDBNull(7) ? BundlePrintStatus.Pending : reader.GetString(7),
            CloseSource = reader.IsDBNull(8) ? null : reader.GetString(8),
            SlitNo = reader.IsDBNull(9) ? string.Empty : reader.GetString(9),
            RejectedPipes = reader.IsDBNull(10) ? 0 : reader.GetInt32(10),
            NdtShortLengthPipe = reader.IsDBNull(11) ? string.Empty : reader.GetString(11)
        };
    }

    private static string Truncate(string value, int max) =>
        value.Length <= max ? value : value[..max];
}
