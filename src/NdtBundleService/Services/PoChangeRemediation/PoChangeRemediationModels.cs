using System.Globalization;
using System.IO;
using System.Text.RegularExpressions;
using NdtBundleService.Models;
using NdtBundleService.Services;

namespace NdtBundleService.Services.PoChangeRemediation;

public sealed class PoChangeRemediationRequest
{
    public int MillNo { get; set; }
    public string? Reason { get; set; }
    public string? ReconciledBy { get; set; }
    public List<PoChangeRemediationFinishedBatch> FinishedBatches { get; set; } = new();
    public PoChangeRemediationFormingBatch? Forming { get; set; }
    public bool ConfirmCommit { get; set; }
}

public sealed class PoChangeRemediationFinishedBatch
{
    public string? BundleNo { get; set; }
    public string? PoNumber { get; set; }
    public int TotalNdtPcs { get; set; }
    public bool Reprint { get; set; }
    public List<PoChangeRemediationSlitRow> Slits { get; set; } = new();
}

public sealed class PoChangeRemediationFormingBatch
{
    public string? BundleNo { get; set; }
    public string? PoNumber { get; set; }
    public int? TargetNdtPcs { get; set; }
    public List<PoChangeRemediationSlitRow> Slits { get; set; } = new();
}

public sealed class PoChangeRemediationSlitRow
{
    public string? SlitNo { get; set; }
    public int NdtPipes { get; set; }
    public int RejectedPipes { get; set; }
    public string? SlitStartTime { get; set; }
    public string? SlitFinishTime { get; set; }
    public string? NdtShortLengthPipe { get; set; }
    public string? RejectedShortLengthPipe { get; set; }

    /// <summary>
    /// Optional existing NDT output CSV basename (e.g. from mill scan). When set, commit overwrites
    /// that file instead of rebuilding the name from slit start time.
    /// </summary>
    public string? SourceFileName { get; set; }
}

public sealed record PoChangeRemediationValidatedSlit(
    string SlitNo,
    int NdtPipes,
    int RejectedPipes,
    DateTime Start,
    DateTime Finish,
    string StartText,
    string FinishText,
    string? NdtShortLengthPipe,
    string? RejectedShortLengthPipe,
    string FileName);

public sealed record PoChangeRemediationValidatedFinished(
    string BundleNo,
    int Sequence,
    string PoNumber,
    int TotalNdtPcs,
    bool Reprint,
    IReadOnlyList<PoChangeRemediationValidatedSlit> Slits);

public sealed record PoChangeRemediationValidatedForming(
    string BundleNo,
    int Sequence,
    string PoNumber,
    int TargetNdtPcs,
    int CsvFilled,
    IReadOnlyList<PoChangeRemediationValidatedSlit> Slits);

public sealed class PoChangeRemediationValidationResult
{
    public List<string> Errors { get; } = new();
    public List<string> Warnings { get; } = new();
    public int MillNo { get; set; }
    public string Reason { get; set; } = string.Empty;
    public string ReconciledBy { get; set; } = string.Empty;
    public List<PoChangeRemediationValidatedFinished> Finished { get; } = new();
    public PoChangeRemediationValidatedForming? Forming { get; set; }
    public int ProposedMillSequence { get; set; }
    public bool Ok => Errors.Count == 0;
}

/// <summary>Pure request validation for the PO-change remediation wizard.</summary>
public static class PoChangeRemediationValidation
{
    public const string SapDateFormat = "dd.MM.yyyy HH:mm:ss";

    private static readonly Regex SapDatePattern = new(
        @"^\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    public static PoChangeRemediationValidationResult Validate(
        PoChangeRemediationRequest? request,
        int? defaultFormingTarget = null)
    {
        var result = new PoChangeRemediationValidationResult();
        if (request is null)
        {
            result.Errors.Add("Request body is required.");
            return result;
        }

        if (request.MillNo is < 1 or > 4)
            result.Errors.Add("Mill No must be 1–4.");
        result.MillNo = request.MillNo;

        var reason = (request.Reason ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(reason))
            result.Errors.Add("Reason is required.");
        result.Reason = reason;

        var by = string.IsNullOrWhiteSpace(request.ReconciledBy)
            ? "po-change-remediation"
            : request.ReconciledBy.Trim();
        result.ReconciledBy = by;

        var finishedRaw = request.FinishedBatches ?? new List<PoChangeRemediationFinishedBatch>();
        if (finishedRaw.Count == 0 && request.Forming is null)
            result.Errors.Add("Enter at least one finished batch and/or a forming batch.");

        var bundleSet = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var slitKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var sequences = new List<int>();

        for (var i = 0; i < finishedRaw.Count; i++)
        {
            var batch = finishedRaw[i];
            var label = $"Finished[{i}]";
            var bundleNo = (batch.BundleNo ?? string.Empty).Trim();
            var po = InputSlitCsvParsing.NormalizePo(batch.PoNumber);
            if (string.IsNullOrWhiteSpace(bundleNo))
            {
                result.Errors.Add($"{label}: Bundle No is required.");
                continue;
            }

            if (!bundleSet.Add(bundleNo))
                result.Errors.Add($"{label}: Duplicate Bundle No {bundleNo}.");

            if (!NdtBundleSequence.TryParseSequence(bundleNo, request.MillNo, out var seq) || seq <= 0)
                result.Errors.Add($"{label}: Bundle No {bundleNo} is not valid for mill {request.MillNo} this year.");
            else
                sequences.Add(seq);

            if (string.IsNullOrWhiteSpace(po))
                result.Errors.Add($"{label}: PO Number is required.");

            if (batch.TotalNdtPcs < 0)
                result.Errors.Add($"{label}: Total NDT Pcs must be >= 0.");

            var slits = ValidateSlits(
                batch.Slits,
                po,
                label,
                slitKeys,
                result);
            var sum = 0;
            foreach (var s in slits)
                sum += s.NdtPipes;

            if (slits.Count > 0 && sum != batch.TotalNdtPcs)
                result.Errors.Add($"{label}: Slit sum {sum} does not match TotalNdtPcs {batch.TotalNdtPcs}.");

            if (!string.IsNullOrWhiteSpace(bundleNo)
                && !string.IsNullOrWhiteSpace(po)
                && seq > 0
                && slits.Count == (batch.Slits?.Count ?? 0))
            {
                result.Finished.Add(new PoChangeRemediationValidatedFinished(
                    bundleNo, seq, po, batch.TotalNdtPcs, batch.Reprint, slits));
            }
        }

        if (request.Forming is not null)
        {
            var f = request.Forming;
            const string label = "Forming";
            var bundleNo = (f.BundleNo ?? string.Empty).Trim();
            var po = InputSlitCsvParsing.NormalizePo(f.PoNumber);
            if (string.IsNullOrWhiteSpace(bundleNo))
                result.Errors.Add($"{label}: Bundle No is required (or omit forming).");
            else if (!bundleSet.Add(bundleNo))
                result.Errors.Add($"{label}: Bundle No {bundleNo} already used in finished batches.");

            var seq = 0;
            if (!string.IsNullOrWhiteSpace(bundleNo)
                && (!NdtBundleSequence.TryParseSequence(bundleNo, request.MillNo, out seq) || seq <= 0))
                result.Errors.Add($"{label}: Bundle No {bundleNo} is not valid for mill {request.MillNo} this year.");
            else if (seq > 0)
                sequences.Add(seq);

            if (string.IsNullOrWhiteSpace(po))
                result.Errors.Add($"{label}: PO Number is required.");

            var slits = ValidateSlits(f.Slits, po, label, slitKeys, result);
            var filled = 0;
            foreach (var s in slits)
                filled += s.NdtPipes;

            var target = f.TargetNdtPcs is > 0
                ? f.TargetNdtPcs.Value
                : Math.Max(defaultFormingTarget ?? 0, Math.Max(1, filled));

            if (filled > target)
                result.Errors.Add($"{label}: Slit sum {filled} exceeds target {target}.");

            if (!string.IsNullOrWhiteSpace(bundleNo)
                && !string.IsNullOrWhiteSpace(po)
                && seq > 0
                && slits.Count == (f.Slits?.Count ?? 0))
            {
                result.Forming = new PoChangeRemediationValidatedForming(
                    bundleNo, seq, po, target, filled, slits);
            }
        }

        result.ProposedMillSequence = sequences.Count == 0 ? 0 : sequences.Max();

        if (sequences.Count > 1)
        {
            var ordered = sequences.Distinct().OrderBy(x => x).ToList();
            for (var i = 1; i < ordered.Count; i++)
            {
                if (ordered[i] != ordered[i - 1] + 1)
                {
                    result.Warnings.Add(
                        $"Sequence gap between {ordered[i - 1]} and {ordered[i]} — ensure voided placeholders cover holes.");
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Whether an existing mill stamp should be voided so only <paramref name="keepBundleNo"/> remains forming.
    /// Never voids Printed tags. Voids open fill slots and unprinted <c>CsvOvershoot</c> / phantom
    /// <c>CsvAdvance</c> stamps (dual-forming cleanup), including empty <c>CsvShort</c> orphans.
    /// Does not void filled <c>CsvComplete</c>/<c>CsvShort</c> finished remainders awaiting print.
    /// </summary>
    public static bool ShouldVoidAsExtraForming(
        string bundleNo,
        string? csvFillState,
        string? printStatus,
        bool voided,
        string? keepBundleNo,
        string? closeSource = null)
    {
        if (voided)
            return false;
        if (!string.IsNullOrWhiteSpace(keepBundleNo)
            && string.Equals(bundleNo.Trim(), keepBundleNo.Trim(), StringComparison.OrdinalIgnoreCase))
            return false;

        if (string.Equals(printStatus, BundlePrintStatus.Printed, StringComparison.OrdinalIgnoreCase))
            return false;

        if (CsvFillState.IsIncomplete(csvFillState))
            return true;

        var pending = string.IsNullOrWhiteSpace(printStatus)
            || string.Equals(printStatus, BundlePrintStatus.Pending, StringComparison.OrdinalIgnoreCase);
        if (!pending)
            return false;

        // Phantom next-stamp after overshoot (still Pending, never printed).
        if (string.Equals(csvFillState, CsvFillState.CsvOvershoot, StringComparison.OrdinalIgnoreCase))
            return true;

        // Empty/pending CsvAdvance stamp with unknown/legacy fill state.
        if (string.Equals(closeSource, BundleCloseSource.CsvAdvance, StringComparison.OrdinalIgnoreCase)
            && (string.IsNullOrWhiteSpace(csvFillState)
                || string.Equals(csvFillState, CsvFillState.Voided, StringComparison.OrdinalIgnoreCase)))
            return true;

        return false;
    }

    /// <summary>Live forming / phantom candidates shown by mill scan (same void rules, keep=null).</summary>
    public static bool IsFormingCandidate(
        string? csvFillState,
        string? printStatus,
        bool voided,
        string? closeSource) =>
        ShouldVoidAsExtraForming(
            bundleNo: "_candidate_",
            csvFillState,
            printStatus,
            voided,
            keepBundleNo: null,
            closeSource);

    public static bool TryParseSapDateTime(string? raw, out DateTime parsed, out string normalized)
    {
        parsed = default;
        normalized = string.Empty;
        var text = NormalizeSapDateTimeInput(raw);
        if (!SapDatePattern.IsMatch(text))
            return false;
        if (!DateTime.TryParseExact(
                text,
                SapDateFormat,
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out parsed))
            return false;
        normalized = parsed.ToString(SapDateFormat, CultureInfo.InvariantCulture);
        return true;
    }

    private static List<PoChangeRemediationValidatedSlit> ValidateSlits(
        List<PoChangeRemediationSlitRow>? slits,
        string po,
        string label,
        HashSet<string> slitKeys,
        PoChangeRemediationValidationResult result)
    {
        var list = new List<PoChangeRemediationValidatedSlit>();
        var rows = slits ?? new List<PoChangeRemediationSlitRow>();
        for (var i = 0; i < rows.Count; i++)
        {
            var row = rows[i];
            var slitLabel = $"{label}.Slits[{i}]";
            var slitNo = (row.SlitNo ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(slitNo))
            {
                result.Errors.Add($"{slitLabel}: Slit No is required.");
                continue;
            }

            if (row.NdtPipes < 0)
                result.Errors.Add($"{slitLabel}: NDT Pipes must be >= 0.");
            if (row.RejectedPipes < 0)
                result.Errors.Add($"{slitLabel}: Rejected Pipes must be >= 0.");

            if (!TryParseSapDateTime(row.SlitStartTime, out var start, out var startText))
            {
                result.Errors.Add($"{slitLabel}: Slit Start Time must be {SapDateFormat}.");
                continue;
            }

            if (!TryParseSapDateTime(row.SlitFinishTime, out var finish, out var finishText))
            {
                result.Errors.Add($"{slitLabel}: Slit Finish Time must be {SapDateFormat}.");
                continue;
            }

            if (string.IsNullOrWhiteSpace(po))
                continue;

            var key = po + "|" + slitNo;
            if (!slitKeys.Add(key))
                result.Errors.Add($"{slitLabel}: Duplicate slit {slitNo} for PO {po} in this request.");

            var fileName = !string.IsNullOrWhiteSpace(row.SourceFileName)
                ? Path.GetFileName(row.SourceFileName.Trim())
                : BuildOutputFileName(slitNo, start, po);
            list.Add(new PoChangeRemediationValidatedSlit(
                slitNo,
                row.NdtPipes,
                row.RejectedPipes,
                start,
                finish,
                startText,
                finishText,
                string.IsNullOrWhiteSpace(row.NdtShortLengthPipe) ? null : row.NdtShortLengthPipe.Trim(),
                string.IsNullOrWhiteSpace(row.RejectedShortLengthPipe) ? null : row.RejectedShortLengthPipe.Trim(),
                fileName));
        }

        return list;
    }

    public static string BuildOutputFileName(string slitNo, DateTime slitStart, string poNumber)
    {
        var yyMMdd = slitStart.ToString("yyMMdd", CultureInfo.InvariantCulture);
        return $"{slitNo.Trim()}_{yyMMdd}_{InputSlitCsvParsing.NormalizePo(poNumber)}.csv";
    }

    private static string NormalizeSapDateTimeInput(string? raw)
    {
        var text = (raw ?? string.Empty)
            .Replace('\u00a0', ' ')
            .Replace('\t', ' ')
            .Trim();
        if (text.Length >= 2 && text[0] == '"' && text[^1] == '"')
            text = text[1..^1].Trim();
        while (text.Contains("  ", StringComparison.Ordinal))
            text = text.Replace("  ", " ", StringComparison.Ordinal);
        return text;
    }
}
