using Microsoft.Extensions.Logging;

namespace NdtBundleService.Services;

/// <summary>
/// Worker fill-to-target assignment: stamp final batch from SQL fill pointer.
/// When no incomplete target exists, returns empty (no invented number) so the worker
/// can retry on the next poll or the operator can use Manual Input Slit / Manual Reconcile.
/// </summary>
public sealed class SlitCsvFillAssigner
{
    private readonly ICsvFillService _csvFill;
    private readonly ILogger<SlitCsvFillAssigner> _logger;

    public SlitCsvFillAssigner(ICsvFillService csvFill, ILogger<SlitCsvFillAssigner> logger)
    {
        _csvFill = csvFill;
        _logger = logger;
    }

    /// <summary>
    /// Stamp whole-file pipes onto the oldest incomplete fill target.
    /// When no target exists, returns <see cref="SlitCsvFillAssignResult.AwaitingTarget"/> = true
    /// with no batch (file must not be marked handled so the next poll can retry).
    /// <paramref name="holdWhenNoOpenBundle"/> is retained for call-site compatibility but no longer
    /// writes <c>NDT_Csv_Fill_Hold</c> — recovery is retry + Manual Reconcile / Manual Input Slit.
    /// </summary>
    public async Task<SlitCsvFillAssignResult> AssignAsync(
        string sourceFilePath,
        string poNumber,
        int millNo,
        string? pipeSize,
        int fileNdtPipes,
        bool holdWhenNoOpenBundle,
        CancellationToken cancellationToken)
    {
        if (fileNdtPipes < 0)
            throw new ArgumentOutOfRangeException(nameof(fileNdtPipes));

        var stamped = await _csvFill
            .TryStampFileAsync(poNumber, millNo, pipeSize, fileNdtPipes, cancellationToken)
            .ConfigureAwait(false);

        if (stamped is not null)
        {
            return new SlitCsvFillAssignResult(
                BatchNo: stamped.BundleNo,
                Held: false,
                Stamp: stamped,
                AwaitingTarget: false);
        }

        // No open fill target. Do not invent a batch number and do not write hold/Manual_Review.
        _logger.LogDebug(
            "Fill-to-target: no open bundle for PO {PO} Mill {Mill} file {File} — will retry next poll (or use Manual Input Slit after tag print).",
            InputSlitCsvParsing.NormalizePo(poNumber),
            millNo,
            Path.GetFileName(sourceFilePath));

        // Held kept when holdWhenNoOpenBundle for older call sites; AwaitingTarget is the real signal.
        return new SlitCsvFillAssignResult(
            BatchNo: null,
            Held: holdWhenNoOpenBundle,
            Stamp: null,
            AwaitingTarget: true);
    }
}

/// <summary>Outcome of one worker fill-assignment attempt.</summary>
public sealed record SlitCsvFillAssignResult(
    string? BatchNo,
    bool Held,
    CsvFillStampResult? Stamp,
    bool AwaitingTarget = false);
