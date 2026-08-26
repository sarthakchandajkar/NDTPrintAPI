using Microsoft.Extensions.Logging;

namespace NdtBundleService.Services;

/// <summary>
/// Worker fill-to-target assignment: stamp final batch from SQL fill pointer, or hold unpublished
/// with no invented number when no incomplete target exists.
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
    /// When no target exists and <paramref name="holdWhenNoOpenBundle"/> is true, records a hold and
    /// returns an empty batch (file must stay unpublished).
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
                Stamp: stamped);
        }

        if (!holdWhenNoOpenBundle)
            return new SlitCsvFillAssignResult(BatchNo: null, Held: false, Stamp: null);

        await _csvFill
            .UpsertHoldAsync(
                sourceFilePath,
                poNumber,
                millNo,
                pipeSize,
                CsvFillHoldReason.NoOpenBundle,
                cancellationToken)
            .ConfigureAwait(false);

        _logger.LogInformation(
            "Fill-to-target hold: no open bundle for PO {PO} Mill {Mill} file {File} — unpublished, no invented number.",
            InputSlitCsvParsing.NormalizePo(poNumber),
            millNo,
            Path.GetFileName(sourceFilePath));

        return new SlitCsvFillAssignResult(BatchNo: null, Held: true, Stamp: null);
    }
}

/// <summary>Outcome of one worker fill-assignment attempt.</summary>
public sealed record SlitCsvFillAssignResult(string? BatchNo, bool Held, CsvFillStampResult? Stamp);
