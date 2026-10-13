using Microsoft.Extensions.Logging;

namespace NdtBundleService.Services;

/// <summary>
/// Worker fill-to-target assignment: stamp final batch from SQL fill pointer.
/// When no incomplete target exists, optionally seeds a <c>CsvAdvance</c> stamp (new PO / WIP lag),
/// otherwise returns empty so the worker can retry on the next poll.
/// </summary>
public sealed class SlitCsvFillAssigner
{
    private readonly ICsvFillService _csvFill;
    private readonly IMillSequenceService? _millSequence;
    private readonly IFormationChartProvider? _formationChartProvider;
    private readonly IWipBundleRunningPoProvider? _wipRunningPo;
    private readonly ILogger<SlitCsvFillAssigner> _logger;

    public SlitCsvFillAssigner(
        ICsvFillService csvFill,
        IMillSequenceService millSequence,
        IFormationChartProvider formationChartProvider,
        IWipBundleRunningPoProvider wipRunningPo,
        ILogger<SlitCsvFillAssigner> logger)
    {
        _csvFill = csvFill;
        _millSequence = millSequence;
        _formationChartProvider = formationChartProvider;
        _wipRunningPo = wipRunningPo;
        _logger = logger;
    }

    /// <summary>Test helper: seed path disabled.</summary>
    public SlitCsvFillAssigner(ICsvFillService csvFill, ILogger<SlitCsvFillAssigner> logger)
    {
        _csvFill = csvFill;
        _millSequence = null;
        _formationChartProvider = null;
        _wipRunningPo = null;
        _logger = logger;
    }

    /// <summary>
    /// Stamp whole-file pipes onto the oldest incomplete fill target.
    /// When no target exists and <paramref name="seedOpenFillIfMissing"/> is true, open a
    /// provisional <c>CsvAdvance</c> stamp for this PO/mill and retry once (WIP-lag / 1-slit seed).
    /// Closed-PO call sites must pass <c>seedOpenFillIfMissing: false</c>.
    /// </summary>
    public async Task<SlitCsvFillAssignResult> AssignAsync(
        string sourceFilePath,
        string poNumber,
        int millNo,
        string? pipeSize,
        int fileNdtPipes,
        bool holdWhenNoOpenBundle,
        CancellationToken cancellationToken,
        bool seedOpenFillIfMissing = false)
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

        if (seedOpenFillIfMissing &&
            _millSequence is { IsEnabled: true } &&
            _formationChartProvider is not null &&
            millNo is >= 1 and <= 4 &&
            await ShouldSeedForRunningPoAsync(poNumber, millNo, cancellationToken).ConfigureAwait(false))
        {
            var seeded = await TrySeedOpenFillAndStampAsync(
                    sourceFilePath,
                    poNumber,
                    millNo,
                    pipeSize,
                    fileNdtPipes,
                    cancellationToken)
                .ConfigureAwait(false);
            if (seeded is not null)
                return seeded;
        }

        _logger.LogDebug(
            "Fill-to-target: no open bundle for PO {PO} Mill {Mill} file {File} — will retry next poll (or use Manual Input Slit after tag print).",
            InputSlitCsvParsing.NormalizePo(poNumber),
            millNo,
            Path.GetFileName(sourceFilePath));

        return new SlitCsvFillAssignResult(
            BatchNo: null,
            Held: holdWhenNoOpenBundle,
            Stamp: null,
            AwaitingTarget: true);
    }

    /// <summary>
    /// Seed only for the mill's current WIP running PO. Prevents backlog Input Slit files
    /// (other POs / old dates) from burning Mill_Sequence numbers.
    /// </summary>
    private async Task<bool> ShouldSeedForRunningPoAsync(
        string poNumber,
        int millNo,
        CancellationToken cancellationToken)
    {
        if (_wipRunningPo is null)
            return false;

        if (_wipRunningPo.IsWaitingForNewWipAfterPoEnd(millNo))
            return false;

        var running = await _wipRunningPo
            .TryGetRunningPoForMillAsync(millNo, cancellationToken)
            .ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(running))
            return false;

        return InputSlitCsvParsing.PoEquals(running, poNumber);
    }

    private async Task<SlitCsvFillAssignResult?> TrySeedOpenFillAndStampAsync(
        string sourceFilePath,
        string poNumber,
        int millNo,
        string? pipeSize,
        int fileNdtPipes,
        CancellationToken cancellationToken)
    {
        try
        {
            var formation = await _formationChartProvider!
                .GetFormationChartAsync(cancellationToken)
                .ConfigureAwait(false);
            var provisional = Math.Max(1, FormationChartLookup.ResolveThreshold(formation, pipeSize));
            var opened = await _millSequence!
                .TryOpenCsvAdvanceStampTargetAsync(poNumber, millNo, provisional, slitNo: null, cancellationToken)
                .ConfigureAwait(false);

            if (opened is null)
                return null;

            _logger.LogInformation(
                "Seeded CSV-advance stamp {BundleNo} for PO {PO} Mill {Mill} (file {File}, provisionalTarget={Target}).",
                opened.Value.Formatted,
                InputSlitCsvParsing.NormalizePo(poNumber),
                millNo,
                Path.GetFileName(sourceFilePath),
                provisional);

            var stamped = await _csvFill
                .TryStampFileAsync(poNumber, millNo, pipeSize, fileNdtPipes, cancellationToken)
                .ConfigureAwait(false);

            if (stamped is null)
                return null;

            return new SlitCsvFillAssignResult(
                BatchNo: stamped.BundleNo,
                Held: false,
                Stamp: stamped,
                AwaitingTarget: false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "Seed open-fill failed for PO {PO} Mill {Mill} file {File}.",
                InputSlitCsvParsing.NormalizePo(poNumber),
                millNo,
                Path.GetFileName(sourceFilePath));
            return null;
        }
    }
}

/// <summary>Outcome of one worker fill-assignment attempt.</summary>
public sealed record SlitCsvFillAssignResult(
    string? BatchNo,
    bool Held,
    CsvFillStampResult? Stamp,
    bool AwaitingTarget = false);
