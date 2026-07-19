using System.Globalization;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services.PoLifecycle;

namespace NdtBundleService.Services;

/// <summary>
/// Background service that periodically scans the Input Slit CSV folder,
/// feeds records into the bundle engine, and reacts to PO-end signals.
/// When <see cref="NdtBundleOptions.BackfillReconciliationEnabled"/> is true (and SQL is on),
/// startup/periodic reconcile ingests inbox files absent from <c>Input_Slit_Row</c> within the lookback window.
/// When disabled or SQL is off, pre-existing files are baseline-seeded (historical behavior).
/// Inbox files may have no extension (SAP) or <c>.csv</c>; only reads them—never moves or deletes source files in <see cref="NdtBundleOptions.InputSlitFolder"/>.
/// </summary>
public sealed class SlitMonitoringWorker : BackgroundService
{
    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly IBundleEngine _bundleEngine;
    private readonly IBundleOutputWriter _outputWriter;
    private readonly INdtBatchStateService _batchState;
    private readonly INdtBundleRuntimeStateStore _runtimeState;
    private readonly PlcPoEndPollHandler _plcPoEndPollHandler;
    private readonly ITraceabilityRepository _traceability;
    private readonly INdtBundleRepository _bundleRepository;
    private readonly ISqlTraceabilityWriteTracker _sqlWriteTracker;
    private readonly IWipBundleRunningPoProvider _wipRunningPo;
    private readonly IMillNdtCountReader _millNdtCountReader;
    private readonly IPoPlanWipEnrichmentProvider _poPlanWipEnrichment;
    private readonly IPipeSizeProvider _pipeSizeProvider;
    private readonly IMillBundleStateLock _millBundleStateLock;
    private readonly IPoLifecycleService _poLifecycle;
    private readonly INdtTagPrinter _zplTagPrinter;
    private readonly ILogger<SlitMonitoringWorker> _logger;

    // Per input path: last LastWriteTimeUtc we treated as fully handled (seed baseline or successful run).
    // Same path with a newer timestamp (SAP overwrite / same-name export) is processed again; unchanged files are skipped.
    private readonly Dictionary<string, DateTime> _inputSlitLastHandledWriteUtc = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Paths queued by F-5 reconcile that must apply the F-5.2 double-print / Manual_Review guard.</summary>
    private readonly HashSet<string> _backfillCandidatePaths = new(StringComparer.OrdinalIgnoreCase);

    private DateTime _lastBackfillReconcileUtc = DateTime.MinValue;

    /// <summary>Plc-mill deferral backoff so one gated/deferred file does not HOL-block the inbox every poll.</summary>
    private readonly InputSlitFileRetryTracker _fileRetryTracker = new();

    public SlitMonitoringWorker(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        IBundleEngine bundleEngine,
        IBundleOutputWriter outputWriter,
        INdtBatchStateService batchState,
        INdtBundleRuntimeStateStore runtimeState,
        PlcPoEndPollHandler plcPoEndPollHandler,
        ITraceabilityRepository traceability,
        INdtBundleRepository bundleRepository,
        ISqlTraceabilityWriteTracker sqlWriteTracker,
        IWipBundleRunningPoProvider wipRunningPo,
        IMillNdtCountReader millNdtCountReader,
        IPoPlanWipEnrichmentProvider poPlanWipEnrichment,
        IPipeSizeProvider pipeSizeProvider,
        IMillBundleStateLock millBundleStateLock,
        IPoLifecycleService poLifecycle,
        INdtTagPrinter zplTagPrinter,
        ILogger<SlitMonitoringWorker> logger)
    {
        _optionsMonitor = optionsMonitor;
        _bundleEngine = bundleEngine;
        _outputWriter = outputWriter;
        _batchState = batchState;
        _runtimeState = runtimeState;
        _plcPoEndPollHandler = plcPoEndPollHandler;
        _traceability = traceability;
        _bundleRepository = bundleRepository;
        _sqlWriteTracker = sqlWriteTracker;
        _wipRunningPo = wipRunningPo;
        _millNdtCountReader = millNdtCountReader;
        _poPlanWipEnrichment = poPlanWipEnrichment;
        _pipeSizeProvider = pipeSizeProvider;
        _millBundleStateLock = millBundleStateLock;
        _poLifecycle = poLifecycle;
        _zplTagPrinter = zplTagPrinter;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!await WaitForInputSlitFolderExistsAsync(stoppingToken).ConfigureAwait(false))
            return;

        if (stoppingToken.IsCancellationRequested)
            return;

        var inbox = (_optionsMonitor.CurrentValue.InputSlitFolder ?? string.Empty).Trim();
        _logger.LogInformation("SlitMonitoringWorker started. Watching folder {Folder}", inbox);

        await _runtimeState.EnsureInitializedAsync(stoppingToken).ConfigureAwait(false);

        await InitializeInputSlitBaselineAsync(stoppingToken).ConfigureAwait(false);

        var outputFolder = (_optionsMonitor.CurrentValue.OutputBundleFolder ?? string.Empty).Trim();
        if (string.IsNullOrEmpty(outputFolder))
        {
            _logger.LogWarning(
                "NdtBundle:OutputBundleFolder is not set. NDT Input Slit CSV files (with NDT Batch No) will not be written. " +
                "Set it to e.g. Z:\\To SAP\\TM\\NDT\\NDT Input Slit\\Input Slit.");
        }
        else
        {
            _logger.LogInformation(
                "NDT Input Slit output CSV folder (NdtBundle:OutputBundleFolder): {Folder}",
                outputFolder);
        }

        _logger.LogInformation(
            "NDT Input Slit processing limited to mill(s): {Mills}.",
            FormatInputSlitProcessMills(_optionsMonitor.CurrentValue));

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await ProcessNewSlitFilesAsync(stoppingToken).ConfigureAwait(false);

                if (ShouldRunPeriodicBackfillReconcile(_optionsMonitor.CurrentValue))
                    await ReconcileInputSlitInboxAsync(stoppingToken).ConfigureAwait(false);

                if (!IsPlcHandshakeEnabled())
                    await _plcPoEndPollHandler.PollAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error while processing slit files or PLC signals.");
            }

            var delaySeconds = Math.Max(1, _optionsMonitor.CurrentValue.PollIntervalSeconds);
            await Task.Delay(TimeSpan.FromSeconds(delaySeconds), stoppingToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Waits until <see cref="NdtBundleOptions.InputSlitFolder"/> exists. Mapped drives (e.g. Z:\) often appear only after
    /// user logon; Local System never sees them—use a service account with the drive or a UNC path.
    /// </summary>
    private async Task<bool> WaitForInputSlitFolderExistsAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var folder = (_optionsMonitor.CurrentValue.InputSlitFolder ?? string.Empty).Trim();
            if (string.IsNullOrEmpty(folder))
            {
                _logger.LogWarning("InputSlitFolder is not configured. SlitMonitoringWorker will not process files.");
                return false;
            }

            try
            {
                if (Directory.Exists(folder))
                    return true;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error while checking Input Slit folder {Folder}.", folder);
            }

            _logger.LogWarning(
                "InputSlitFolder is not reachable: {Folder}. Mapped drives (Z:\\) are per-user—run the service under that user or switch to a UNC path. Retrying in 10s.",
                folder);
            await Task.Delay(TimeSpan.FromSeconds(10), stoppingToken).ConfigureAwait(false);
        }

        return false;
    }

    /// <summary>
    /// F-5: reconcile against <c>Input_Slit_Row</c> when enabled+SQL; otherwise historical baseline seed.
    /// </summary>
    private async Task InitializeInputSlitBaselineAsync(CancellationToken cancellationToken)
    {
        var o = _optionsMonitor.CurrentValue;
        if (!o.BackfillReconciliationEnabled || !SqlTraceabilityConnection.IsSqlEnabled(o))
        {
            if (o.BackfillReconciliationEnabled && !SqlTraceabilityConnection.IsSqlEnabled(o))
            {
                _logger.LogWarning(
                    "BackfillReconciliationEnabled=true but SQL is disabled; falling back to legacy Input Slit seed baseline.");
            }

            SeedPreExistingInputSlitCsvsAsProcessed();
            return;
        }

        await ReconcileInputSlitInboxAsync(cancellationToken).ConfigureAwait(false);
    }

    private bool ShouldRunPeriodicBackfillReconcile(NdtBundleOptions o)
    {
        if (!o.BackfillReconciliationEnabled || !SqlTraceabilityConnection.IsSqlEnabled(o))
            return false;

        var minutes = Math.Max(1, o.ReconcileIntervalMinutes);
        return DateTime.UtcNow - _lastBackfillReconcileUtc >= TimeSpan.FromMinutes(minutes);
    }

    /// <summary>
    /// Enumerate inbox within lookback; mark already-imported versions handled; queue absent versions for ingest (F-5).
    /// </summary>
    internal async Task ReconcileInputSlitInboxAsync(CancellationToken cancellationToken)
    {
        var o = _optionsMonitor.CurrentValue;
        var folder = (o.InputSlitFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return;

        var lookbackHours = Math.Max(1, o.BackfillLookbackHours);
        var lookbackCutoff = DateTime.UtcNow.AddHours(-lookbackHours);
        var minUtc = SourceFileEligibility.ParseMinUtc(o);
        DateTime? effectiveMin = lookbackCutoff;
        if (minUtc.HasValue && minUtc.Value > effectiveMin.Value)
            effectiveMin = minUtc;

        var scanned = 0;
        var alreadyImported = 0;
        var queued = 0;
        var outsideLookback = 0;

        foreach (var path in InputSlitInboxEnumeration.EnumerateFiles(folder))
        {
            cancellationToken.ThrowIfCancellationRequested();
            scanned++;
            var full = Path.GetFullPath(path);
            DateTime lwUtc;
            try
            {
                lwUtc = File.GetLastWriteTimeUtc(full);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Input Slit reconcile: could not read LastWriteTimeUtc for {File}", full);
                continue;
            }

            if (!SourceFileEligibility.IncludeFileUtc(lwUtc, effectiveMin))
            {
                _inputSlitLastHandledWriteUtc[full] = lwUtc;
                _backfillCandidatePaths.Remove(full);
                outsideLookback++;
                continue;
            }

            var imported = await _traceability
                .IsInputSlitFileVersionImportedAsync(full, lwUtc, cancellationToken)
                .ConfigureAwait(false);
            if (imported)
            {
                _inputSlitLastHandledWriteUtc[full] = lwUtc;
                _backfillCandidatePaths.Remove(full);
                alreadyImported++;
                continue;
            }

            // Absent from SQL (or newer than stored write) → process via normal poll with F-5.2 guard.
            _inputSlitLastHandledWriteUtc.Remove(full);
            _backfillCandidatePaths.Add(full);
            queued++;
        }

        _lastBackfillReconcileUtc = DateTime.UtcNow;
        _logger.LogInformation(
            "Input Slit reconcile: scanned {Scanned}, already imported {Imported}, queued for backfill {Queued}, outside lookback/min-write {Outside} (lookbackHours={Hours}).",
            scanned,
            alreadyImported,
            queued,
            outsideLookback,
            lookbackHours);
    }

    /// <summary>
    /// Records each existing slit inbox file (no extension or <c>.csv</c>) with its current LastWriteTimeUtc so we do not backlog-generate NDT outputs for slits
    /// that completed before this process started. Overwrites of the same path (newer timestamp) are picked up on later polls.
    /// </summary>
    private void SeedPreExistingInputSlitCsvsAsProcessed()
    {
        var folder = (_optionsMonitor.CurrentValue.InputSlitFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return;

        foreach (var path in InputSlitInboxEnumeration.EnumerateFiles(folder))
        {
            var full = Path.GetFullPath(path);
            try
            {
                _inputSlitLastHandledWriteUtc[full] = File.GetLastWriteTimeUtc(full);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Input Slit seed: could not read LastWriteTimeUtc for {File}", full);
            }
        }

        var n = _inputSlitLastHandledWriteUtc.Count;
        if (n > 0)
        {
            _logger.LogInformation(
                "Input Slit: recorded {Count} slit file(s) already in {Folder} at service start (no NDT output for those versions). New paths or same path with a newer LastWriteTimeUtc will be processed.",
                n,
                folder);
        }
        else
        {
            _logger.LogInformation("Input Slit: no pre-existing slit files in {Folder} at service start.", folder);
        }
    }

    private async Task ProcessNewSlitFilesAsync(CancellationToken cancellationToken)
    {
        var o = _optionsMonitor.CurrentValue;
        var inputFolder = (o.InputSlitFolder ?? string.Empty).Trim();
        if (string.IsNullOrEmpty(inputFolder) || !Directory.Exists(inputFolder))
        {
            _logger.LogWarning(
                "Input Slit folder is not available this poll cycle: {Folder}. Skipping until it is reachable again.",
                string.IsNullOrEmpty(inputFolder) ? "(not configured)" : inputFolder);
            return;
        }

        IEnumerable<string> filesEnumerable;
        try
        {
            filesEnumerable = InputSlitInboxEnumeration.EnumerateFiles(inputFolder);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Cannot enumerate slit files in Input Slit folder {Folder}.", inputFolder);
            return;
        }

        var minUtc = SourceFileEligibility.ParseMinUtc(o);
        var live = o.MillSlitLive;

        foreach (var file in filesEnumerable)
        {
            var fileFull = Path.GetFullPath(file);
            DateTime lwUtc;
            try
            {
                lwUtc = File.GetLastWriteTimeUtc(fileFull);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Input Slit: could not read LastWriteTimeUtc for {File}", fileFull);
                continue;
            }

            if (_inputSlitLastHandledWriteUtc.TryGetValue(fileFull, out var lastHandledUtc) && lwUtc <= lastHandledUtc)
                continue;

            if (_fileRetryTracker.ShouldSkip(fileFull, DateTime.UtcNow))
                continue;

            if (!SourceFileEligibility.IncludeFileUtc(lwUtc, minUtc))
            {
                if (minUtc.HasValue)
                {
                    _logger.LogWarning(
                        "Skipping Input Slit file {File}: LastWriteUtc {LastWrite:o} is before NdtBundle:MinSourceFileLastWriteUtc {Min:o}. Clear or update MinSourceFileLastWriteUtc if this file should be processed.",
                        fileFull,
                        lwUtc,
                        minUtc.Value);
                }

                continue;
            }

            _logger.LogInformation("Processing Input Slit file {File}", fileFull);

            try
            {
                var (headerLine, rows, ndtColumnIndex, poColumnIndex) = await ReadSlitFileWithRawLinesAsync(fileFull, cancellationToken).ConfigureAwait(false);

                var qualifyingForLive = rows
                    .Select(r => r.Record)
                    .Where(r => r is not null
                                && live.Enabled
                                && r.MillNo == live.ApplyToMillNo
                                && !string.IsNullOrWhiteSpace(r.PoNumber))
                    .ToList();

                int? plcNdt = null;
                var liveSingleSlitRow = live.Enabled && qualifyingForLive.Count == 1;
                if (liveSingleSlitRow)
                {
                    plcNdt = await _millNdtCountReader.TryReadNdtPipesCountAsync(cancellationToken).ConfigureAwait(false);
                    if (plcNdt.HasValue)
                    {
                        _logger.LogInformation(
                            "MillSlitLive: using PLC NDT={Ndt} for mill {Mill} (file {File}).",
                            plcNdt.Value,
                            live.ApplyToMillNo,
                            Path.GetFileName(fileFull));
                    }
                    else
                    {
                        _logger.LogWarning(
                            "MillSlitLive: PLC NDT read failed; using CSV NDT count for mill {Mill} (file {File}).",
                            live.ApplyToMillNo,
                            Path.GetFileName(fileFull));
                    }
                }

                if (live.Enabled && qualifyingForLive.Count > 1)
                {
                    _logger.LogWarning(
                        "MillSlitLive: file {File} has {N} slit row(s) for mill {M}; PLC NDT applies only to a single slit row per file. Using CSV counts for this file.",
                        fileFull,
                        qualifyingForLive.Count,
                        live.ApplyToMillNo);
                }

                // Build output content: same format as input with one extra column "NDT Batch No".
                var outputLines = new List<string> { headerLine.TrimEnd() + ",NDT Batch No" };
                var inputRowsForSql = new List<(InputSlitRecord Record, int SourceRowNumber)>();
                var outputRowsForSql = new List<(InputSlitRecord Record, string NdtBatchNo, int SourceRowNumber)>();

                IReadOnlyDictionary<string, PoPlanWipRow> wipByPo =
                    _poPlanWipEnrichment.TryGetCachedEnrichment()?.ByPo
                    ?? (await _poPlanWipEnrichment.GetEnrichmentAsync(cancellationToken).ConfigureAwait(false)).ByPo;
                IReadOnlyDictionary<string, string> pipeSizeByPo =
                    _pipeSizeProvider.TryGetCachedPipeSizes()
                    ?? await _pipeSizeProvider.GetPipeSizeByPoAsync(cancellationToken).ConfigureAwait(false);

                var isBackfill = _backfillCandidatePaths.Contains(fileFull);
                var backfillCoverage = BackfillCoverageKind.None;
                if (isBackfill)
                {
                    var eligibleForCoverage = rows
                        .Select(r => r.Record)
                        .Where(r => r is not null && IsMillAllowedForNdtInputSlit(o, r!.MillNo))
                        .Cast<InputSlitRecord>()
                        .ToList();
                    backfillCoverage = InputSlitBackfillCoverage.Evaluate(fileFull, eligibleForCoverage, o, _logger);
                    if (backfillCoverage == BackfillCoverageKind.None)
                    {
                        foreach (var er in eligibleForCoverage
                                     .GroupBy(r => (Po: InputSlitCsvParsing.NormalizePo(r.PoNumber), r.MillNo)))
                        {
                            if (await _bundleRepository
                                    .HasPrintedBundleForPoAsync(er.Key.MillNo, er.Key.Po, cancellationToken)
                                    .ConfigureAwait(false))
                            {
                                backfillCoverage = BackfillCoverageKind.Ambiguous;
                                break;
                            }
                        }
                    }

                    _logger.LogInformation(
                        "Input Slit backfill {File}: coverage={Coverage}.",
                        Path.GetFileName(fileFull),
                        backfillCoverage);
                }

                var sourceRowNumber = 2; // CSV header is row 1
                var anyRowBundled = false;
                var anyEligibleMillRow = false;
                var pendingOrphanClose = new HashSet<(string Po, int Mill)>();
                var slitSumByPoMill = new Dictionary<(string Po, int Mill), int>();
                var manualReviewFlagged = new HashSet<(string Po, int Mill)>();
                foreach (var row in rows)
                {
                    if (row.Record is null)
                    {
                        // Keep source content intact; append blank batch for non-parseable rows.
                        outputLines.Add(row.RawLine.TrimEnd() + ",");
                        sourceRowNumber++;
                        continue;
                    }

                    var record = row.Record;
                    if (!IsMillAllowedForNdtInputSlit(o, record.MillNo))
                    {
                        _logger.LogDebug(
                            "Mill {Mill}: NDT Input Slit processing disabled (InputSlitProcessMills); skipping slit row in {File}.",
                            record.MillNo,
                            Path.GetFileName(fileFull));
                        sourceRowNumber++;
                        continue;
                    }

                    anyEligibleMillRow = true;
                    var runningPoFromWip = await _wipRunningPo
                        .TryGetRunningPoForMillAsync(record.MillNo, cancellationToken)
                        .ConfigureAwait(false);
                    var waitingForWip = _wipRunningPo.IsWaitingForNewWipAfterPoEnd(record.MillNo);
                    var poEndSource = MillPoEndSourceResolver.ForMill(record.MillNo, o);
                    // F-3: Plc mills with a valid slit-file PO continue bundling during WIP wait (config-gated).
                    // File mills keep the historical hard-stop byte-for-byte.
                    if (SlitWipBundlingGate.ShouldSkipBundling(
                            waitingForWip,
                            runningPoFromWip,
                            record.PoNumber,
                            poEndSource,
                            o.BundleSlitRowsWithFilePoDuringWipWait))
                    {
                        _logger.LogInformation(
                            "Mill {Mill}: waiting for new WIP bundle file in TM Bundle folder after PO end; skipping bundling for slit row in {File}.",
                            record.MillNo,
                            Path.GetFileName(fileFull));
                        outputLines.Add(row.RawLine.TrimEnd() + ",");
                        sourceRowNumber++;
                        continue;
                    }

                    var useLiveThisRow = liveSingleSlitRow
                                         && record.MillNo == live.ApplyToMillNo
                                         && !string.IsNullOrWhiteSpace(record.PoNumber);

                    var effectivePo = SlitEffectivePoResolver.Resolve(record.PoNumber, runningPoFromWip);
                    if (!string.IsNullOrWhiteSpace(runningPoFromWip)
                        && !string.IsNullOrWhiteSpace(record.PoNumber)
                        && !InputSlitCsvParsing.PoEquals(record.PoNumber, runningPoFromWip))
                    {
                        _logger.LogInformation(
                            "Mill {Mill}: Input Slit PO {SlitPo} used for NDT output (WIP bundle PO {WipPo} ignored for slit file {File}).",
                            record.MillNo,
                            record.PoNumber,
                            runningPoFromWip,
                            Path.GetFileName(fileFull));
                    }

                    var effectiveNdt = useLiveThisRow && plcNdt.HasValue
                        ? plcNdt.Value
                        : record.NdtPipes;

                    var effectiveRecord = new InputSlitRecord
                    {
                        PoNumber = effectivePo,
                        SlitNo = record.SlitNo,
                        NdtPipes = effectiveNdt,
                        RejectedPipes = record.RejectedPipes,
                        SlitStartTime = record.SlitStartTime,
                        SlitFinishTime = record.SlitFinishTime,
                        MillNo = record.MillNo,
                        NdtShortLengthPipe = record.NdtShortLengthPipe,
                        RejectedShortLengthPipe = record.RejectedShortLengthPipe,
                    };

                    // MillSlitLive: PLC value is for live display in the output NDT Pipes column only.
                    // NDT Batch No, bundle state, and tag printing follow the Input Slit CSV row (slit end),
                    // so thresholds apply to whole-slit totals (e.g. 9 + 6 → one label for 15), not PLC step deltas.
                    var bundleNdtPipes = useLiveThisRow && plcNdt.HasValue ? record.NdtPipes : effectiveRecord.NdtPipes;
                    var bundleRecord = CloneRecordWithNdt(effectiveRecord, bundleNdtPipes);

                    BackfillBundlingAction backfillAction = BackfillBundlingAction.NormalBundle;
                    if (isBackfill)
                    {
                        var phase = _poLifecycle.GetPhase(bundleRecord.MillNo, bundleRecord.PoNumber);
                        backfillAction = InputSlitBackfillPolicy.Decide(
                            backfillCoverage,
                            phase,
                            poEndSource,
                            o.AutoCloseOrphanBundles);
                    }

                    if (backfillAction is BackfillBundlingAction.TraceabilityOnly or BackfillBundlingAction.ManualReview)
                    {
                        if (backfillAction == BackfillBundlingAction.ManualReview)
                        {
                            var key = (InputSlitCsvParsing.NormalizePo(bundleRecord.PoNumber), bundleRecord.MillNo);
                            if (manualReviewFlagged.Add(key))
                            {
                                _logger.LogWarning(
                                    "Backfill Manual_Review: PO {PO} Mill {Mill} file {File} (coverage={Coverage}, no auto-print).",
                                    key.Item1,
                                    key.Item2,
                                    Path.GetFileName(fileFull),
                                    backfillCoverage);
                                await _bundleRepository
                                    .MarkManualReviewAsync(key.Item1, key.Item2, cancellationToken)
                                    .ConfigureAwait(false);
                            }
                        }

                        var rawTrace = row.RawLine;
                        if (useLiveThisRow && plcNdt.HasValue && ndtColumnIndex >= 0)
                            rawTrace = InputSlitCsvParsing.ReplaceFieldAtIndex(row.RawLine, ndtColumnIndex, effectiveNdt.ToString(CultureInfo.InvariantCulture));
                        if (poColumnIndex >= 0
                            && !string.IsNullOrWhiteSpace(effectivePo)
                            && !InputSlitCsvParsing.PoEquals(record.PoNumber, effectivePo))
                        {
                            rawTrace = InputSlitCsvParsing.ReplaceFieldAtIndex(rawTrace, poColumnIndex, effectivePo);
                        }

                        outputLines.Add(rawTrace.TrimEnd() + ",");
                        inputRowsForSql.Add((effectiveRecord, sourceRowNumber));
                        // Still count as "bundled" for SQL/output write so Input_Slit_Row is recorded.
                        anyRowBundled = true;
                        sourceRowNumber++;
                        continue;
                    }

                    var (pipeType, pipeSize) = ResolvePipeInfoForPo(bundleRecord.PoNumber, wipByPo, pipeSizeByPo);
                    var omitBatch = NdtBatchNumberRules.ShouldOmitNdtBatchNumber(pipeType, pipeSize);

                    string ndtBatchNoFormatted;
                    if (omitBatch)
                    {
                        ndtBatchNoFormatted = string.Empty;
                    }
                    else
                    {
                        var bundleLock = await _millBundleStateLock
                            .AcquireAsync(bundleRecord.MillNo, cancellationToken)
                            .ConfigureAwait(false);
                        try
                        {
                            var awaitingPlc = await _bundleRepository
                                .TryGetAwaitingPlcReconBatchAsync(
                                    bundleRecord.PoNumber,
                                    bundleRecord.MillNo,
                                    cancellationToken)
                                .ConfigureAwait(false);

                            if (awaitingPlc is { } awaiting)
                            {
                                // F-2: attach late CSV rows to the PLC-closed bundle; do not open a new sequence
                                // or re-accumulate size counts toward a second close.
                                ndtBatchNoFormatted = awaiting.BundleNo;
                                var reconKey = (
                                    InputSlitCsvParsing.NormalizePo(bundleRecord.PoNumber),
                                    bundleRecord.MillNo);
                                slitSumByPoMill.TryGetValue(reconKey, out var prevSum);
                                slitSumByPoMill[reconKey] = prevSum + Math.Max(0, bundleRecord.NdtPipes);
                            }
                            else
                            {
                                var (bn, _, _) = await _batchState
                                    .GetBatchForRecordAsync(
                                        bundleRecord.PoNumber,
                                        bundleRecord.MillNo,
                                        bundleRecord.NdtPipes,
                                        cancellationToken,
                                        pipeSize)
                                    .ConfigureAwait(false);

                                int? closedPrintedBatch = null;
                                if (bundleRecord.NdtPipes > 0)
                                {
                                    try
                                    {
                                        await _bundleEngine.ProcessSlitRecordAsync(
                                            bundleRecord,
                                            async (contextRecord, batchNo, totalNdtPcs) =>
                                            {
                                                if (totalNdtPcs <= 0)
                                                    return;

                                                closedPrintedBatch = batchNo;
                                                try
                                                {
                                                    await _outputWriter.WriteBundleAsync(contextRecord, batchNo, totalNdtPcs, cancellationToken).ConfigureAwait(false);
                                                    _logger.LogInformation(
                                                        "Bundle output completed for {BatchNo} ({Pcs} pcs).",
                                                        FormatNdtBatchNo(batchNo, contextRecord.MillNo),
                                                        totalNdtPcs);
                                                }
                                                catch (Exception ex)
                                                {
                                                    _logger.LogError(ex, "Tag print failed for bundle {BatchNo}.", FormatNdtBatchNo(batchNo, contextRecord.MillNo));
                                                }
                                            },
                                            cancellationToken,
                                            pipeSize).ConfigureAwait(false);

                                        if (backfillAction == BackfillBundlingAction.OrphanAutoClose)
                                        {
                                            pendingOrphanClose.Add((
                                                InputSlitCsvParsing.NormalizePo(bundleRecord.PoNumber),
                                                bundleRecord.MillNo));
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        _logger.LogError(ex, "Bundle engine failed for record in {File}; output CSV was already written.", fileFull);
                                    }
                                }

                                var batchForOutput = closedPrintedBatch ?? bn;
                                ndtBatchNoFormatted = batchForOutput > 0
                                    ? FormatNdtBatchNo(batchForOutput, effectiveRecord.MillNo)
                                    : string.Empty;
                            }
                        }
                        finally
                        {
                            bundleLock.Dispose();
                        }
                    }

                    var rawOut = row.RawLine;
                    if (useLiveThisRow && plcNdt.HasValue && ndtColumnIndex >= 0)
                        rawOut = InputSlitCsvParsing.ReplaceFieldAtIndex(row.RawLine, ndtColumnIndex, effectiveNdt.ToString(CultureInfo.InvariantCulture));

                    if (poColumnIndex >= 0
                        && !string.IsNullOrWhiteSpace(effectivePo)
                        && !InputSlitCsvParsing.PoEquals(record.PoNumber, effectivePo))
                    {
                        rawOut = InputSlitCsvParsing.ReplaceFieldAtIndex(rawOut, poColumnIndex, effectivePo);
                    }

                    outputLines.Add(rawOut.TrimEnd() + "," + ndtBatchNoFormatted);
                    inputRowsForSql.Add((effectiveRecord, sourceRowNumber));
                    outputRowsForSql.Add((bundleRecord, ndtBatchNoFormatted, sourceRowNumber));
                    anyRowBundled = true;
                    sourceRowNumber++;
                }

                // F-5.2 / F-4.4: Closed-PO backfill with AutoClose — flush any reopeneds immediately.
                foreach (var (po, mill) in pendingOrphanClose)
                {
                    var bundleLock = await _millBundleStateLock.AcquireAsync(mill, cancellationToken).ConfigureAwait(false);
                    try
                    {
                        await _bundleEngine.HandlePoEndAsync(
                            po,
                            mill,
                            async (contextRecord, batchNo, totalNdtPcs) =>
                            {
                                if (totalNdtPcs <= 0)
                                    return;
                                await _outputWriter
                                    .WriteBundleAsync(contextRecord, batchNo, totalNdtPcs, cancellationToken)
                                    .ConfigureAwait(false);
                                _logger.LogInformation(
                                    "Backfill orphan auto-closed: PO {PO} Mill {Mill} Batch {Batch} NdtPcs {Pcs}.",
                                    po,
                                    mill,
                                    FormatNdtBatchNo(batchNo, mill),
                                    totalNdtPcs);
                            },
                            cancellationToken).ConfigureAwait(false);
                        await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);
                    }
                    finally
                    {
                        bundleLock.Dispose();
                    }
                }

                // Write one output file under OutputBundleFolder using the same name as the input inbox file.
                var outputFolder = (o.OutputBundleFolder ?? string.Empty).Trim();
                string? outputPath = null;
                if (!anyRowBundled)
                {
                    if (!anyEligibleMillRow)
                    {
                        _logger.LogInformation(
                            "Skipping NDT Input Slit output for {File}; no rows for configured mills ({Mills}).",
                            Path.GetFileName(fileFull),
                            FormatInputSlitProcessMills(o));
                        _fileRetryTracker.Clear(fileFull);
                        _inputSlitLastHandledWriteUtc[fileFull] = File.GetLastWriteTimeUtc(fileFull);
                        _backfillCandidatePaths.Remove(fileFull);
                    }
                    else if (ShouldApplyPlcFileRetryBackoff(o, rows))
                    {
                        var (delay, shouldLog, step) = _fileRetryTracker.Park(
                            fileFull,
                            DateTime.UtcNow,
                            o.FileRetryBackoffSeconds);
                        if (shouldLog)
                        {
                            _logger.LogInformation(
                                "Skipping NDT Input Slit output for {File}; no slit rows were bundled. File parked with retry backoff step {Step} ({DelaySeconds}s).",
                                Path.GetFileName(fileFull),
                                step,
                                (int)delay.TotalSeconds);
                        }
                    }
                    else
                    {
                        _logger.LogInformation(
                            "Skipping NDT Input Slit output for {File}; no slit rows were bundled. File will be retried on next poll.",
                            Path.GetFileName(fileFull));
                    }
                }
                else if (!string.IsNullOrWhiteSpace(outputFolder))
                {
                    Directory.CreateDirectory(outputFolder);
                    var outputFileName = Path.GetFileName(fileFull);
                    outputPath = Path.Combine(outputFolder, outputFileName);
                    if (isBackfill
                        && backfillCoverage == BackfillCoverageKind.ExactMatch
                        && File.Exists(outputPath))
                    {
                        _logger.LogInformation(
                            "Backfill ExactMatch: leaving existing NDT Input Slit output unchanged: {Path}",
                            outputPath);
                    }
                    else
                    {
                        await File.WriteAllLinesAsync(outputPath, outputLines, cancellationToken).ConfigureAwait(false);
                        _logger.LogInformation("Wrote bundle CSV: {Path}", outputPath);
                    }
                }

                // Best-effort SQL traceability; CSV flow should not fail if SQL is down.
                if (anyRowBundled)
                {
                    await _traceability
                        .RecordInputSlitRowsAsync(fileFull, inputRowsForSql, cancellationToken, lwUtc)
                        .ConfigureAwait(false);
                    if (outputRowsForSql.Count > 0)
                    {
                        await _traceability
                            .RecordOutputSlitRowsAsync(outputPath ?? fileFull, outputRowsForSql, cancellationToken)
                            .ConfigureAwait(false);
                    }

                    foreach (var batchNo in outputRowsForSql
                                 .Select(r => r.NdtBatchNo)
                                 .Where(static b => !string.IsNullOrWhiteSpace(b))
                                 .Distinct(StringComparer.OrdinalIgnoreCase))
                    {
                        try
                        {
                            await _bundleRepository
                                .TrySyncBundleTotalFromSlitsAsync(batchNo, forceFromSlits: false, cancellationToken)
                                .ConfigureAwait(false);
                        }
                        catch (Exception syncEx)
                        {
                            _logger.LogWarning(
                                syncEx,
                                "Failed to sync NDT_Bundle total from slit sum for batch {BatchNo} after output slit import.",
                                batchNo);
                        }
                    }

                    foreach (var ((po, mill), slitSum) in slitSumByPoMill)
                    {
                        try
                        {
                            var recon = await _bundleRepository
                                .TryReconcilePlcClosedBundleAsync(po, mill, slitSum, cancellationToken)
                                .ConfigureAwait(false);
                            if (recon is null)
                                continue;

                            if (recon.CountDiscrepancy && o.ReprintOnCountMismatch)
                            {
                                await TryReprintOnPlcCsvMismatchAsync(recon, mill, cancellationToken)
                                    .ConfigureAwait(false);
                            }
                        }
                        catch (Exception reconEx)
                        {
                            _logger.LogWarning(
                                reconEx,
                                "PLC CSV recon failed for PO {PO} Mill {Mill} after slit import.",
                                po,
                                mill);
                        }
                    }
                }

                LogSqlWriteFailuresIfAny(fileFull);

                if (anyRowBundled)
                {
                    _fileRetryTracker.Clear(fileFull);
                    _inputSlitLastHandledWriteUtc[fileFull] = File.GetLastWriteTimeUtc(fileFull);
                    _backfillCandidatePaths.Remove(fileFull);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to process Input Slit file {File}", fileFull);
            }
        }
    }

    /// <summary>
    /// Backoff applies when any eligible row belongs to a <c>PoEndSource=Plc</c> mill.
    /// File-mill-only deferrals keep immediate next-poll retry (unchanged).
    /// </summary>
    private static bool ShouldApplyPlcFileRetryBackoff(NdtBundleOptions o, IReadOnlyList<(string RawLine, InputSlitRecord? Record)> rows)
    {
        foreach (var row in rows)
        {
            if (row.Record is null)
                continue;
            if (!IsMillAllowedForNdtInputSlit(o, row.Record.MillNo))
                continue;
            if (MillPoEndSourceResolver.ForMill(row.Record.MillNo, o) == MillPoEndSource.Plc)
                return true;
        }

        return false;
    }

    private void LogSqlWriteFailuresIfAny(string sourceFile)
    {
        if (!SqlTraceabilityConnection.IsSqlEnabled(_optionsMonitor.CurrentValue))
        {
            _logger.LogWarning(
                "SQL traceability is not configured; slit file {File} was written to CSV only (no JazeeraMES_Prod rows). Set NdtBundle:ConnectionString or NdtBundle__ConnectionString.",
                sourceFile);
            return;
        }

        var failures = _sqlWriteTracker.GetRecentResults()
            .Where(r => !r.Success && r.Utc > DateTime.UtcNow.AddMinutes(-2))
            .Take(5)
            .ToList();
        if (failures.Count == 0)
            return;

        _logger.LogError(
            "JazeeraMES_Prod SQL writes failed after processing slit file {File}. Labels/CSV succeeded but database rows were not saved. Latest errors: {Errors}",
            sourceFile,
            string.Join(" | ", failures.Select(f => $"{f.Operation}: {f.Error}")));
    }

    private static InputSlitRecord CloneRecordWithNdt(InputSlitRecord r, int ndtPipes) =>
        new()
        {
            PoNumber = r.PoNumber,
            SlitNo = r.SlitNo,
            NdtPipes = ndtPipes,
            RejectedPipes = r.RejectedPipes,
            SlitStartTime = r.SlitStartTime,
            SlitFinishTime = r.SlitFinishTime,
            MillNo = r.MillNo,
            NdtShortLengthPipe = r.NdtShortLengthPipe,
            RejectedShortLengthPipe = r.RejectedShortLengthPipe,
        };

    private static (string PipeType, string PipeSize) ResolvePipeInfoForPo(
        string poNumber,
        IReadOnlyDictionary<string, PoPlanWipRow> wipByPo,
        IReadOnlyDictionary<string, string> pipeSizeByPo)
    {
        var normalized = InputSlitCsvParsing.NormalizePo(poNumber);
        var pipeType = string.Empty;
        var pipeSize = string.Empty;

        if (wipByPo.TryGetValue(normalized, out var wip))
        {
            pipeType = wip.PipeType ?? string.Empty;
            pipeSize = wip.PipeSize ?? string.Empty;
        }

        if (string.IsNullOrWhiteSpace(pipeSize) && pipeSizeByPo.TryGetValue(normalized, out var fromMap))
            pipeSize = fromMap;

        return (pipeType.Trim(), pipeSize.Trim());
    }

    private async Task TryReprintOnPlcCsvMismatchAsync(
        PlcCsvReconResult recon,
        int millNo,
        CancellationToken cancellationToken)
    {
        try
        {
            await _bundleRepository
                .TrySyncBundleTotalFromSlitsAsync(recon.BundleNo, forceFromSlits: true, cancellationToken)
                .ConfigureAwait(false);
            var bundle = await _bundleRepository
                .GetByBatchNoAsync(recon.BundleNo, cancellationToken)
                .ConfigureAwait(false);
            if (bundle is null || !TryParseEngineSequenceFromBundleNo(recon.BundleNo, out var seq))
            {
                _logger.LogWarning(
                    "ReprintOnCountMismatch: could not load bundle {BundleNo} for corrected reprint.",
                    recon.BundleNo);
                return;
            }

            var pcs = bundle.TotalNdtPcs > 0 ? bundle.TotalNdtPcs : recon.SlitSum;
            var context = new InputSlitRecord
            {
                PoNumber = bundle.PoNumber,
                MillNo = millNo,
                SlitNo = bundle.SlitNo,
                NdtPipes = pcs
            };
            var printed = await _zplTagPrinter
                .PrintBundleTagAsync(context, seq, pcs, isReprint: true, cancellationToken)
                .ConfigureAwait(false);
            _logger.LogWarning(
                "ReprintOnCountMismatch: corrected reprint for {BundleNo} slitSum={SlitSum} plc={PlcTotal} sent={Sent}.",
                recon.BundleNo,
                recon.SlitSum,
                recon.PlcTotal,
                printed);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "ReprintOnCountMismatch failed for {BundleNo}.", recon.BundleNo);
        }
    }

    private static bool TryParseEngineSequenceFromBundleNo(string bundleNo, out int sequence)
    {
        sequence = 0;
        if (string.IsNullOrWhiteSpace(bundleNo) || bundleNo.Length < 5)
            return false;
        return int.TryParse(
            bundleNo.AsSpan(bundleNo.Length - 5),
            NumberStyles.None,
            CultureInfo.InvariantCulture,
            out sequence);
    }

    private static string FormatNdtBatchNo(int sequenceNumber, int millNo)
    {
        var yy = (DateTime.Now.Year % 100).ToString("D2", CultureInfo.InvariantCulture);
        var millDigit = (millNo >= 1 && millNo <= 4) ? millNo.ToString(CultureInfo.InvariantCulture) : "1";
        var seq = sequenceNumber.ToString("D5", CultureInfo.InvariantCulture);
        return "12" + yy + millDigit + seq;
    }

    private static async Task<(string HeaderLine, List<(string RawLine, InputSlitRecord? Record)> Rows, int NdtColumnIndex, int PoColumnIndex)> ReadSlitFileWithRawLinesAsync(
        string path,
        CancellationToken cancellationToken)
    {
        var rows = new List<(string RawLine, InputSlitRecord?)>();

        await using var stream = File.OpenRead(path);
        using var reader = new StreamReader(stream);

        var headerRaw = await reader.ReadLineAsync();
        if (headerRaw is null)
            return (string.Empty, rows, -1, -1);

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

            var line = await reader.ReadLineAsync();
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

            // Preserve all input rows in output. Only rows with a PO can participate in batch computation.
            rows.Add((line, string.IsNullOrWhiteSpace(record.PoNumber) ? null : record));
        }

        return (headerLine, rows, ndtIndex, poIndex);
    }

    /// <summary>
    /// Empty/null <see cref="NdtBundleOptions.InputSlitProcessMills"/> = all mills 1–4.
    /// </summary>
    internal static bool IsMillAllowedForNdtInputSlit(NdtBundleOptions options, int millNo)
    {
        if (millNo is < 1 or > 4)
            return false;

        var mills = options.InputSlitProcessMills;
        if (mills is null || mills.Length == 0)
            return true;

        return mills.Contains(millNo);
    }

    private static string FormatInputSlitProcessMills(NdtBundleOptions options)
    {
        var mills = options.InputSlitProcessMills;
        if (mills is null || mills.Length == 0)
            return "1-4";
        return string.Join(",", mills);
    }

    private static int GetMillNo(string[] cols, int index)
    {
        var raw = GetString(cols, index);
        return InputSlitCsvParsing.TryParseMillNo(raw, out var m) ? m : 0;
    }

    private static int GetIntFlexible(string[] cols, int index)
    {
        if (index < 0 || index >= cols.Length)
            return 0;
        return InputSlitCsvParsing.TryParseIntFlexible(cols[index].Trim(), out var v) ? v : 0;
    }

    private static string GetString(string[] cols, int index)
    {
        if (index < 0 || index >= cols.Length)
            return string.Empty;
        return cols[index].Trim();
    }

    private static DateTime? GetDateTime(string[] cols, int index)
    {
        if (index < 0 || index >= cols.Length)
            return null;
        var raw = cols[index].Trim();
        if (string.IsNullOrEmpty(raw))
            return null;
        return DateTime.TryParse(raw, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var dt) ? dt : null;
    }

    private bool IsPlcHandshakeEnabled() =>
        _optionsMonitor.CurrentValue.PlcHandshake?.Enabled == true;
}
