using System.Globalization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

public enum NdtRebuildSource
{
    InputSlitCsv,
    InputSlitRow
}

public sealed class NdtBundleRebuildRequest
{
    public DateTime FromUtc { get; init; }
    /// <summary>SAP planned production month (e.g. 6 = June). When set, slit rows are filtered by PO Planned Month and slit time within that calendar month.</summary>
    public int? PlannedMonth { get; init; }
    public int? ProductionYear { get; init; }
    public bool DryRun { get; init; } = true;
    public bool PurgeExistingFromDate { get; init; }
    public NdtRebuildSource Source { get; init; } = NdtRebuildSource.InputSlitCsv;
    public IReadOnlyDictionary<int, int>? StartingSequenceByMill { get; init; }
}

public sealed class NdtBundleRebuildResult
{
    public bool DryRun { get; init; }
    public DateTime FromUtc { get; init; }
    public int InputFilesConsidered { get; init; }
    public int SlitRowsReplayed { get; init; }
    public int BundlesClosed { get; init; }
    public int OutputSlitFilesWritten { get; init; }
    public NdtTraceabilityPurgeResult? TraceabilityPurge { get; init; }
    public NdtBundleCsvPurgeResult? CsvPurge { get; init; }
    public IReadOnlyDictionary<int, int> MillMaxSequence { get; init; } = new Dictionary<int, int>();
    public IReadOnlyList<string> Warnings { get; init; } = Array.Empty<string>();
    public int SlitRowsExcluded { get; init; }
    public IReadOnlyList<string> ExcludedSamples { get; init; } = Array.Empty<string>();
    public int? PlannedMonth { get; init; }
    public int? ProductionYear { get; init; }
    public IReadOnlyDictionary<int, string> TargetPoByMill { get; init; } = new Dictionary<int, string>();
    public string Message { get; init; } = string.Empty;
}

public interface INdtBundleRebuildService
{
    Task<NdtBundleRebuildResult> RebuildFromDateAsync(NdtBundleRebuildRequest request, CancellationToken cancellationToken);
}

/// <summary>
/// Replays input slit history from a cutoff date to regenerate NDT batch numbers, CSV outputs, and SQL traceability.
/// Physical labels are not printed when <see cref="NdtBundleOptions.EnableNdtTagZplAndPrint"/> is false.
/// </summary>
public sealed class NdtBundleRebuildService : INdtBundleRebuildService
{
    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly INdtBundleRuntimeStateStore _runtimeState;
    private readonly INdtBatchStateService _batchState;
    private readonly IBundleEngine _bundleEngine;
    private readonly IBundleOutputWriter _outputWriter;
    private readonly IPoEndWorkflowService _poEndWorkflow;
    private readonly ITraceabilityRepository _traceability;
    private readonly INdtBundleRepository _bundleRepository;
    private readonly IPoPlanRegistry _poPlanRegistry;
    private readonly ILogger<NdtBundleRebuildService> _logger;

    public NdtBundleRebuildService(
        IOptionsMonitor<NdtBundleOptions> optionsMonitor,
        INdtBundleRuntimeStateStore runtimeState,
        INdtBatchStateService batchState,
        IBundleEngine bundleEngine,
        IBundleOutputWriter outputWriter,
        IPoEndWorkflowService poEndWorkflow,
        ITraceabilityRepository traceability,
        INdtBundleRepository bundleRepository,
        IPoPlanRegistry poPlanRegistry,
        ILogger<NdtBundleRebuildService> logger)
    {
        _optionsMonitor = optionsMonitor;
        _runtimeState = runtimeState;
        _batchState = batchState;
        _bundleEngine = bundleEngine;
        _outputWriter = outputWriter;
        _poEndWorkflow = poEndWorkflow;
        _traceability = traceability;
        _bundleRepository = bundleRepository;
        _poPlanRegistry = poPlanRegistry;
        _logger = logger;
    }

    private NdtBundleOptions Opt => _optionsMonitor.CurrentValue;

    public async Task<NdtBundleRebuildResult> RebuildFromDateAsync(NdtBundleRebuildRequest request, CancellationToken cancellationToken)
    {
        var warnings = new List<string>();
        NdtTraceabilityPurgeResult? tracePurge = null;
        NdtBundleCsvPurgeResult? csvPurge = null;

        var registry = await _poPlanRegistry.GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
        var plannedMonth = request.PlannedMonth ?? Opt.RebuildPlannedMonth;
        var productionYear = request.ProductionYear
            ?? (plannedMonth.HasValue
                ? ProductionMonthEligibility.ResolveProductionYear(Opt, plannedMonth.Value)
                : (int?)null);

        var loadResult = request.Source switch
        {
            NdtRebuildSource.InputSlitRow => await LoadEventsFromSqlAsync(request, registry, plannedMonth, productionYear, cancellationToken).ConfigureAwait(false),
            _ => await LoadEventsFromCsvAsync(request, registry, plannedMonth, productionYear, cancellationToken).ConfigureAwait(false)
        };

        var events = loadResult.Events;
        var excludedSamples = loadResult.ExcludedSamples;
        var targetPoByMill = plannedMonth.HasValue
            ? registry.GetPrimaryPoByPlannedMonth(plannedMonth.Value)
                .ToDictionary(kv => kv.Key, kv => kv.Value.PoNumber)
            : new Dictionary<int, string>();

        if (request.DryRun)
        {
            var millPreview = events
                .Where(e => e.Record.NdtPipes > 0)
                .GroupBy(e => e.Record.MillNo)
                .ToDictionary(g => g.Key, g => g.Count());

            return new NdtBundleRebuildResult
            {
                DryRun = true,
                FromUtc = request.FromUtc,
                PlannedMonth = plannedMonth,
                ProductionYear = productionYear,
                TargetPoByMill = targetPoByMill,
                InputFilesConsidered = loadResult.FilesConsidered,
                SlitRowsReplayed = events.Count(e => e.Record.NdtPipes > 0),
                SlitRowsExcluded = loadResult.ExcludedCount,
                ExcludedSamples = excludedSamples,
                Message = "Dry run completed. No files or database rows were modified.",
                MillMaxSequence = millPreview,
                Warnings = warnings
            };
        }

        if (request.PurgeExistingFromDate)
        {
            if (plannedMonth.HasValue)
            {
                var targetPos = registry.GetPoNumbersForPlannedMonth(plannedMonth.Value);
                tracePurge = await _traceability.PurgeTraceabilityForPoNumbersAsync(targetPos, cancellationToken).ConfigureAwait(false);
                csvPurge = await _bundleRepository.PurgeDerivedForPoNumbersAsync(targetPos, request.FromUtc, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                tracePurge = await _traceability.PurgeTraceabilityFromDateAsync(request.FromUtc, cancellationToken).ConfigureAwait(false);
                csvPurge = await _bundleRepository.PurgeDerivedCsvAndBundlesFromDateAsync(request.FromUtc, cancellationToken).ConfigureAwait(false);
            }
        }

        var startingSequences = request.StartingSequenceByMill;
        if (startingSequences is null || startingSequences.Count == 0)
        {
            if (plannedMonth.HasValue)
            {
                var priorPos = registry.GetPoNumbersBeforePlannedMonth(plannedMonth.Value);
                startingSequences = await _bundleRepository
                    .GetMaxSequenceByMillForPoNumbersAsync(priorPos, cancellationToken)
                    .ConfigureAwait(false);
            }
            else
            {
                startingSequences = await _bundleRepository
                    .GetMaxSequenceByMillBeforeUtcAsync(request.FromUtc, cancellationToken)
                    .ConfigureAwait(false);
            }

            if (startingSequences.Count > 0)
            {
                _logger.LogInformation(
                    "Rebuild using StartingSequenceByMill: {Sequences}.",
                    string.Join(", ", startingSequences.Select(kv => $"Mill-{kv.Key}={kv.Value}")));
            }
        }

        _runtimeState.PrepareForRebuild(startingSequences);

        var bundlesClosed = 0;
        var currentPoByMill = new Dictionary<int, string>();
        var outputAccumulators = new Dictionary<string, OutputFileAccumulator>(StringComparer.OrdinalIgnoreCase);

        foreach (var evt in events)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var record = evt.Record;
            if (record.MillNo is < 1 or > 4)
                continue;

            if (!string.IsNullOrWhiteSpace(record.PoNumber)
                && currentPoByMill.TryGetValue(record.MillNo, out var activePo)
                && !InputSlitCsvParsing.PoEquals(activePo, record.PoNumber))
            {
                try
                {
                    await _poEndWorkflow.ExecuteAsync(activePo, record.MillNo, advancePoPlanFile: false, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    warnings.Add($"PO end for Mill-{record.MillNo} PO {activePo} failed: {ex.Message}");
                    _logger.LogWarning(ex, "Rebuild PO end failed for Mill {Mill} PO {Po}.", record.MillNo, activePo);
                }
            }

            if (!string.IsNullOrWhiteSpace(record.PoNumber))
                currentPoByMill[record.MillNo] = InputSlitCsvParsing.NormalizePo(record.PoNumber);

            await ProcessSlitRowAsync(evt, outputAccumulators, () => bundlesClosed++, cancellationToken).ConfigureAwait(false);
        }

        foreach (var (mill, po) in currentPoByMill)
        {
            try
            {
                await _poEndWorkflow.ExecuteAsync(po, mill, advancePoPlanFile: false, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                warnings.Add($"Final PO end for Mill-{mill} PO {po} failed: {ex.Message}");
            }
        }

        var outputFilesWritten = await FlushOutputFilesAsync(outputAccumulators, cancellationToken).ConfigureAwait(false);
        await _runtimeState.SaveAsync(cancellationToken).ConfigureAwait(false);

        var millMax = _runtimeState.GetMillSequenceSnapshot()
            .ToDictionary(kv => kv.Key, kv => Math.Max(kv.Value.BatchOffset, kv.Value.EngineBatchNo));

        return new NdtBundleRebuildResult
        {
            DryRun = false,
            FromUtc = request.FromUtc,
            PlannedMonth = plannedMonth,
            ProductionYear = productionYear,
            TargetPoByMill = targetPoByMill,
            InputFilesConsidered = loadResult.FilesConsidered,
            SlitRowsReplayed = events.Count(e => e.Record.NdtPipes > 0),
            SlitRowsExcluded = loadResult.ExcludedCount,
            ExcludedSamples = excludedSamples,
            BundlesClosed = bundlesClosed,
            OutputSlitFilesWritten = outputFilesWritten,
            TraceabilityPurge = tracePurge,
            CsvPurge = csvPurge,
            MillMaxSequence = millMax,
            Warnings = warnings,
            Message = "Rebuild completed. Verify mill sequences before re-enabling ZPL print."
        };
    }

    private sealed record SlitLoadResult(
        List<RebuildSlitEvent> Events,
        int FilesConsidered,
        int ExcludedCount,
        IReadOnlyList<string> ExcludedSamples);

    private bool TryIncludeSlitRow(
        InputSlitRecord record,
        PoPlanRegistrySnapshot registry,
        int? plannedMonth,
        int? productionYear,
        List<string> excludedSamples,
        ref int excludedCount,
        out string? excludeReason)
    {
        excludeReason = null;
        if (!plannedMonth.HasValue || !productionYear.HasValue)
            return true;

        registry.TryGet(record.MillNo, record.PoNumber, out var poEntry);
        if (!ProductionMonthEligibility.ShouldIncludeSlitRow(
                record,
                poEntry,
                productionYear.Value,
                plannedMonth.Value,
                out excludeReason))
        {
            excludedCount++;
            if (excludedSamples.Count < 25 && !string.IsNullOrWhiteSpace(excludeReason))
                excludedSamples.Add(excludeReason);
            return false;
        }

        return true;
    }

    private async Task ProcessSlitRowAsync(
        RebuildSlitEvent evt,
        Dictionary<string, OutputFileAccumulator> outputAccumulators,
        Action onBundleClosed,
        CancellationToken cancellationToken)
    {
        var record = evt.Record;
        string ndtBatchNoFormatted;
        if (record.NdtPipes <= 0)
        {
            ndtBatchNoFormatted = string.Empty;
        }
        else
        {
            var (batchNo, _, _) = await _batchState
                .GetBatchForRecordAsync(record.PoNumber, record.MillNo, record.NdtPipes, cancellationToken)
                .ConfigureAwait(false);

            await _bundleEngine.ProcessSlitRecordAsync(
                record,
                async (contextRecord, closedBatchNo, totalNdtPcs) =>
                {
                    if (totalNdtPcs <= 0)
                        return;

                    await _outputWriter.WriteBundleAsync(contextRecord, closedBatchNo, totalNdtPcs, cancellationToken).ConfigureAwait(false);
                    onBundleClosed();
                },
                cancellationToken).ConfigureAwait(false);

            ndtBatchNoFormatted = batchNo > 0 ? NdtBundleSequence.Format(batchNo, record.MillNo) : string.Empty;
        }

        StageOutputLine(evt, ndtBatchNoFormatted, outputAccumulators);
    }

    private void StageOutputLine(RebuildSlitEvent evt, string ndtBatchNoFormatted, Dictionary<string, OutputFileAccumulator> outputAccumulators)
    {
        var outputFolder = (Opt.OutputBundleFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(outputFolder))
            return;

        var outputFileName = BuildOutputSlitCsvFileName(evt);
        var outputPath = Path.Combine(outputFolder, outputFileName);
        if (!outputAccumulators.TryGetValue(outputPath, out var acc))
        {
            acc = new OutputFileAccumulator(evt.HeaderLine.TrimEnd() + ",NDT Batch No");
            outputAccumulators[outputPath] = acc;
        }

        acc.SetLine(evt.SourceRowIndex, evt.RawLine.TrimEnd() + "," + ndtBatchNoFormatted);
        acc.TrackTraceability(evt.SourcePath, evt.Record, ndtBatchNoFormatted, evt.SourceRowIndex + 2);
    }

    private async Task<int> FlushOutputFilesAsync(Dictionary<string, OutputFileAccumulator> accumulators, CancellationToken cancellationToken)
    {
        var count = 0;
        foreach (var (outputPath, acc) in accumulators)
        {
            var dir = Path.GetDirectoryName(outputPath);
            if (!string.IsNullOrEmpty(dir))
                Directory.CreateDirectory(dir);

            await File.WriteAllLinesAsync(outputPath, acc.BuildLines(), cancellationToken).ConfigureAwait(false);
            count++;

            foreach (var trace in acc.TraceRows)
            {
                await _traceability.RecordInputSlitRowsAsync(trace.SourcePath, [(trace.Record, trace.SourceRowNumber)], cancellationToken).ConfigureAwait(false);
                await _traceability.RecordOutputSlitRowsAsync(outputPath, [(trace.Record, trace.NdtBatchNo, trace.SourceRowNumber)], cancellationToken).ConfigureAwait(false);
            }
        }

        return count;
    }

    private static string BuildOutputSlitCsvFileName(RebuildSlitEvent evt)
    {
        var record = evt.Record;
        var slit = string.IsNullOrWhiteSpace(record.SlitNo) ? "NoSlit" : record.SlitNo.Trim();
        var date = (record.SlitStartTime ?? record.SlitFinishTime)?.Date ?? evt.SourceLastWriteUtc.Date;
        var safeSlit = CsvOutputFileNaming.SanitizeToken(slit, "NoSlit");
        var safePo = CsvOutputFileNaming.SanitizeToken(record.PoNumber, "NA");
        return $"{safeSlit}_{date:yyyyMMdd}_{safePo}.csv";
    }

    private async Task<SlitLoadResult> LoadEventsFromCsvAsync(
        NdtBundleRebuildRequest request,
        PoPlanRegistrySnapshot registry,
        int? plannedMonth,
        int? productionYear,
        CancellationToken cancellationToken)
    {
        var fromUtc = request.FromUtc;
        var usePlannedMonth = plannedMonth.HasValue && productionYear.HasValue;
        var excludedSamples = new List<string>();
        var excludedCount = 0;
        var filesConsidered = 0;

        var fileMinUtc = ResolveRebuildFileMinUtc(fromUtc, plannedMonth, productionYear);
        var events = new List<RebuildSlitEvent>();
        foreach (var folder in EnumerateInputSlitFolders())
        {
            if (!Directory.Exists(folder))
                continue;

            foreach (var path in InputSlitInboxEnumeration.EnumerateFiles(folder).OrderBy(p => p, StringComparer.OrdinalIgnoreCase))
            {
                if (fileMinUtc.HasValue)
                {
                    try
                    {
                        var lwUtc = File.GetLastWriteTimeUtc(path);
                        if (!SourceFileEligibility.IncludeFileUtc(lwUtc, fileMinUtc))
                            continue;
                    }
                    catch (Exception ex)
                    {
                        _logger.LogDebug(ex, "Rebuild: skipping file with unreadable LastWriteTimeUtc: {Path}", path);
                        continue;
                    }
                }

                var content = await InputSlitFileReader.ReadAsync(path, cancellationToken).ConfigureAwait(false);
                if (content is null)
                    continue;

                filesConsidered++;
                var rowIndex = 0;
                foreach (var (rawLine, record) in content.Rows)
                {
                    if (record is null || string.IsNullOrWhiteSpace(record.PoNumber))
                    {
                        rowIndex++;
                        continue;
                    }

                    if (usePlannedMonth)
                    {
                        if (!TryIncludeSlitRow(record, registry, plannedMonth, productionYear, excludedSamples, ref excludedCount, out _))
                        {
                            rowIndex++;
                            continue;
                        }
                    }
                    else
                    {
                        var slitTime = record.SlitFinishTime ?? record.SlitStartTime;
                        var sortTime = slitTime ?? content.SourceLastWriteUtc;
                        var sortUtc = sortTime.Kind == DateTimeKind.Utc ? sortTime : sortTime.ToUniversalTime();
                        if (sortUtc < fromUtc && content.SourceLastWriteUtc < fromUtc)
                        {
                            rowIndex++;
                            continue;
                        }
                    }

                    var eventSortTime = record.SlitFinishTime ?? record.SlitStartTime ?? content.SourceLastWriteUtc;
                    var eventSortUtc = eventSortTime.Kind == DateTimeKind.Utc ? eventSortTime : eventSortTime.ToUniversalTime();

                    events.Add(new RebuildSlitEvent(
                        content.SourcePath,
                        content.SourceLastWriteUtc,
                        content.HeaderLine,
                        rawLine,
                        record,
                        rowIndex,
                        eventSortUtc));

                    rowIndex++;
                }
            }
        }

        var ordered = events
            .OrderBy(e => e.SortTimeUtc)
            .ThenBy(e => e.Record.MillNo)
            .ThenBy(e => e.SourcePath, StringComparer.OrdinalIgnoreCase)
            .ThenBy(e => e.SourceRowIndex)
            .ToList();

        return new SlitLoadResult(ordered, filesConsidered, excludedCount, excludedSamples);
    }

    private async Task<SlitLoadResult> LoadEventsFromSqlAsync(
        NdtBundleRebuildRequest request,
        PoPlanRegistrySnapshot registry,
        int? plannedMonth,
        int? productionYear,
        CancellationToken cancellationToken)
    {
        if (!SqlTraceabilityConnection.IsSqlEnabled(Opt))
            return new SlitLoadResult([], 0, 0, Array.Empty<string>());

        var fromUtc = request.FromUtc;
        var usePlannedMonth = plannedMonth.HasValue && productionYear.HasValue;
        var excludedSamples = new List<string>();
        var excludedCount = 0;

        var events = new List<RebuildSlitEvent>();
        await using var conn = SqlTraceabilityConnection.Create(Opt);
        await SqlTraceabilityConnection.OpenAsync(conn, _logger, "rebuild load input slits", cancellationToken).ConfigureAwait(false);
        const string sql = @"
SELECT PO_Number, Slit_No, NDT_Pipes, Rejected_P, Slit_Start_Time, Slit_Finish_Time, Mill_No,
       NDT_Short_Length_Pipe, Rejected_Short_Length_Pipe, Source_File, Source_Row_Number, ImportedAtUtc
FROM dbo.Input_Slit_Row
WHERE ImportedAtUtc >= @FromUtc OR Slit_Finish_Time >= @FromUtc
ORDER BY COALESCE(Slit_Finish_Time, Slit_Start_Time, ImportedAtUtc), Mill_No, Input_Slit_Row_ID";

        await using var cmd = new Microsoft.Data.SqlClient.SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@FromUtc", fromUtc);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        var idx = 0;
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var record = new InputSlitRecord
            {
                PoNumber = InputSlitCsvParsing.NormalizePo(reader.GetString(0)),
                SlitNo = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                NdtPipes = reader.GetInt32(2),
                RejectedPipes = reader.GetInt32(3),
                SlitStartTime = reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                SlitFinishTime = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                MillNo = reader.IsDBNull(6) ? 1 : reader.GetInt32(6),
                NdtShortLengthPipe = reader.IsDBNull(7) ? string.Empty : reader.GetString(7),
                RejectedShortLengthPipe = reader.IsDBNull(8) ? string.Empty : reader.GetString(8)
            };

            if (usePlannedMonth)
            {
                if (!TryIncludeSlitRow(record, registry, plannedMonth, productionYear, excludedSamples, ref excludedCount, out _))
                {
                    idx++;
                    continue;
                }
            }

            var sourceFile = reader.IsDBNull(9) ? "sql-rebuild" : reader.GetString(9);
            var sourceRow = reader.IsDBNull(10) ? idx : reader.GetInt32(10) - 2;
            var imported = reader.GetDateTime(11);
            var sortTime = record.SlitFinishTime ?? record.SlitStartTime ?? imported;
            var sortUtc = sortTime.Kind == DateTimeKind.Utc ? sortTime : sortTime.ToUniversalTime();

            events.Add(new RebuildSlitEvent(
                sourceFile,
                imported,
                "PO Number,Slit No,NDT Pipes",
                BuildSyntheticRawLine(record),
                record,
                Math.Max(0, sourceRow),
                sortUtc));
            idx++;
        }

        return new SlitLoadResult(events, 1, excludedCount, excludedSamples);
    }

    private static string BuildSyntheticRawLine(InputSlitRecord record) =>
        string.Join(",",
            record.PoNumber,
            record.SlitNo,
            record.NdtPipes.ToString(CultureInfo.InvariantCulture),
            record.RejectedPipes.ToString(CultureInfo.InvariantCulture),
            record.SlitStartTime?.ToString("O", CultureInfo.InvariantCulture) ?? string.Empty,
            record.SlitFinishTime?.ToString("O", CultureInfo.InvariantCulture) ?? string.Empty,
            record.MillNo.ToString(CultureInfo.InvariantCulture),
            record.NdtShortLengthPipe,
            record.RejectedShortLengthPipe);

    private IEnumerable<string> EnumerateInputSlitFolders()
    {
        var inbox = (Opt.InputSlitFolder ?? string.Empty).Trim();
        if (!string.IsNullOrEmpty(inbox))
            yield return inbox;

        var accepted = (Opt.InputSlitAcceptedFolder ?? string.Empty).Trim();
        if (!string.IsNullOrEmpty(accepted))
            yield return accepted;
    }

    /// <summary>
    /// Skips reading ancient inbox files during rebuild. Uses the later of config min and (production month start - 7 days).
    /// </summary>
    private DateTime? ResolveRebuildFileMinUtc(DateTime fromUtc, int? plannedMonth, int? productionYear)
    {
        var configMin = SourceFileEligibility.ParseMinUtc(Opt);

        DateTime? rebuildMin = null;
        if (plannedMonth is >= 1 and <= 12 && productionYear is > 2000)
        {
            var (monthStart, _) = ProductionMonthEligibility.GetMonthBoundsUtc(productionYear.Value, plannedMonth.Value);
            rebuildMin = monthStart.AddDays(-7);
        }
        else if (fromUtc != default)
        {
            rebuildMin = fromUtc.AddDays(-7);
        }

        if (configMin is null)
            return rebuildMin;
        if (rebuildMin is null)
            return configMin;

        return configMin.Value >= rebuildMin.Value ? configMin : rebuildMin;
    }

    private sealed record RebuildSlitEvent(
        string SourcePath,
        DateTime SourceLastWriteUtc,
        string HeaderLine,
        string RawLine,
        InputSlitRecord Record,
        int SourceRowIndex,
        DateTime SortTimeUtc);

    private sealed class OutputFileAccumulator
    {
        private readonly string _header;
        private readonly SortedDictionary<int, string> _lines = new();
        public List<(string SourcePath, InputSlitRecord Record, string NdtBatchNo, int SourceRowNumber)> TraceRows { get; } = new();

        public OutputFileAccumulator(string header) => _header = header;

        public void SetLine(int sourceRowIndex, string line) => _lines[sourceRowIndex] = line;

        public void TrackTraceability(string sourcePath, InputSlitRecord record, string ndtBatchNo, int sourceRowNumber) =>
            TraceRows.Add((sourcePath, record, ndtBatchNo, sourceRowNumber));

        public IReadOnlyList<string> BuildLines()
        {
            var result = new List<string> { _header };
            result.AddRange(_lines.Values);
            return result;
        }
    }
}
