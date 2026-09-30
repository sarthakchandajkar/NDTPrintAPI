using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

public interface IUploadNdtBundleFileService
{
    /// <summary>
    /// Writes one <c>UploadNdtBundle__PO__…</c> CSV for a single NDT batch after Visual, Hydrotesting,
    /// and Revisual have produced an <c>NDT_process_</c> file (OK pcs = Revisual OK).
    /// </summary>
    Task<UploadNdtBundleGenerationResult> GenerateForBatchAsync(string ndtBatchNo, CancellationToken cancellationToken);

    /// <summary>
    /// Lists NDT batches that already have an upload CSV (SQL <c>Upload_Bundle_Row</c> and/or files on disk
    /// under <c>UploadNdtBundleFilesFolder</c>). One row per batch (latest generation).
    /// </summary>
    Task<IReadOnlyList<UploadNdtBundleGeneratedItem>> ListGeneratedAsync(
        int take,
        CancellationToken cancellationToken);
}

public sealed class UploadNdtBundleGenerationResult
{
    public string FilePath { get; init; } = string.Empty;
    public int RowCount { get; init; }
    public string NdtBatchNo { get; init; } = string.Empty;
}

public sealed class UploadNdtBundleGeneratedItem
{
    public string NdtBatchNo { get; init; } = string.Empty;
    public string PoNo { get; init; } = string.Empty;
    public int? MillNo { get; init; }
    public string SlitNo { get; init; } = string.Empty;
    public int NumOfPipes { get; init; }
    public string FileName { get; init; } = string.Empty;
    public string FilePath { get; init; } = string.Empty;
    public DateTime? GeneratedAtUtc { get; init; }
    public bool FileExistsOnDisk { get; init; }
}

public sealed class UploadNdtBundleFileService : IUploadNdtBundleFileService
{
    internal const string RevisualRequiredMessage =
        "Upload CSV is written only after Visual, Hydrotesting, and Revisual are complete for this NDT batch.";

    /// <summary>
    /// UTF-8 with BOM so Windows associates the file as a Microsoft Excel Comma Separated Values File
    /// (.csv), not a binary Excel Worksheet (.xlsx).
    /// </summary>
    private static readonly Encoding ExcelCsvUtf8 = new UTF8Encoding(encoderShouldEmitUTF8Identifier: true);

    private static readonly Regex UploadFileName = new(
        @"^UploadNdtBundle__PO__(?<po>.+?)__(?<batch>.+?)-(?<mill>\d)__TS-(?<ts>.+)\.csv$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private readonly NdtBundleOptions _options;
    private readonly INdtBundleRepository _bundleRepository;
    private readonly ITraceabilityRepository _traceability;
    private readonly IWipLabelProvider _wipLabelProvider;
    private readonly ISlitAcceptedRepository _slitAcceptedRepository;
    private readonly IFgBundleRepository _fgBundleRepository;
    private readonly ILogger<UploadNdtBundleFileService> _logger;

    public UploadNdtBundleFileService(
        IOptions<NdtBundleOptions> options,
        INdtBundleRepository bundleRepository,
        ITraceabilityRepository traceability,
        IWipLabelProvider wipLabelProvider,
        ISlitAcceptedRepository slitAcceptedRepository,
        IFgBundleRepository fgBundleRepository,
        ILogger<UploadNdtBundleFileService> logger)
    {
        _options = options.Value;
        _bundleRepository = bundleRepository;
        _traceability = traceability;
        _wipLabelProvider = wipLabelProvider;
        _slitAcceptedRepository = slitAcceptedRepository;
        _fgBundleRepository = fgBundleRepository;
        _logger = logger;
    }

    public async Task<IReadOnlyList<UploadNdtBundleGeneratedItem>> ListGeneratedAsync(
        int take,
        CancellationToken cancellationToken)
    {
        var limit = take <= 0 ? 200 : Math.Min(take, 2000);
        var byBatch = new Dictionary<string, UploadNdtBundleGeneratedItem>(StringComparer.OrdinalIgnoreCase);

        try
        {
            var fromSql = await _traceability
                .GetLatestUploadBundleRowsAsync(limit, cancellationToken)
                .ConfigureAwait(false);
            foreach (var row in fromSql)
            {
                var batch = (row.BundleNumber ?? string.Empty).Trim();
                if (string.IsNullOrWhiteSpace(batch))
                    continue;

                var path = (row.SourceFile ?? string.Empty).Trim();
                var exists = !string.IsNullOrWhiteSpace(path) && File.Exists(path);
                byBatch[batch] = new UploadNdtBundleGeneratedItem
                {
                    NdtBatchNo = batch,
                    PoNo = row.PoNo ?? string.Empty,
                    SlitNo = row.SlitNo ?? string.Empty,
                    NumOfPipes = row.NumOfPipes,
                    FilePath = path,
                    FileName = string.IsNullOrWhiteSpace(path) ? string.Empty : Path.GetFileName(path),
                    GeneratedAtUtc = row.GeneratedAtUtc,
                    FileExistsOnDisk = exists,
                    MillNo = TryParseMillFromFileName(Path.GetFileName(path))
                };
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "SQL list of generated upload bundles failed; folder scan may still apply.");
        }

        var folder = (_options.UploadNdtBundleFilesFolder ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(folder) && Directory.Exists(folder))
        {
            try
            {
                foreach (var path in Directory.EnumerateFiles(folder, "UploadNdtBundle__*.csv"))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var name = Path.GetFileName(path);
                    var parsed = TryParseUploadFileName(name);
                    if (parsed is null)
                        continue;

                    DateTime? writeUtc = null;
                    try
                    {
                        writeUtc = File.GetLastWriteTimeUtc(path);
                    }
                    catch
                    {
                        // ignore
                    }

                    if (byBatch.TryGetValue(parsed.Batch, out var existing))
                    {
                        var existingTime = existing.GeneratedAtUtc ?? DateTime.MinValue;
                        var fileTime = writeUtc ?? DateTime.MinValue;
                        if (fileTime > existingTime || string.IsNullOrWhiteSpace(existing.FilePath))
                        {
                            byBatch[parsed.Batch] = new UploadNdtBundleGeneratedItem
                            {
                                NdtBatchNo = parsed.Batch,
                                PoNo = string.IsNullOrWhiteSpace(existing.PoNo) ? parsed.Po : existing.PoNo,
                                MillNo = parsed.Mill ?? existing.MillNo,
                                SlitNo = existing.SlitNo,
                                NumOfPipes = existing.NumOfPipes,
                                FileName = name,
                                FilePath = path,
                                GeneratedAtUtc = writeUtc ?? existing.GeneratedAtUtc,
                                FileExistsOnDisk = true
                            };
                        }
                        else
                        {
                            byBatch[parsed.Batch] = new UploadNdtBundleGeneratedItem
                            {
                                NdtBatchNo = existing.NdtBatchNo,
                                PoNo = existing.PoNo,
                                MillNo = existing.MillNo ?? parsed.Mill,
                                SlitNo = existing.SlitNo,
                                NumOfPipes = existing.NumOfPipes,
                                FileName = existing.FileName,
                                FilePath = existing.FilePath,
                                GeneratedAtUtc = existing.GeneratedAtUtc,
                                FileExistsOnDisk = existing.FileExistsOnDisk || File.Exists(existing.FilePath)
                            };
                        }
                    }
                    else
                    {
                        byBatch[parsed.Batch] = new UploadNdtBundleGeneratedItem
                        {
                            NdtBatchNo = parsed.Batch,
                            PoNo = parsed.Po,
                            MillNo = parsed.Mill,
                            FileName = name,
                            FilePath = path,
                            GeneratedAtUtc = writeUtc,
                            FileExistsOnDisk = true
                        };
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Folder scan of upload NDT bundle CSVs failed under {Folder}.", folder);
            }
        }

        return byBatch.Values
            .OrderByDescending(x => x.GeneratedAtUtc ?? DateTime.MinValue)
            .ThenByDescending(x => x.NdtBatchNo, StringComparer.OrdinalIgnoreCase)
            .Take(limit)
            .ToList();
    }

    public async Task<UploadNdtBundleGenerationResult> GenerateForBatchAsync(
        string ndtBatchNo,
        CancellationToken cancellationToken)
    {
        var batch = (ndtBatchNo ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(batch))
            throw new InvalidOperationException("NdtBatchNo is required.");

        var ndtProcessFolder = (_options.NdtProcessOutputFolder ?? string.Empty).Trim();
        var uploadFolder = (_options.UploadNdtBundleFilesFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(ndtProcessFolder) || !Directory.Exists(ndtProcessFolder))
            throw new InvalidOperationException("NdtProcessOutputFolder is not configured or does not exist.");
        if (string.IsNullOrWhiteSpace(uploadFolder))
            throw new InvalidOperationException("UploadNdtBundleFilesFolder is not configured.");

        var processPath = NdtProcessCsvReconcileHelper.FindLatestNdtProcessFileForBatch(ndtProcessFolder, batch);
        if (processPath is null)
            throw new InvalidOperationException(RevisualRequiredMessage);

        var metrics = NdtProcessCsvReconcileHelper.TryReadMetricsForBatch(_options, batch);
        if (metrics is null)
            throw new InvalidOperationException(RevisualRequiredMessage);

        var poNo = (metrics.Value.Po ?? string.Empty).Trim();
        var okPcs = metrics.Value.Ok;
        var bundle = await _bundleRepository.GetByBatchNoAsync(batch, cancellationToken).ConfigureAwait(false);
        var millNo = bundle?.MillNo ?? 0;
        if (string.IsNullOrWhiteSpace(poNo))
            poNo = bundle?.PoNumber?.Trim() ?? string.Empty;

        var sourceSlitNo = await ResolveUploadSlitNoAsync(batch, bundle?.SlitNo, cancellationToken)
            .ConfigureAwait(false);

        var hrcNumber = ExtractHrcNumber(sourceSlitNo);
        var wip = await ReadWipByPoAndMillAsync(poNo, millNo is >= 1 and <= 4 ? millNo : null, cancellationToken)
            .ConfigureAwait(false);
        var slitWidth = await TryResolveSlitWidthAsync(sourceSlitNo, cancellationToken).ConfigureAwait(false);
        var slitGrade = wip.PipeGrade;
        if (string.IsNullOrWhiteSpace(slitGrade))
        {
            slitGrade = await TryResolveFgGradeAsync(
                    poNo,
                    millNo is >= 1 and <= 4 ? millNo : null,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        var slitThick = wip.PipeThickness;
        var lenPerPipe = wip.PipeLength;
        var totalBundleWt = NdtBundleWeightCalculator.FormatBundleWeight(
            wip.PipeWeightPerMeter,
            lenPerPipe,
            okPcs);

        var header = "PO_NO,Slit_No,HRC Number,Slit Width,Slit Thick,NSS,Slit Grade,Bundle Number,NumOfPipes,TotalBundleWt,LenPerPipe,IsFullBundle";
        var outputLine = string.Join(",",
            Escape(poNo),
            Escape(sourceSlitNo),
            Escape(hrcNumber),
            Escape(slitWidth),
            Escape(slitThick),
            "",
            Escape(slitGrade),
            Escape(batch),
            okPcs.ToString(CultureInfo.InvariantCulture),
            Escape(totalBundleWt),
            Escape(lenPerPipe),
            "");

        Directory.CreateDirectory(uploadFolder);
        var ts = DateTime.Now.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
        var safePo = CsvOutputFileNaming.SanitizeToken(string.IsNullOrWhiteSpace(poNo) ? "NA" : poNo);
        var safeBatch = CsvOutputFileNaming.SanitizeToken(batch);
        // Explicit .csv extension — SAP pickup rejects Excel Worksheet (.xlsx) files.
        var fileName = $"UploadNdtBundle__PO__{safePo}__{safeBatch}-{millNo}__TS-{ts}.csv";
        var fullPath = Path.Combine(uploadFolder, fileName);
        await File.WriteAllLinesAsync(
                fullPath,
                new[] { header, outputLine },
                ExcelCsvUtf8,
                cancellationToken)
            .ConfigureAwait(false);

        var uploadRow = new UploadBundleRow
        {
            PoNo = poNo,
            SlitNo = sourceSlitNo,
            HrcNumber = hrcNumber,
            SlitWidth = slitWidth,
            SlitThick = slitThick,
            Nss = string.Empty,
            SlitGrade = slitGrade,
            BundleNumber = batch,
            NumOfPipes = okPcs,
            TotalBundleWt = totalBundleWt,
            LenPerPipe = lenPerPipe,
            IsFullBundle = null,
            SourceFile = fullPath
        };
        try
        {
            await _traceability.RecordUploadBundleRowsAsync(fullPath, [uploadRow], cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Upload CSV {Path} was written but SQL traceability failed for batch {BatchNo}.", fullPath, batch);
        }

        _logger.LogInformation(
            "Generated upload NDT bundle CSV for batch {BatchNo} slit {SlitNo} ({OkPcs} OK pcs): {Path}",
            batch,
            sourceSlitNo,
            okPcs,
            fullPath);
        return new UploadNdtBundleGenerationResult { FilePath = fullPath, RowCount = 1, NdtBatchNo = batch };
    }

    private async Task<string> ResolveUploadSlitNoAsync(
        string batch,
        string? contextSlitNo,
        CancellationToken cancellationToken)
    {
        IReadOnlyList<(string SlitNo, DateTime? SlitFinishTime)> outputSlits =
            Array.Empty<(string, DateTime?)>();
        IReadOnlyCollection<string> claimed = Array.Empty<string>();
        try
        {
            outputSlits = await _traceability
                .GetOutputSlitsByFinishTimeForBatchAsync(batch, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Output_Slit_Row finish-time lookup failed for batch {BatchNo}.", batch);
        }

        try
        {
            claimed = await _traceability
                .GetSlitNosClaimedByOtherBundlesAsync(batch, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Claimed slit lookup failed for batch {BatchNo}.", batch);
        }

        var picked = UploadBundleSlitNoSelection.Pick(contextSlitNo, outputSlits, claimed);
        var context = (contextSlitNo ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(picked))
        {
            if (!string.IsNullOrWhiteSpace(context)
                && claimed.Contains(context, StringComparer.OrdinalIgnoreCase))
            {
                _logger.LogWarning(
                    "Upload slit for batch {BatchNo}: Context_Slit_No {SlitNo} is already claimed by another bundle and no unclaimed Output_Slit_Row fallback remains.",
                    batch,
                    context);
            }
            else
            {
                _logger.LogWarning(
                    "Upload slit for batch {BatchNo}: Context_Slit_No blank and no Output_Slit_Row fallback available.",
                    batch);
            }
        }

        return picked;
    }

    private async Task<(string PipeGrade, string PipeThickness, string PipeLength, string PipeWeightPerMeter)> ReadWipByPoAndMillAsync(
        string poNo,
        int? millNo,
        CancellationToken cancellationToken)
    {
        if (!millNo.HasValue || millNo.Value is < 1 or > 4 || string.IsNullOrWhiteSpace(poNo))
            return ("", "", "", "");

        try
        {
            // Prefers dbo.PO_Plan_WIP when PreferSqlForPoPlanWip is on, then CSV with shared read.
            var wip = await _wipLabelProvider
                .GetWipLabelAsync(poNo, millNo.Value, cancellationToken)
                .ConfigureAwait(false);
            if (wip is null)
                return ("", "", "", "");

            return (wip.PipeGrade, wip.PipeThickness, wip.PipeLength, wip.PipeWeightPerMeter);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "WIP / PO_Plan_WIP lookup failed for PO {PoNumber} mill {MillNo}; upload columns may be empty.",
                poNo,
                millNo);
            return ("", "", "", "");
        }
    }

    private async Task<string> TryResolveSlitWidthAsync(string sourceSlitNo, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(sourceSlitNo))
            return string.Empty;

        if (_options.PreferSqlForUploadLookups)
        {
            try
            {
                var fromSql = await _slitAcceptedRepository
                    .TryGetLatestSlitWidthAsync(sourceSlitNo, cancellationToken)
                    .ConfigureAwait(false);
                if (!string.IsNullOrWhiteSpace(fromSql))
                    return fromSql.Trim();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Slit_Accepted_Row width lookup failed for slit {SlitNo}.", sourceSlitNo);
            }
        }

        try
        {
            return await SlitAcceptedCsvLookup
                .ResolveSlitWidthAsync(_options, sourceSlitNo, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Slit Accepted width lookup failed for slit {SlitNo}.", sourceSlitNo);
            return string.Empty;
        }
    }

    private async Task<string> TryResolveFgGradeAsync(
        string poNo,
        int? millNo,
        CancellationToken cancellationToken)
    {
        if (_options.PreferSqlForUploadLookups)
        {
            try
            {
                var fromSql = await _fgBundleRepository
                    .TryGetLatestPipeGradeAsync(poNo, millNo, cancellationToken)
                    .ConfigureAwait(false);
                if (!string.IsNullOrWhiteSpace(fromSql))
                    return fromSql.Trim();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Fg_Bundle_Row grade lookup failed for PO {PoNumber} mill {MillNo}.", poNo, millNo);
            }
        }

        try
        {
            return await FgBundleCsvLookup
                .ResolvePipeGradeAsync(_options, poNo, millNo, _logger, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "FG bundle grade lookup failed for PO {PoNumber} mill {MillNo}.", poNo, millNo);
            return string.Empty;
        }
    }

    private static string ExtractHrcNumber(string slitNo)
    {
        if (string.IsNullOrWhiteSpace(slitNo))
            return string.Empty;
        var idx = slitNo.IndexOf('_');
        return idx > 0 ? slitNo[..idx].Trim() : slitNo.Trim();
    }

    private static string Escape(string value)
    {
        if (value.IndexOfAny(new[] { ',', '"', '\r', '\n' }) >= 0)
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        return value;
    }

    private static int? TryParseMillFromFileName(string? fileName)
    {
        var parsed = TryParseUploadFileName(fileName);
        return parsed?.Mill;
    }

    private static ParsedUploadFileName? TryParseUploadFileName(string? fileName)
    {
        if (string.IsNullOrWhiteSpace(fileName))
            return null;
        var m = UploadFileName.Match(fileName.Trim());
        if (!m.Success)
            return null;
        var po = m.Groups["po"].Value.Trim();
        var batch = m.Groups["batch"].Value.Trim();
        if (string.IsNullOrWhiteSpace(batch))
            return null;
        int? mill = null;
        if (int.TryParse(m.Groups["mill"].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var millNo)
            && millNo is >= 1 and <= 4)
        {
            mill = millNo;
        }

        return new ParsedUploadFileName(po, batch, mill);
    }

    private sealed record ParsedUploadFileName(string Po, string Batch, int? Mill);
}
