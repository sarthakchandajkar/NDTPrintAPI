using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

public interface IUploadNdtBundleFileService
{
    /// <summary>
    /// Writes/updates one <c>ndt-{ddMMyy}1607000.csv</c> for a batch after Revisual.
    /// When <paramref name="okPcsOverride"/> is set (e.g. process CSV already in Bundle Accepted),
    /// uses that OK count instead of reading the on-disk process file.
    /// </summary>
    Task<UploadNdtBundleGenerationResult> GenerateForBatchAsync(
        string ndtBatchNo,
        CancellationToken cancellationToken,
        int? okPcsOverride = null,
        string? poNumberOverride = null);

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

    /// <summary>Legacy MES PAS upload name (still scanned for dashboard history).</summary>
    private static readonly Regex LegacyUploadFileName = new(
        @"^UploadNdtBundle__PO__(?<po>.+?)__(?<batch>.+?)-(?<mill>\d)__TS-(?<ts>.+)\.csv$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>SAP pickup stem prefix; trailing digits start at <see cref="UploadFileSequenceStart"/>.</summary>
    internal const string UploadFileNamePrefix = "ndt-";

    /// <summary>Fixed SAP sequence seed in <c>ndt-ddmmyy1607000</c>; increments when that name already exists.</summary>
    internal const int UploadFileSequenceStart = 1_607_000;

    private readonly NdtBundleOptions _options;
    private readonly INdtBundleRepository _bundleRepository;
    private readonly ITraceabilityRepository _traceability;
    private readonly IWipLabelProvider _wipLabelProvider;
    private readonly IPoPlanWipRepository _poPlanWipRepository;
    private readonly ISlitAcceptedRepository _slitAcceptedRepository;
    private readonly IFgBundleRepository _fgBundleRepository;
    private readonly ILogger<UploadNdtBundleFileService> _logger;

    public UploadNdtBundleFileService(
        IOptions<NdtBundleOptions> options,
        INdtBundleRepository bundleRepository,
        ITraceabilityRepository traceability,
        IWipLabelProvider wipLabelProvider,
        IPoPlanWipRepository poPlanWipRepository,
        ISlitAcceptedRepository slitAcceptedRepository,
        IFgBundleRepository fgBundleRepository,
        ILogger<UploadNdtBundleFileService> logger)
    {
        _options = options.Value;
        _bundleRepository = bundleRepository;
        _traceability = traceability;
        _wipLabelProvider = wipLabelProvider;
        _poPlanWipRepository = poPlanWipRepository;
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
                // Legacy names only — current ndt-ddmmyy#######.csv files have no batch in the name;
                // SQL Upload_Bundle_Row (SourceFile) is the source of truth for those.
                foreach (var path in Directory.EnumerateFiles(folder, "UploadNdtBundle__*.csv"))
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var name = Path.GetFileName(path);
                    var parsed = TryParseLegacyUploadFileName(name);
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
        CancellationToken cancellationToken,
        int? okPcsOverride = null,
        string? poNumberOverride = null)
    {
        var batch = (ndtBatchNo ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(batch))
            throw new InvalidOperationException("NdtBatchNo is required.");

        var ndtProcessFolder = (_options.NdtProcessOutputFolder ?? string.Empty).Trim();
        var uploadFolder = (_options.UploadNdtBundleFilesFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(uploadFolder))
            throw new InvalidOperationException("UploadNdtBundleFilesFolder is not configured.");

        string poNo;
        int okPcs;
        if (okPcsOverride.HasValue)
        {
            // Corrected Revisual OK when process CSV is already under Bundle Accepted (disk values are stale).
            okPcs = okPcsOverride.Value;
            poNo = (poNumberOverride ?? string.Empty).Trim();
        }
        else
        {
            if (string.IsNullOrWhiteSpace(ndtProcessFolder) || !Directory.Exists(ndtProcessFolder))
                throw new InvalidOperationException("NdtProcessOutputFolder is not configured or does not exist.");

            var processPath = NdtProcessCsvReconcileHelper.FindLatestNdtProcessFileForBatch(ndtProcessFolder, batch);
            if (processPath is null)
            {
                var acceptedFolder = (_options.NdtProcessAcceptedFolder ?? string.Empty).Trim();
                if (!string.IsNullOrWhiteSpace(acceptedFolder) && Directory.Exists(acceptedFolder))
                    processPath = NdtProcessCsvReconcileHelper.FindLatestNdtProcessFileForBatch(acceptedFolder, batch);
            }

            if (processPath is null)
                throw new InvalidOperationException(RevisualRequiredMessage);

            var metrics = NdtProcessCsvReconcileHelper.TryReadMetricsFromPath(processPath, batch)
                ?? NdtProcessCsvReconcileHelper.TryReadMetricsForBatch(_options, batch);
            if (metrics is null)
                throw new InvalidOperationException(RevisualRequiredMessage);

            poNo = (metrics.Value.Po ?? string.Empty).Trim();
            okPcs = metrics.Value.Ok;
        }

        var bundle = await _bundleRepository.GetByBatchNoAsync(batch, cancellationToken).ConfigureAwait(false);
        var millNo = bundle?.MillNo ?? 0;
        if (string.IsNullOrWhiteSpace(poNo))
            poNo = bundle?.PoNumber?.Trim() ?? string.Empty;

        var sourceSlitNo = await ResolveUploadSlitNoAsync(batch, bundle?.SlitNo, cancellationToken)
            .ConfigureAwait(false);

        var priorUpload = await _traceability
            .TryGetLatestUploadBundleRowForBatchAsync(batch, cancellationToken)
            .ConfigureAwait(false);
        var priorPath = (priorUpload?.SourceFile ?? string.Empty).Trim();
        var acceptedUploadFolder = (_options.UploadNdtBundleAcceptedFolder ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(priorPath) && File.Exists(priorPath))
        {
            if (IsPathUnderFolder(priorPath, acceptedUploadFolder))
            {
                throw new InvalidOperationException(
                    $"Upload CSV for batch {batch} is already under Bundle Accepted ({priorPath}). " +
                    $"Mail the receiving team with NumOfPipes={okPcs} instead of regenerating a duplicate file.");
            }
        }

        var hrcNumber = ExtractHrcNumber(sourceSlitNo);
        var wip = await ReadWipByPoAndMillAsync(poNo, millNo is >= 1 and <= 4 ? millNo : null, cancellationToken)
            .ConfigureAwait(false);
        var slitWidth = await TryResolveSlitWidthAsync(sourceSlitNo, cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(slitWidth) && !string.IsNullOrWhiteSpace(priorUpload?.SlitWidth))
            slitWidth = priorUpload!.SlitWidth.Trim();

        var slitGrade = wip.PipeGrade;
        if (string.IsNullOrWhiteSpace(slitGrade) && !string.IsNullOrWhiteSpace(priorUpload?.SlitGrade))
            slitGrade = priorUpload!.SlitGrade.Trim();
        if (string.IsNullOrWhiteSpace(slitGrade))
        {
            slitGrade = await TryResolveFgGradeAsync(
                    poNo,
                    millNo is >= 1 and <= 4 ? millNo : null,
                    cancellationToken)
                .ConfigureAwait(false);
        }

        var slitThick = string.IsNullOrWhiteSpace(wip.PipeThickness)
            ? (priorUpload?.SlitThick ?? string.Empty).Trim()
            : wip.PipeThickness;
        var lenPerPipe = string.IsNullOrWhiteSpace(wip.PipeLength)
            ? (priorUpload?.LenPerPipe ?? string.Empty).Trim()
            : wip.PipeLength;
        var weightPerMeter = wip.PipeWeightPerMeter;
        var totalBundleWt = NdtBundleWeightCalculator.FormatBundleWeight(
            weightPerMeter,
            lenPerPipe,
            okPcs);
        if (string.IsNullOrWhiteSpace(totalBundleWt) &&
            !string.IsNullOrWhiteSpace(priorUpload?.TotalBundleWt) &&
            priorUpload!.NumOfPipes > 0 &&
            priorUpload.NumOfPipes == okPcs)
        {
            totalBundleWt = priorUpload.TotalBundleWt.Trim();
        }

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
        // Prefer rewriting the prior MES PAS file for this batch while it is still in Bundle
        // (avoids a second ndt-* file for the same bundle). Allocate a new name only on first write.
        string fullPath;
        if (!string.IsNullOrWhiteSpace(priorPath) &&
            File.Exists(priorPath) &&
            IsPathUnderFolder(priorPath, uploadFolder))
        {
            fullPath = priorPath;
            _logger.LogInformation(
                "Updating existing upload NDT bundle CSV in place for batch {BatchNo}: {Path}",
                batch,
                fullPath);
        }
        else
        {
            // SAP expects ndt-ddmmyy1607000.csv (date + fixed sequence seed). Explicit .csv — not .xlsx.
            var fileName = AllocateUploadFileName(uploadFolder, DateTime.Now);
            fullPath = Path.Combine(uploadFolder, fileName);
        }

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
        if (string.IsNullOrWhiteSpace(poNo))
            return ("", "", "", "");

        // Upload generation must not scan WIP/PO Accepted CSV folders on the request path.
        if (_options.PreferSqlForUploadLookups || PoPlanWipSql.IsEnabled(_options))
        {
            try
            {
                var row = await _poPlanWipRepository
                    .TryGetLatestByPoAsync(poNo, cancellationToken)
                    .ConfigureAwait(false);
                if (row is null)
                    return ("", "", "", "");

                var wip = PoPlanWipRowMapper.ToWipLabel(row);
                return (wip.PipeGrade, wip.PipeThickness, wip.PipeLength, wip.PipeWeightPerMeter);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "PO_Plan_WIP lookup failed for PO {PoNumber}; upload columns may be empty.",
                    poNo);
                return ("", "", "", "");
            }
        }

        if (!millNo.HasValue || millNo.Value is < 1 or > 4)
            return ("", "", "", "");

        try
        {
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
                return string.IsNullOrWhiteSpace(fromSql) ? string.Empty : fromSql.Trim();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Slit_Accepted_Row width lookup failed for slit {SlitNo}.", sourceSlitNo);
                return string.Empty;
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
                return string.IsNullOrWhiteSpace(fromSql) ? string.Empty : fromSql.Trim();
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Fg_Bundle_Row grade lookup failed for PO {PoNumber} mill {MillNo}.", poNo, millNo);
                return string.Empty;
            }
        }

        try
        {
            return await FgBundleCsvLookup
                .ResolvePipeGradeAsync(
                    _options,
                    poNo,
                    millNo,
                    _logger,
                    cancellationToken,
                    includeAcceptedFolders: false)
                .ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "FG bundle grade lookup failed for PO {PoNumber} mill {MillNo}.", poNo, millNo);
            return string.Empty;
        }
    }

    internal static bool IsPathUnderFolder(string filePath, string? folder)
    {
        if (string.IsNullOrWhiteSpace(filePath) || string.IsNullOrWhiteSpace(folder))
            return false;
        try
        {
            var fullFile = Path.GetFullPath(filePath);
            var fullFolder = Path.GetFullPath(folder.TrimEnd(
                Path.DirectorySeparatorChar,
                Path.AltDirectorySeparatorChar));
            if (!fullFolder.EndsWith(Path.DirectorySeparatorChar))
                fullFolder += Path.DirectorySeparatorChar;
            return fullFile.StartsWith(fullFolder, StringComparison.OrdinalIgnoreCase);
        }
        catch
        {
            return false;
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

    /// <summary>
    /// Builds <c>ndt-{ddMMyy}{seq}.csv</c> starting at <see cref="UploadFileSequenceStart"/>.
    /// If that file already exists in <paramref name="folder"/>, increments seq until free.
    /// </summary>
    internal static string AllocateUploadFileName(string folder, DateTime localNow)
    {
        var datePart = localNow.ToString("ddMMyy", CultureInfo.InvariantCulture);
        for (var seq = UploadFileSequenceStart; seq <= UploadFileSequenceStart + 999_999; seq++)
        {
            var fileName = $"{UploadFileNamePrefix}{datePart}{seq.ToString(CultureInfo.InvariantCulture)}.csv";
            var fullPath = Path.Combine(folder, fileName);
            if (!File.Exists(fullPath))
                return fileName;
        }

        throw new InvalidOperationException(
            $"Could not allocate an unused upload file name under {folder} for {datePart}.");
    }

    private static int? TryParseMillFromFileName(string? fileName)
    {
        var parsed = TryParseLegacyUploadFileName(fileName);
        return parsed?.Mill;
    }

    private static ParsedUploadFileName? TryParseLegacyUploadFileName(string? fileName)
    {
        if (string.IsNullOrWhiteSpace(fileName))
            return null;
        var m = LegacyUploadFileName.Match(fileName.Trim());
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
