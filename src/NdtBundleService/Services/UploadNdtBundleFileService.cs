using System.Globalization;
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
}

public sealed class UploadNdtBundleGenerationResult
{
    public string FilePath { get; init; } = string.Empty;
    public int RowCount { get; init; }
    public string NdtBatchNo { get; init; } = string.Empty;
}

public sealed class UploadNdtBundleFileService : IUploadNdtBundleFileService
{
    internal const string RevisualRequiredMessage =
        "Upload CSV is written only after Visual, Hydrotesting, and Revisual are complete for this NDT batch.";

    private readonly NdtBundleOptions _options;
    private readonly INdtBundleRepository _bundleRepository;
    private readonly ITraceabilityRepository _traceability;
    private readonly ILogger<UploadNdtBundleFileService> _logger;

    public UploadNdtBundleFileService(
        IOptions<NdtBundleOptions> options,
        INdtBundleRepository bundleRepository,
        ITraceabilityRepository traceability,
        ILogger<UploadNdtBundleFileService> logger)
    {
        _options = options.Value;
        _bundleRepository = bundleRepository;
        _traceability = traceability;
        _logger = logger;
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
        var sourceSlitNo = bundle?.SlitNo?.Trim() ?? string.Empty;
        var millNo = bundle?.MillNo ?? 0;
        if (string.IsNullOrWhiteSpace(poNo))
            poNo = bundle?.PoNumber?.Trim() ?? string.Empty;

        var hrcNumber = ExtractHrcNumber(sourceSlitNo);
        var wip = await ReadWipByPoAndMillAsync(poNo, millNo is >= 1 and <= 4 ? millNo : null, cancellationToken)
            .ConfigureAwait(false);
        var slitWidth = await SlitAcceptedCsvLookup.ResolveSlitWidthAsync(_options, sourceSlitNo, cancellationToken)
            .ConfigureAwait(false);
        var slitGrade = await FgBundleCsvLookup.ResolvePipeGradeAsync(
                _options,
                poNo,
                millNo is >= 1 and <= 4 ? millNo : null,
                _logger,
                cancellationToken)
            .ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(slitGrade))
            slitGrade = wip.PipeGrade;
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
        var fileName = $"UploadNdtBundle__PO__{safePo}__{safeBatch}-{millNo}__TS-{ts}.csv";
        var fullPath = Path.Combine(uploadFolder, fileName);
        await File.WriteAllLinesAsync(fullPath, new[] { header, outputLine }, cancellationToken).ConfigureAwait(false);

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
            IsFullBundle = null
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
            "Generated upload NDT bundle CSV for batch {BatchNo} ({OkPcs} OK pcs): {Path}",
            batch,
            okPcs,
            fullPath);
        return new UploadNdtBundleGenerationResult { FilePath = fullPath, RowCount = 1, NdtBatchNo = batch };
    }

    private async Task<(string PipeGrade, string PipeThickness, string PipeLength, string PipeWeightPerMeter)> ReadWipByPoAndMillAsync(
        string poNo,
        int? millNo,
        CancellationToken cancellationToken)
    {
        if (!millNo.HasValue || millNo.Value is < 1 or > 4)
            return ("", "", "", "");

        var wip = await WipCsvLabelLookup.ResolveAsync(
            _options,
            ResolvePoPlanPath(),
            poNo,
            millNo.Value,
            _logger,
            cancellationToken).ConfigureAwait(false);
        if (wip is null)
            return ("", "", "", "");

        return (wip.PipeGrade, wip.PipeThickness, wip.PipeLength, wip.PipeWeightPerMeter);
    }

    private string ResolvePoPlanPath()
    {
        if (!string.IsNullOrWhiteSpace(_options.PoPlanCsvPath))
            return _options.PoPlanCsvPath.Trim();

        var folder = (_options.PoPlanFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return string.Empty;
        return Directory.EnumerateFiles(folder, "*.csv")
            .Where(f => SourceFileEligibility.IncludePoPlanFolderFileUtc(File.GetLastWriteTimeUtc(f), _options))
            .OrderBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault() ?? string.Empty;
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
}
