using System.Globalization;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

public interface IUploadNdtBundleFileService
{
    Task<UploadNdtBundleGenerationResult> GenerateAsync(CancellationToken cancellationToken);
}

public sealed class UploadNdtBundleGenerationResult
{
    public string FilePath { get; init; } = string.Empty;
    public int RowCount { get; init; }
}

public sealed class UploadNdtBundleFileService : IUploadNdtBundleFileService
{
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

    public async Task<UploadNdtBundleGenerationResult> GenerateAsync(CancellationToken cancellationToken)
    {
        var ndtProcessFolder = (_options.NdtProcessOutputFolder ?? string.Empty).Trim();
        var uploadFolder = (_options.UploadNdtBundleFilesFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(ndtProcessFolder) || !Directory.Exists(ndtProcessFolder))
            throw new InvalidOperationException("NdtProcessOutputFolder is not configured or does not exist.");
        if (string.IsNullOrWhiteSpace(uploadFolder))
            throw new InvalidOperationException("UploadNdtBundleFilesFolder is not configured.");

        Directory.CreateDirectory(uploadFolder);

        var rows = new List<string>
        {
            "PO_NO,Slit_No,HRC Number,Slit Width,Slit Thick,NSS,Slit Grade,Bundle Number,NumOfPipes,TotalBundleWt,LenPerPipe,IsFullBundle"
        };
        var uploadRowsForSql = new List<UploadBundleRow>();
        string? firstPo = null;
        string? firstBatch = null;
        int? firstMill = null;

        var revisualFiles = Directory.EnumerateFiles(ndtProcessFolder, "*.csv")
            .Where(p => Path.GetFileName(p).StartsWith("NDT process_", StringComparison.OrdinalIgnoreCase))
            .OrderBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        foreach (var file in revisualFiles)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                var lines = await ReadAllLinesSharedAsync(file, cancellationToken).ConfigureAwait(false);
                if (lines.Length < 2)
                    continue;

                // Consolidated NDT process CSV: header + one data row (final OK count at column index 3).
                // Be defensive and use the last non-empty line as the data row.
                var dataLine = lines
                    .LastOrDefault(static line => !string.IsNullOrWhiteSpace(line));
                if (string.IsNullOrWhiteSpace(dataLine))
                    continue;

                var values = SplitCsvLine(dataLine);
                if (values.Count < 4)
                    continue;

                var poNo = values[0].Trim();
                var batchNo = values[1].Trim();
                var okPcs = ParseInt(values[3]);

                if (string.IsNullOrWhiteSpace(batchNo))
                    continue;

                var bundle = await _bundleRepository.GetByBatchNoAsync(batchNo, cancellationToken).ConfigureAwait(false);
                var sourceSlitNo = bundle?.SlitNo?.Trim() ?? string.Empty;
                var convertedSlitNo = ConvertSlitNoPrefix(sourceSlitNo);
                var hrcNumber = ExtractHrcNumber(sourceSlitNo);

                var wip = await ReadWipByPoAndMillAsync(poNo, bundle?.MillNo, cancellationToken).ConfigureAwait(false);
                var slitWidth = await FindSlitWidthFromAcceptedFilesAsync(sourceSlitNo, cancellationToken).ConfigureAwait(false);
                var slitThick = wip.PipeThickness;
                var slitGrade = wip.PipeGrade;
                var lenPerPipe = wip.PipeLength;
                var totalBundleWt = FormatWeight(wip.PipeWeightPerMeter, okPcs);

                var outputLine = string.Join(",",
                    Escape(poNo),
                    Escape(convertedSlitNo),
                    Escape(hrcNumber),
                    Escape(slitWidth),
                    Escape(slitThick),
                    "", // NSS
                    Escape(slitGrade),
                    Escape(batchNo),
                    okPcs.ToString(CultureInfo.InvariantCulture),
                    Escape(totalBundleWt),
                    Escape(lenPerPipe),
                    ""); // IsFullBundle

                rows.Add(outputLine);

                uploadRowsForSql.Add(new UploadBundleRow
                {
                    PoNo = poNo,
                    SlitNo = convertedSlitNo,
                    HrcNumber = hrcNumber,
                    SlitWidth = slitWidth,
                    SlitThick = slitThick,
                    Nss = string.Empty,
                    SlitGrade = slitGrade,
                    BundleNumber = batchNo,
                    NumOfPipes = okPcs,
                    TotalBundleWt = totalBundleWt,
                    LenPerPipe = lenPerPipe,
                    IsFullBundle = null
                });

                if (firstPo is null)
                {
                    firstPo = poNo;
                    firstBatch = batchNo;
                    firstMill = bundle?.MillNo ?? 0;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Skipping NDT process CSV {File} during upload bundle generation.", file);
            }
        }

        var ts = DateTime.Now.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
        var safePo = CsvOutputFileNaming.SanitizeToken(firstPo ?? "NA");
        var safeBatch = CsvOutputFileNaming.SanitizeToken(firstBatch ?? "NA");
        var mill = firstMill ?? 0;
        // UploadNdtBundle__PO__<PO>__<NDT Bundle Number>-<MillNo>__TS-<yyyyMMdd_HHmmss>.csv
        var fileName = $"UploadNdtBundle__PO__{safePo}__{safeBatch}-{mill}__TS-{ts}.csv";
        var fullPath = Path.Combine(uploadFolder, fileName);
        await File.WriteAllLinesAsync(fullPath, rows, cancellationToken).ConfigureAwait(false);

        // Best-effort SQL traceability; do not fail generation if SQL is down.
        await _traceability.RecordUploadBundleRowsAsync(fullPath, uploadRowsForSql, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("Generated upload NDT bundle CSV: {Path} with {Rows} row(s).", fullPath, rows.Count - 1);
        return new UploadNdtBundleGenerationResult { FilePath = fullPath, RowCount = Math.Max(0, rows.Count - 1) };
    }

    private async Task<(string PipeGrade, string PipeThickness, string PipeLength, string PipeWeightPerMeter)> ReadWipByPoAndMillAsync(
        string poNo,
        int? millNo,
        CancellationToken cancellationToken)
    {
        var path = ResolvePoPlanPath();
        if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
            return ("", "", "", "");

        var lines = await ReadAllLinesSharedAsync(path, cancellationToken).ConfigureAwait(false);
        if (lines.Length < 2)
            return ("", "", "", "");

        var headers = SplitCsvLine(lines[0]);
        int Idx(string name) => headers.FindIndex(h => h.Equals(name, StringComparison.OrdinalIgnoreCase));
        var poIdx = Idx("PO_No");
        var millIdx = Idx("Mill Number");
        var gradeIdx = Idx("Pipe Grade");
        var thickIdx = Idx("Pipe Thickness");
        var lengthIdx = Idx("Pipe Length");
        var weightIdx = Idx("Pipe Weight Per Meter");
        if (poIdx < 0)
            return ("", "", "", "");

        foreach (var line in lines.Skip(1))
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;
            var cols = SplitCsvLine(line);
            if (poIdx >= cols.Count)
                continue;
            if (!cols[poIdx].Trim().Equals(poNo.Trim(), StringComparison.OrdinalIgnoreCase))
                continue;
            if (millNo.HasValue && millIdx >= 0 && millIdx < cols.Count)
            {
                var m = cols[millIdx].Trim();
                if (int.TryParse(m, out var parsedMill) && parsedMill != millNo.Value)
                    continue;
            }

            string Get(int idx) => idx >= 0 && idx < cols.Count ? cols[idx].Trim() : "";
            return (Get(gradeIdx), Get(thickIdx), Get(lengthIdx), Get(weightIdx));
        }

        return ("", "", "", "");
    }

    private string ResolvePoPlanPath()
    {
        if (!string.IsNullOrWhiteSpace(_options.PoPlanCsvPath))
            return _options.PoPlanCsvPath.Trim();

        var folder = (_options.PoPlanFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return string.Empty;
        return Directory.EnumerateFiles(folder, "*.csv")
            .OrderBy(Path.GetFileName, StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault() ?? string.Empty;
    }

    private async Task<string> FindSlitWidthFromAcceptedFilesAsync(string slitNo, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(slitNo))
            return string.Empty;

        var folder = (_options.SlitAcceptedFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return string.Empty;

        foreach (var path in Directory.EnumerateFiles(folder, "*.csv"))
        {
            try
            {
                var lines = await ReadAllLinesSharedAsync(path, cancellationToken).ConfigureAwait(false);
                if (lines.Length < 2)
                    continue;
                var headers = SplitCsvLine(lines[0]);
                var values = SplitCsvLine(lines[1]);
                for (var i = 0; i < headers.Count; i++)
                {
                    if (!headers[i].EndsWith("Batch_No", StringComparison.OrdinalIgnoreCase))
                        continue;
                    if (i >= values.Count || !values[i].Trim().Equals(slitNo.Trim(), StringComparison.OrdinalIgnoreCase))
                        continue;
                    var widthHeader = headers[i].Replace("Batch_No", "Width", StringComparison.OrdinalIgnoreCase);
                    var widthIndex = headers.FindIndex(h => h.Equals(widthHeader, StringComparison.OrdinalIgnoreCase));
                    if (widthIndex >= 0 && widthIndex < values.Count)
                        return values[widthIndex].Trim();
                }
            }
            catch
            {
                // Ignore malformed slit accepted files.
            }
        }

        return string.Empty;
    }

    private static string ConvertSlitNoPrefix(string slitNo)
    {
        if (string.IsNullOrWhiteSpace(slitNo))
            return string.Empty;
        var value = slitNo.Trim();
        var first = value[0];
        if (!char.IsDigit(first))
            return value;
        var digit = first - '0';
        if (digit <= 0)
            return value;
        var letter = (char)('A' + digit - 1);
        return letter + value[1..];
    }

    private static string ExtractHrcNumber(string slitNo)
    {
        if (string.IsNullOrWhiteSpace(slitNo))
            return string.Empty;
        var idx = slitNo.IndexOf('_');
        return idx > 0 ? slitNo[..idx].Trim() : slitNo.Trim();
    }

    private static int ParseInt(string value)
    {
        return int.TryParse(value.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) ? parsed : 0;
    }

    private static string FormatWeight(string pipeWeightPerMeter, int numOfPipes)
    {
        if (!decimal.TryParse(pipeWeightPerMeter.Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out var perMeter))
            return string.Empty;
        var total = perMeter * numOfPipes;
        return total.ToString("0.###", CultureInfo.InvariantCulture);
    }

    private static string Escape(string value)
    {
        if (value.IndexOfAny(new[] { ',', '"', '\r', '\n' }) >= 0)
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        return value;
    }

    private static List<string> SplitCsvLine(string line)
    {
        return line.Split(',').Select(c => c.Trim()).ToList();
    }

    private static async Task<string[]> ReadAllLinesSharedAsync(string path, CancellationToken cancellationToken)
    {
        await using var stream = new FileStream(
            path,
            FileMode.Open,
            FileAccess.Read,
            FileShare.ReadWrite | FileShare.Delete);
        using var reader = new StreamReader(stream);
        var list = new List<string>();
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var line = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
            if (line is null)
                break;
            list.Add(line);
        }

        return list.ToArray();
    }
}

