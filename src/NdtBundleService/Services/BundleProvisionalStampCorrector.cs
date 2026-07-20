using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// When close allocates a mill-wide number that differs from the open provisional stamp,
/// rewrites <c>Output_Slit_Row</c> and per-slit output CSVs so traceability matches the printed tag.
/// </summary>
public interface IBundleProvisionalStampCorrector
{
    Task CorrectAsync(
        string poNumber,
        int millNo,
        int provisionalSequence,
        int finalSequence,
        CancellationToken cancellationToken);
}

public sealed class BundleProvisionalStampCorrector : IBundleProvisionalStampCorrector
{
    private readonly ITraceabilityRepository _traceability;
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private readonly ILogger<BundleProvisionalStampCorrector> _logger;

    public BundleProvisionalStampCorrector(
        ITraceabilityRepository traceability,
        IOptionsMonitor<NdtBundleOptions> options,
        ILogger<BundleProvisionalStampCorrector> logger)
    {
        _traceability = traceability;
        _options = options;
        _logger = logger;
    }

    public async Task CorrectAsync(
        string poNumber,
        int millNo,
        int provisionalSequence,
        int finalSequence,
        CancellationToken cancellationToken)
    {
        if (provisionalSequence <= 0 || provisionalSequence == finalSequence)
            return;

        var oldBatch = NdtBundleSequence.Format(provisionalSequence, millNo);
        var newBatch = NdtBundleSequence.Format(finalSequence, millNo);
        var po = InputSlitCsvParsing.NormalizePo(poNumber);

        var files = await _traceability
            .UpdateOutputSlitBatchNoAsync(po, millNo, oldBatch, newBatch, cancellationToken)
            .ConfigureAwait(false);

        _logger.LogInformation(
            "Corrected Output_Slit_Row batch {OldBatch} → {NewBatch} for PO {PO} Mill {Mill} ({FileCount} file ref(s)).",
            oldBatch,
            newBatch,
            po,
            millNo,
            files.Count);

        var outputFolder = (_options.CurrentValue.OutputBundleFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(outputFolder))
        {
            _logger.LogWarning(
                "Per-slit CSV rewrite skipped for {OldBatch} → {NewBatch}: OutputBundleFolder is empty (Output_Slit_Row already corrected).",
                oldBatch,
                newBatch);
            return;
        }

        // Mandatory CSV rewrite: prefer SQL Source_File refs; if none, scan folder for the provisional batch.
        var targets = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var sourceFile in files)
        {
            var baseName = Path.GetFileName(sourceFile);
            if (!string.IsNullOrEmpty(baseName))
                targets.Add(baseName);
        }

        if (targets.Count == 0 && Directory.Exists(outputFolder))
        {
            foreach (var path in Directory.EnumerateFiles(outputFolder, "*.csv", SearchOption.TopDirectoryOnly))
            {
                try
                {
                    var text = await File.ReadAllTextAsync(path, Encoding.UTF8, cancellationToken).ConfigureAwait(false);
                    if (text.Contains(oldBatch, StringComparison.OrdinalIgnoreCase))
                        targets.Add(Path.GetFileName(path));
                }
                catch
                {
                    // best-effort scan
                }
            }
        }

        if (targets.Count == 0)
        {
            _logger.LogWarning(
                "No per-slit output CSV found to rewrite for {OldBatch} → {NewBatch} under {Folder} (Output_Slit_Row already corrected).",
                oldBatch,
                newBatch,
                outputFolder);
            return;
        }

        foreach (var baseName in targets)
        {
            var csvPath = Path.Combine(outputFolder, baseName);
            if (!File.Exists(csvPath))
            {
                _logger.LogWarning(
                    "Provisional stamp correct: output CSV not found for rewrite ({Path}); Output_Slit_Row already {NewBatch}.",
                    csvPath,
                    newBatch);
                continue;
            }

            try
            {
                // Downstream (SAP) may already have consumed this file — always warn when rewriting.
                _logger.LogWarning(
                    "Rewriting per-slit output CSV {File} batch column {OldBatch} → {NewBatch} (may already have been consumed downstream).",
                    baseName,
                    oldBatch,
                    newBatch);

                var text = await File.ReadAllTextAsync(csvPath, Encoding.UTF8, cancellationToken).ConfigureAwait(false);
                var rewritten = RewriteBatchColumn(text, oldBatch, newBatch);
                if (!string.Equals(text, rewritten, StringComparison.Ordinal))
                {
                    await File.WriteAllTextAsync(csvPath, rewritten, Encoding.UTF8, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Failed to rewrite per-slit output CSV {File} for batch correction {OldBatch} → {NewBatch}.",
                    baseName,
                    oldBatch,
                    newBatch);
            }
        }
    }

    /// <summary>Replaces trailing batch field when it equals <paramref name="oldBatch"/>.</summary>
    internal static string RewriteBatchColumn(string csvText, string oldBatch, string newBatch)
    {
        if (string.IsNullOrEmpty(csvText) || string.IsNullOrEmpty(oldBatch))
            return csvText;

        var lines = csvText.Replace("\r\n", "\n", StringComparison.Ordinal).Replace('\r', '\n').Split('\n');
        var sb = new StringBuilder(csvText.Length + 16);
        for (var i = 0; i < lines.Length; i++)
        {
            var line = lines[i];
            if (i > 0)
                sb.Append('\n');

            if (string.IsNullOrEmpty(line))
                continue;

            var lastComma = line.LastIndexOf(',');
            if (lastComma >= 0)
            {
                var lastField = line[(lastComma + 1)..].Trim();
                if (string.Equals(lastField, oldBatch, StringComparison.OrdinalIgnoreCase))
                {
                    sb.Append(line.AsSpan(0, lastComma + 1));
                    sb.Append(newBatch);
                    continue;
                }
            }

            sb.Append(line);
        }

        return sb.ToString();
    }
}
