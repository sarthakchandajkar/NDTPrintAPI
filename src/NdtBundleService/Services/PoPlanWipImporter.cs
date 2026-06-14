using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

public interface IPoPlanWipImporter
{
    Task<PoPlanWipImportResult> ImportEligibleFilesAsync(CancellationToken cancellationToken);
}

public sealed class PoPlanWipImportResult
{
    public static PoPlanWipImportResult Disabled { get; } = new() { SkippedReason = "disabled" };

    public static PoPlanWipImportResult Unavailable { get; } = new() { SkippedReason = "unavailable" };

    public string? SkippedReason { get; init; }
    public int FilesScanned { get; init; }
    public int FilesImported { get; init; }
    public int FilesSkippedUnchanged { get; init; }
    public int RowsInserted { get; init; }
}

public sealed class PoPlanWipImporter : IPoPlanWipImporter
{
    private readonly NdtBundleOptions _options;
    private readonly IPoPlanWipRepository _repository;
    private readonly ILogger<PoPlanWipImporter> _logger;

    public PoPlanWipImporter(
        IOptions<NdtBundleOptions> options,
        IPoPlanWipRepository repository,
        ILogger<PoPlanWipImporter> logger)
    {
        _options = options.Value;
        _repository = repository;
        _logger = logger;
    }

    public async Task<PoPlanWipImportResult> ImportEligibleFilesAsync(CancellationToken cancellationToken)
    {
        if (!PoPlanWipImportSettings.IsEnabled(_options))
        {
            _logger.LogDebug("PO_Plan_WIP folder import is disabled or SQL is not configured.");
            return PoPlanWipImportResult.Disabled;
        }

        if (!await _repository.EnsureImportReadyAsync(cancellationToken).ConfigureAwait(false))
        {
            _logger.LogWarning(
                "PO_Plan_WIP folder import skipped: SQL unavailable or dbo.PO_Plan_WIP table missing.");
            return PoPlanWipImportResult.Unavailable;
        }

        var files = PoPlanWipCsvMerger.ResolveEligiblePoPlanImportFiles(_options);
        var filesImported = 0;
        var filesSkippedUnchanged = 0;
        var rowsInserted = 0;

        foreach (var filePath in files)
        {
            cancellationToken.ThrowIfCancellationRequested();

            long lastWriteTicks;
            try
            {
                lastWriteTicks = File.GetLastWriteTimeUtc(filePath).Ticks;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Skipping PO plan file after stat failure: {File}", filePath);
                continue;
            }

            var sourceKey = PoPlanWipImportKeys.Format(filePath, lastWriteTicks);
            if (await _repository.IsImportSourceFilePresentAsync(sourceKey, cancellationToken).ConfigureAwait(false))
            {
                filesSkippedUnchanged++;
                continue;
            }

            var merge = new PoPlanWipCsvMerger.MergeResult();
            try
            {
                if (!await PoPlanWipCsvMerger.MergeFileAsync(filePath, merge, _logger, cancellationToken)
                        .ConfigureAwait(false))
                {
                    continue;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Skipping PO plan file after read/parse error: {File}", filePath);
                continue;
            }

            if (merge.ByPo.Count == 0)
                continue;

            var inserted = await _repository.InsertImportRowsAsync(
                    sourceKey,
                    merge.ByPo.Values,
                    cancellationToken)
                .ConfigureAwait(false);

            if (inserted <= 0)
                continue;

            filesImported++;
            rowsInserted += inserted;
            _logger.LogInformation(
                "Imported PO plan file into dbo.PO_Plan_WIP: {File} ({RowCount} PO row(s)).",
                filePath,
                inserted);
        }

        if (filesImported > 0 || filesSkippedUnchanged > 0 || files.Count > 0)
        {
            _logger.LogInformation(
                "PO_Plan_WIP folder import finished: scanned {Scanned}, imported {Imported}, skipped unchanged {Skipped}, inserted {Rows} row(s) from {Folder}.",
                files.Count,
                filesImported,
                filesSkippedUnchanged,
                rowsInserted,
                _options.PoPlanFolder);
        }

        return new PoPlanWipImportResult
        {
            FilesScanned = files.Count,
            FilesImported = filesImported,
            FilesSkippedUnchanged = filesSkippedUnchanged,
            RowsInserted = rowsInserted
        };
    }
}
