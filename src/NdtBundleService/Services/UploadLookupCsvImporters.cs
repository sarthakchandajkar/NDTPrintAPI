using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

public interface ISlitAcceptedImporter
{
    Task<CsvFolderImportResult> ImportEligibleFilesAsync(CancellationToken cancellationToken);
}

public interface IFgBundleImporter
{
    Task<CsvFolderImportResult> ImportEligibleFilesAsync(CancellationToken cancellationToken);
}

public sealed class CsvFolderImportResult
{
    public static CsvFolderImportResult Disabled { get; } = new() { SkippedReason = "disabled" };
    public static CsvFolderImportResult Unavailable { get; } = new() { SkippedReason = "unavailable" };

    public string? SkippedReason { get; init; }
    public int FilesScanned { get; init; }
    public int FilesImported { get; init; }
    public int FilesSkippedUnchanged { get; init; }
    public int RowsInserted { get; init; }
    public int AuditRowsInserted { get; init; }
}

/// <summary>
/// Imports Slit Accepted CSVs into lookup + audit SQL tables.
/// Source folder files are read-only inputs — never modified, moved, transformed on disk, or deleted.
/// </summary>
public sealed class SlitAcceptedImporter : ISlitAcceptedImporter
{
    private readonly NdtBundleOptions _options;
    private readonly ISlitAcceptedRepository _repository;
    private readonly ILogger<SlitAcceptedImporter> _logger;

    public SlitAcceptedImporter(
        IOptions<NdtBundleOptions> options,
        ISlitAcceptedRepository repository,
        ILogger<SlitAcceptedImporter> logger)
    {
        _options = options.Value;
        _repository = repository;
        _logger = logger;
    }

    public async Task<CsvFolderImportResult> ImportEligibleFilesAsync(CancellationToken cancellationToken)
    {
        if (!_options.ImportSlitAcceptedFromFolder || !SqlTraceabilityConnection.IsSqlEnabled(_options))
            return CsvFolderImportResult.Disabled;

        if (!await _repository.EnsureImportReadyAsync(cancellationToken).ConfigureAwait(false))
            return CsvFolderImportResult.Unavailable;

        var files = SlitAcceptedCsvParser.ResolveEligibleFiles(_options);
        var imported = 0;
        var skipped = 0;
        var lookupRows = 0;
        var auditRows = 0;

        foreach (var filePath in files)
        {
            cancellationToken.ThrowIfCancellationRequested();
            long ticks;
            try
            {
                ticks = File.GetLastWriteTimeUtc(filePath).Ticks;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Skipping Slit Accepted file after stat failure: {File}", filePath);
                continue;
            }

            var key = PoPlanWipImportKeys.Format(filePath, ticks);
            if (await _repository.IsImportSourceFilePresentAsync(key, cancellationToken).ConfigureAwait(false))
            {
                skipped++;
                continue;
            }

            CsvFolderFileParseResult<SlitAcceptedRow> parsed;
            try
            {
                parsed = await SlitAcceptedCsvParser.ParseFileAsync(filePath, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Skipping Slit Accepted file after parse error: {File}", filePath);
                continue;
            }

            if (parsed.AuditRows.Count == 0 && parsed.LookupRows.Count == 0)
                continue;

            var auditInserted = await _repository
                .InsertAuditRowsAsync(key, parsed.AuditRows, cancellationToken)
                .ConfigureAwait(false);
            var lookupInserted = await _repository
                .InsertImportRowsAsync(key, parsed.LookupRows, cancellationToken)
                .ConfigureAwait(false);

            if (auditInserted <= 0 && lookupInserted <= 0)
                continue;

            imported++;
            auditRows += auditInserted;
            lookupRows += lookupInserted;
            _logger.LogInformation(
                "Imported Slit Accepted file: {File} (audit {AuditRows} row(s), lookup {LookupRows} slit(s)).",
                filePath,
                auditInserted,
                lookupInserted);
        }

        if (files.Count > 0)
        {
            _logger.LogInformation(
                "Slit Accepted import finished: scanned {Scanned}, imported {Imported}, skipped unchanged {Skipped}, audit {Audit}, lookup {Lookup}.",
                files.Count,
                imported,
                skipped,
                auditRows,
                lookupRows);
        }

        return new CsvFolderImportResult
        {
            FilesScanned = files.Count,
            FilesImported = imported,
            FilesSkippedUnchanged = skipped,
            RowsInserted = lookupRows,
            AuditRowsInserted = auditRows
        };
    }
}

/// <summary>
/// Imports FG bundle CSVs into lookup + audit SQL tables.
/// Source folder files are read-only inputs — never modified, moved, transformed on disk, or deleted.
/// </summary>
public sealed class FgBundleImporter : IFgBundleImporter
{
    private readonly NdtBundleOptions _options;
    private readonly IFgBundleRepository _repository;
    private readonly ILogger<FgBundleImporter> _logger;

    public FgBundleImporter(
        IOptions<NdtBundleOptions> options,
        IFgBundleRepository repository,
        ILogger<FgBundleImporter> logger)
    {
        _options = options.Value;
        _repository = repository;
        _logger = logger;
    }

    public async Task<CsvFolderImportResult> ImportEligibleFilesAsync(CancellationToken cancellationToken)
    {
        if (!_options.ImportFgBundleFromFolder || !SqlTraceabilityConnection.IsSqlEnabled(_options))
            return CsvFolderImportResult.Disabled;

        if (!await _repository.EnsureImportReadyAsync(cancellationToken).ConfigureAwait(false))
            return CsvFolderImportResult.Unavailable;

        var files = FgBundleCsvParser.ResolveEligibleFiles(_options);
        var imported = 0;
        var skipped = 0;
        var lookupRows = 0;
        var auditRows = 0;

        foreach (var filePath in files)
        {
            cancellationToken.ThrowIfCancellationRequested();
            long ticks;
            try
            {
                ticks = File.GetLastWriteTimeUtc(filePath).Ticks;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Skipping FG bundle file after stat failure: {File}", filePath);
                continue;
            }

            var key = PoPlanWipImportKeys.Format(filePath, ticks);
            if (await _repository.IsImportSourceFilePresentAsync(key, cancellationToken).ConfigureAwait(false))
            {
                skipped++;
                continue;
            }

            CsvFolderFileParseResult<FgBundleRow> parsed;
            try
            {
                parsed = await FgBundleCsvParser.ParseFileAsync(filePath, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogWarning(ex, "Skipping FG bundle file after parse error: {File}", filePath);
                continue;
            }

            if (parsed.AuditRows.Count == 0 && parsed.LookupRows.Count == 0)
                continue;

            var auditInserted = await _repository
                .InsertAuditRowsAsync(key, parsed.AuditRows, cancellationToken)
                .ConfigureAwait(false);
            var lookupInserted = await _repository
                .InsertImportRowsAsync(key, parsed.LookupRows, cancellationToken)
                .ConfigureAwait(false);

            if (auditInserted <= 0 && lookupInserted <= 0)
                continue;

            imported++;
            auditRows += auditInserted;
            lookupRows += lookupInserted;
            _logger.LogInformation(
                "Imported FG bundle file: {File} (audit {AuditRows} row(s), lookup {LookupRows} row(s)).",
                filePath,
                auditInserted,
                lookupInserted);
        }

        if (files.Count > 0)
        {
            _logger.LogInformation(
                "FG bundle import finished: scanned {Scanned}, imported {Imported}, skipped unchanged {Skipped}, audit {Audit}, lookup {Lookup}.",
                files.Count,
                imported,
                skipped,
                auditRows,
                lookupRows);
        }

        return new CsvFolderImportResult
        {
            FilesScanned = files.Count,
            FilesImported = imported,
            FilesSkippedUnchanged = skipped,
            RowsInserted = lookupRows,
            AuditRowsInserted = auditRows
        };
    }
}
