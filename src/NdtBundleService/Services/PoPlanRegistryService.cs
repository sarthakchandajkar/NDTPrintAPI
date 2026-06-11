using System.Globalization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

public interface IPoPlanRegistry
{
    Task<PoPlanRegistrySnapshot> GetSnapshotAsync(CancellationToken cancellationToken);
}

public sealed class PoPlanRegistrySnapshot
{
    public IReadOnlyDictionary<string, PoPlanEntry> ByPo { get; init; } =
        new Dictionary<string, PoPlanEntry>(StringComparer.OrdinalIgnoreCase);

    public IReadOnlyDictionary<(int MillNo, string PoNumber), PoPlanEntry> ByMillPo { get; init; } =
        new Dictionary<(int MillNo, string PoNumber), PoPlanEntry>();

    public bool TryGet(int millNo, string poNumber, out PoPlanEntry? entry)
    {
        entry = null;
        if (millNo is < 1 or > 4 || string.IsNullOrWhiteSpace(poNumber))
            return false;

        var key = (millNo, InputSlitCsvParsing.NormalizePo(poNumber));
        if (!ByMillPo.TryGetValue(key, out var found))
            return false;

        entry = found;
        return true;
    }

    public IReadOnlyDictionary<int, PoPlanEntry> GetPrimaryPoByPlannedMonth(int plannedMonth)
    {
        var result = new Dictionary<int, PoPlanEntry>();
        foreach (var entry in ByMillPo.Values)
        {
            if (!ProductionMonthEligibility.TryParsePlannedMonth(entry.PlannedMonth, out var pm) || pm != plannedMonth)
                continue;
            result[entry.MillNo] = entry;
        }

        return result;
    }

    public HashSet<string> GetPoNumbersForPlannedMonth(int plannedMonth)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var entry in ByPo.Values)
        {
            if (ProductionMonthEligibility.TryParsePlannedMonth(entry.PlannedMonth, out var pm) && pm == plannedMonth)
                set.Add(InputSlitCsvParsing.NormalizePo(entry.PoNumber));
        }

        return set;
    }

    public HashSet<string> GetPoNumbersBeforePlannedMonth(int plannedMonth)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var entry in ByPo.Values)
        {
            if (ProductionMonthEligibility.TryParsePlannedMonth(entry.PlannedMonth, out var pm) && pm < plannedMonth)
                set.Add(InputSlitCsvParsing.NormalizePo(entry.PoNumber));
        }

        return set;
    }
}

/// <summary>
/// Merges all WIP / PO Accepted CSV rows from <see cref="NdtBundleOptions.PoPlanFolder"/> keyed by (mill, PO).
/// Uses <c>Planned Month</c> from SAP — not file arrival date — to identify production month.
/// </summary>
public sealed class PoPlanRegistryService : IPoPlanRegistry
{
    private readonly IOptionsMonitor<NdtBundleOptions> _optionsMonitor;
    private readonly ILogger<PoPlanRegistryService> _logger;

    public PoPlanRegistryService(IOptionsMonitor<NdtBundleOptions> optionsMonitor, ILogger<PoPlanRegistryService> logger)
    {
        _optionsMonitor = optionsMonitor;
        _logger = logger;
    }

    public async Task<PoPlanRegistrySnapshot> GetSnapshotAsync(CancellationToken cancellationToken)
    {
        var options = _optionsMonitor.CurrentValue;
        var folder = (options.PoPlanFolder ?? string.Empty).Trim();
        if (string.IsNullOrEmpty(folder) || !Directory.Exists(folder))
        {
            _logger.LogWarning("PoPlanFolder not configured or missing; PO registry is empty.");
            return new PoPlanRegistrySnapshot();
        }

        var byMillPo = new Dictionary<(int MillNo, string PoNumber), PoPlanEntry>();
        var byPo = new Dictionary<string, PoPlanEntry>(StringComparer.OrdinalIgnoreCase);

        var files = Directory.EnumerateFiles(folder, "*.csv")
            .Select(f => new FileInfo(f))
            .Where(fi => SourceFileEligibility.IncludePoPlanFolderFileUtc(fi.LastWriteTimeUtc, options))
            .OrderBy(fi => fi.LastWriteTimeUtc)
            .ThenBy(fi => fi.FullName, StringComparer.OrdinalIgnoreCase);

        foreach (var fi in files)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await MergeFileAsync(fi.FullName, byMillPo, byPo, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to read PO plan file {File} for registry.", fi.FullName);
            }
        }

        _logger.LogInformation(
            "PO registry loaded {Count} (mill, PO) row(s) from {Folder}.",
            byMillPo.Count,
            folder);

        return new PoPlanRegistrySnapshot
        {
            ByMillPo = byMillPo,
            ByPo = byPo
        };
    }

    private static async Task MergeFileAsync(
        string filePath,
        Dictionary<(int MillNo, string PoNumber), PoPlanEntry> byMillPo,
        Dictionary<string, PoPlanEntry> byPo,
        CancellationToken cancellationToken)
    {
        await using var stream = File.OpenRead(filePath);
        using var reader = new StreamReader(stream);
        var headerLine = await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false);
        if (headerLine is null)
            return;

        var headers = InputSlitCsvParsing.SplitCsvFields(InputSlitCsvParsing.StripBom(headerLine));
        var poIdx = InputSlitCsvParsing.HeaderIndex(headers, "PO_No", "PO Number", "PO No");
        var millIdx = InputSlitCsvParsing.HeaderIndex(headers, "Mill Number", "Mill No");
        var plannedIdx = InputSlitCsvParsing.HeaderIndex(headers, "Planned Month");
        var pcsIdx = InputSlitCsvParsing.HeaderIndex(headers, "NDTPcsPerBundle", "Pieces Per Bundle");

        if (poIdx < 0 || millIdx < 0)
            return;

        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            if (string.IsNullOrWhiteSpace(line))
                continue;

            var cols = InputSlitCsvParsing.SplitCsvFields(line);
            if (cols.Length <= Math.Max(poIdx, millIdx))
                continue;

            var poRaw = cols[poIdx].Trim();
            if (string.IsNullOrWhiteSpace(poRaw))
                continue;

            if (!InputSlitCsvParsing.TryParseMillNo(cols[millIdx].Trim(), out var millNo))
                continue;

            var po = InputSlitCsvParsing.NormalizePo(poRaw);
            var planned = plannedIdx >= 0 && plannedIdx < cols.Length ? cols[plannedIdx].Trim() : string.Empty;
            var ndtPerBundle = 0;
            if (pcsIdx >= 0 && pcsIdx < cols.Length)
                int.TryParse(cols[pcsIdx].Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out ndtPerBundle);

            var entry = new PoPlanEntry
            {
                PoNumber = po,
                MillNo = millNo,
                PlannedMonth = planned,
                NdtPcsPerBundle = ndtPerBundle,
                SourceFile = Path.GetFileName(filePath)
            };

            byMillPo[(millNo, po)] = entry;
            byPo[po] = entry;
        }
    }
}
