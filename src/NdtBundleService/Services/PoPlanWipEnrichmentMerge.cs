using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

internal static class PoPlanWipEnrichmentMerge
{
    internal static bool IsPoPlanFolderReachable(NdtBundleOptions options)
    {
        var folder = (options.PoPlanFolder ?? string.Empty).Trim();
        return !string.IsNullOrWhiteSpace(folder) && Directory.Exists(folder);
    }

    internal static bool IsRowMissingPlanDetails(PoPlanWipRow row) =>
        string.IsNullOrWhiteSpace(row.PipeSize)
        && string.IsNullOrWhiteSpace(row.PlannedMonth)
        && string.IsNullOrWhiteSpace(row.PipeLength)
        && string.IsNullOrWhiteSpace(row.PiecesPerBundle)
        && string.IsNullOrWhiteSpace(row.TotalPieces);

    internal static PoPlanWipRow MergeRows(PoPlanWipRow primary, PoPlanWipRow fallback)
    {
        if (IsRowMissingPlanDetails(primary))
            return fallback;

        return new PoPlanWipRow
        {
            MillNo = primary.MillNo > 0 ? primary.MillNo : fallback.MillNo,
            PoNumber = !string.IsNullOrWhiteSpace(primary.PoNumber) ? primary.PoNumber : fallback.PoNumber,
            PlannedMonth = Coalesce(primary.PlannedMonth, fallback.PlannedMonth),
            PipeGrade = Coalesce(primary.PipeGrade, fallback.PipeGrade),
            PipeSize = Coalesce(primary.PipeSize, fallback.PipeSize),
            PipeThickness = Coalesce(primary.PipeThickness, fallback.PipeThickness),
            PipeType = Coalesce(primary.PipeType, fallback.PipeType),
            PipeLength = Coalesce(primary.PipeLength, fallback.PipeLength),
            PipeWeightPerMeter = Coalesce(primary.PipeWeightPerMeter, fallback.PipeWeightPerMeter),
            OutputItemcode = Coalesce(primary.OutputItemcode, fallback.OutputItemcode),
            ItemDescription = Coalesce(primary.ItemDescription, fallback.ItemDescription),
            ProductType = Coalesce(primary.ProductType, fallback.ProductType),
            PoSpecification = Coalesce(primary.PoSpecification, fallback.PoSpecification),
            InputWipItemcode = Coalesce(primary.InputWipItemcode, fallback.InputWipItemcode),
            PiecesPerBundle = Coalesce(primary.PiecesPerBundle, fallback.PiecesPerBundle),
            NdtPcsPerBundle = Coalesce(primary.NdtPcsPerBundle, fallback.NdtPcsPerBundle),
            TotalPieces = Coalesce(primary.TotalPieces, fallback.TotalPieces)
        };
    }

    internal static PoPlanWipEnrichmentSnapshot MergeSnapshots(
        PoPlanWipSqlSnapshot sqlSnapshot,
        PoPlanWipEnrichmentSnapshot csvSnapshot,
        string sourceDescription)
    {
        var byPo = new Dictionary<string, PoPlanWipRow>(StringComparer.OrdinalIgnoreCase);
        foreach (var (po, row) in sqlSnapshot.ByPo)
            byPo[po] = row;

        foreach (var (po, csvRow) in csvSnapshot.ByPo)
        {
            if (byPo.TryGetValue(po, out var existing))
                byPo[po] = MergeRows(existing, csvRow);
            else
                byPo[po] = csvRow;
        }

        var byMill = new Dictionary<int, PoPlanWipRow>();
        foreach (var (mill, row) in sqlSnapshot.ByMill)
            byMill[mill] = row;

        foreach (var (mill, csvRow) in csvSnapshot.ByMill)
        {
            if (byMill.TryGetValue(mill, out var existing))
                byMill[mill] = MergeRows(existing, csvRow);
            else
                byMill[mill] = csvRow;
        }

        return new PoPlanWipEnrichmentSnapshot(byMill, byPo, sourceDescription);
    }

    private static string Coalesce(string primary, string fallback) =>
        !string.IsNullOrWhiteSpace(primary) ? primary.Trim() : fallback.Trim();
}
