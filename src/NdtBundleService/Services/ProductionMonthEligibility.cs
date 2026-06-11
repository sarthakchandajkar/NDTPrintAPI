using System.Globalization;
using NdtBundleService.Configuration;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>Decides whether an input slit row belongs to a target SAP planned production month.</summary>
public static class ProductionMonthEligibility
{
    public static (DateTime MonthStartUtc, DateTime MonthEndUtc) GetMonthBoundsUtc(int productionYear, int plannedMonth)
    {
        if (plannedMonth is < 1 or > 12)
            throw new ArgumentOutOfRangeException(nameof(plannedMonth));

        var start = new DateTime(productionYear, plannedMonth, 1, 0, 0, 0, DateTimeKind.Utc);
        var end = plannedMonth == 12
            ? new DateTime(productionYear + 1, 1, 1, 0, 0, 0, DateTimeKind.Utc)
            : new DateTime(productionYear, plannedMonth + 1, 1, 0, 0, 0, DateTimeKind.Utc);
        return (start, end);
    }

    public static bool TryParsePlannedMonth(string? raw, out int month)
    {
        month = 0;
        if (string.IsNullOrWhiteSpace(raw))
            return false;

        var s = raw.Trim();
        if (int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out month))
            return month is >= 1 and <= 12;

        return false;
    }

    public static bool MatchesPlannedMonth(PoPlanEntry? entry, int targetPlannedMonth) =>
        entry is not null
        && TryParsePlannedMonth(entry.PlannedMonth, out var pm)
        && pm == targetPlannedMonth;

    /// <summary>
    /// Returns true when the slit row should be included for the target planned production month.
    /// File last-write time is never used to include rows when slit timestamps are present.
    /// </summary>
    public static bool ShouldIncludeSlitRow(
        InputSlitRecord record,
        PoPlanEntry? poEntry,
        int productionYear,
        int targetPlannedMonth,
        out string? excludeReason)
    {
        excludeReason = null;

        if (string.IsNullOrWhiteSpace(record.PoNumber))
        {
            excludeReason = "missing PO number";
            return false;
        }

        if (record.MillNo is < 1 or > 4)
        {
            excludeReason = "invalid mill number";
            return false;
        }

        if (poEntry is null)
        {
            excludeReason = $"PO {record.PoNumber} not found in PO Accepted registry for Mill-{record.MillNo}";
            return false;
        }

        if (!TryParsePlannedMonth(poEntry.PlannedMonth, out var plannedMonth))
        {
            excludeReason = $"PO {record.PoNumber} has invalid Planned Month '{poEntry.PlannedMonth}'";
            return false;
        }

        if (plannedMonth != targetPlannedMonth)
        {
            excludeReason = $"PO {record.PoNumber} Planned Month={plannedMonth} (target={targetPlannedMonth})";
            return false;
        }

        var slitTime = record.SlitFinishTime ?? record.SlitStartTime;
        if (slitTime.HasValue)
        {
            var utc = slitTime.Value.Kind == DateTimeKind.Utc
                ? slitTime.Value
                : slitTime.Value.ToUniversalTime();
            var (start, end) = GetMonthBoundsUtc(productionYear, targetPlannedMonth);
            if (utc < start || utc >= end)
            {
                excludeReason =
                    $"slit time {utc:o} outside {productionYear}-{targetPlannedMonth:D2} (PO {record.PoNumber})";
                return false;
            }
        }

        return true;
    }

    public static int ResolveActivePlannedMonth(NdtBundleOptions options)
    {
        if (options.ActiveProductionPlannedMonth is >= 1 and <= 12)
            return options.ActiveProductionPlannedMonth.Value;

        return DateTime.UtcNow.Month;
    }

    public static int ResolveProductionYear(NdtBundleOptions options, int plannedMonth)
    {
        if (options.ActiveProductionYear is > 2000)
            return options.ActiveProductionYear.Value;

        var now = DateTime.UtcNow;
        if (plannedMonth > now.Month)
            return now.Year - 1;
        return now.Year;
    }
}
