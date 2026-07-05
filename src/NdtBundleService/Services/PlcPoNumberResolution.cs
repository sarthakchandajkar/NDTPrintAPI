using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Resolves MES PO numbers from PLC DB251 PO_Id values, with validation before trusting small or non-SAP values.
/// </summary>
public static class PlcPoNumberResolution
{
    /// <summary>
    /// Returns true when <paramref name="poId"/> is in range and formats to a plausible SAP-style PO number.
    /// </summary>
    public static bool TryResolveFromPlcPoId(int poId, PlcPoEndOptions cfg, out string normalizedPo)
    {
        normalizedPo = string.Empty;
        if (poId < cfg.MinValidPoId || poId > cfg.MaxValidPoId)
            return false;

        var formatted = PlcPoIdFormatting.Format(poId, cfg.PoNumberFormatFromPlc);
        if (string.IsNullOrWhiteSpace(formatted))
            return false;

        normalizedPo = InputSlitCsvParsing.NormalizePo(formatted);
        return IsPlausibleMesPoNumber(normalizedPo, cfg.MinSapPoNumberDigits);
    }

    /// <summary>Production SAP PO numbers are numeric and typically at least 10 digits.</summary>
    public static bool IsPlausibleMesPoNumber(string? normalizedPo, int minDigits)
    {
        if (string.IsNullOrWhiteSpace(normalizedPo))
            return false;

        var min = Math.Max(1, minDigits);
        var s = normalizedPo.Trim();
        if (s.Length < min)
            return false;

        foreach (var ch in s)
        {
            if (!char.IsDigit(ch))
                return false;
        }

        return true;
    }
}
