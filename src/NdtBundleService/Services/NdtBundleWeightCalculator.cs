using System.Globalization;
using System.Text.RegularExpressions;

namespace NdtBundleService.Services;

/// <summary>
/// Computes total bundle weight (kg) for NDT tags and SAP upload files from WIP/PO plan fields.
/// </summary>
public static class NdtBundleWeightCalculator
{
    private static readonly Regex LeadingNumber = new(
        @"^\s*([+-]?(?:\d+(?:\.\d+)?|\.\d+))",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Total bundle weight = pipe weight per meter × pipe length (m) × number of pipes in the bundle.
    /// When length is unavailable, falls back to weight per meter × pipe count (legacy upload behavior).
    /// </summary>
    public static string FormatBundleWeight(string? pipeWeightPerMeter, string? pipeLength, int pcsInBundle)
    {
        if (pcsInBundle <= 0)
            return string.Empty;

        if (!TryParsePositiveDecimal(pipeWeightPerMeter, out var perMeter))
            return string.Empty;

        if (TryParsePositiveDecimal(pipeLength, out var lengthMeters))
        {
            var total = perMeter * lengthMeters * pcsInBundle;
            return FormatDecimal(total);
        }

        var fallback = perMeter * pcsInBundle;
        return FormatDecimal(fallback);
    }

    public static bool TryParsePositiveDecimal(string? raw, out decimal value)
    {
        value = 0;
        if (string.IsNullOrWhiteSpace(raw))
            return false;

        var trimmed = raw.Trim();
        if (decimal.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out value) && value > 0)
            return true;

        var match = LeadingNumber.Match(trimmed);
        if (!match.Success)
            return false;

        return decimal.TryParse(match.Groups[1].Value, NumberStyles.Float, CultureInfo.InvariantCulture, out value)
               && value > 0;
    }

    private static string FormatDecimal(decimal value) =>
        value.ToString("0.###", CultureInfo.InvariantCulture);
}
