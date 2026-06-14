using System.Globalization;
using System.Text.RegularExpressions;

namespace NdtBundleService.Services;

/// <summary>
/// Parses TM bundle WIP filenames such as <c>WIP_03_1000059168_2603008928_260614_084704</c>.
/// </summary>
public static class WipBundleFileName
{
    private static readonly Regex ReFull = new(
        @"^WIP_(\d{2})_(\d+)_(\d+)_(\d{6})_(\d{6})(\.csv)?$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private static readonly Regex ReShort = new(
        @"^WIP_(\d{2})_(\d+)_(\d{6})_(\d{6})\.csv$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    public static WipBundleFileMeta? TryParse(string fileName)
    {
        var m = ReFull.Match(fileName);
        if (m.Success)
        {
            return TryBuildMeta(m.Groups[1].Value, m.Groups[2].Value, m.Groups[4].Value + "_" + m.Groups[5].Value);
        }

        m = ReShort.Match(fileName);
        if (m.Success)
        {
            return TryBuildMeta(m.Groups[1].Value, m.Groups[2].Value, m.Groups[3].Value + "_" + m.Groups[4].Value);
        }

        return null;
    }

    private static WipBundleFileMeta? TryBuildMeta(string millDigits, string poNumber, string sortKey)
    {
        if (!int.TryParse(millDigits, NumberStyles.Integer, CultureInfo.InvariantCulture, out var millNo) || millNo is < 1 or > 4)
            return null;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po))
            return null;

        return new WipBundleFileMeta(millNo, po, sortKey);
    }

    public sealed record WipBundleFileMeta(int MillNo, string PoNumber, string SortKey);
}
