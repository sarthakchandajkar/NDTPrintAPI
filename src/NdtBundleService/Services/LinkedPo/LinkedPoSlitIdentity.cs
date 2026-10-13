using System.Globalization;
using System.Text.RegularExpressions;

namespace NdtBundleService.Services.LinkedPo;

/// <summary>
/// Linked-PO slits share one physical coil across two POs. Plant markers use a trailing
/// <c>_91</c> on the slit id (e.g. <c>2607388_01_91</c>); inbox files keep the base id
/// with two different PO suffixes (<c>2607388_01_yyMMdd_POA.csv</c> /
/// <c>2607388_01_yyMMdd_POB.csv</c>).
/// </summary>
public static class LinkedPoSlitIdentity
{
    public const string LinkedSuffix = "_91";

    private static readonly Regex InputSlitFileRegex = new(
        @"^(?<slit>.+)_(?<day>\d{6})_(?<po>\d{7,12})\.csv$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    public static bool HasLinkedSuffix(string? slitNo)
    {
        if (string.IsNullOrWhiteSpace(slitNo))
            return false;
        return slitNo.Trim().EndsWith(LinkedSuffix, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Strips a trailing <c>_91</c> linked-PO marker when present.</summary>
    public static string ToBaseSlitNo(string? slitNo)
    {
        var s = (slitNo ?? string.Empty).Trim();
        if (s.Length == 0)
            return string.Empty;
        if (s.EndsWith(LinkedSuffix, StringComparison.OrdinalIgnoreCase))
            return s[..^LinkedSuffix.Length];
        return s;
    }

    /// <summary>
    /// SAP NDT Input Slit basename must match TM Input Slit naming:
    /// <c>{BaseSlit}_{yyMMdd}_{PO}.csv</c> — never embed a trailing <c>_91</c> in the slit segment.
    /// </summary>
    public static string ToSapOutputFileName(string? fileName)
    {
        var name = Path.GetFileName(fileName ?? string.Empty);
        if (!TryParseInputSlitFileName(name, out var slit, out var day, out var po))
            return name;
        var baseSlit = ToBaseSlitNo(slit);
        if (baseSlit.Length == 0)
            return name;
        return $"{baseSlit}_{day}_{po}.csv";
    }

    public static bool SameBaseSlit(string? a, string? b)
    {
        var ba = ToBaseSlitNo(a);
        var bb = ToBaseSlitNo(b);
        if (ba.Length == 0 || bb.Length == 0)
            return false;
        return string.Equals(ba, bb, StringComparison.OrdinalIgnoreCase);
    }

    public static bool TryParseInputSlitFileName(
        string? fileName,
        out string slitNo,
        out string yyMMdd,
        out string poNumber)
    {
        slitNo = string.Empty;
        yyMMdd = string.Empty;
        poNumber = string.Empty;
        var name = Path.GetFileName(fileName ?? string.Empty);
        if (string.IsNullOrWhiteSpace(name))
            return false;

        var m = InputSlitFileRegex.Match(name);
        if (!m.Success)
            return false;

        slitNo = m.Groups["slit"].Value.Trim();
        yyMMdd = m.Groups["day"].Value;
        poNumber = InputSlitCsvParsing.NormalizePo(m.Groups["po"].Value);
        if (slitNo.Length == 0 || poNumber.Length == 0)
            return false;
        if (!DateTime.TryParseExact(yyMMdd, "yyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out _))
            return false;
        return true;
    }
}
