using System.Globalization;

namespace NdtBundleService.Services;

/// <summary>Parses and formats NDT batch numbers (<c>12</c> + YY + mill + 5-digit sequence).</summary>
public static class NdtBundleSequence
{
    public const int BatchNoLength = 10;

    public static string Format(int sequenceNumber, int millNo, DateTime? asOf = null)
    {
        var when = asOf ?? DateTime.Now;
        var yy = (when.Year % 100).ToString("D2", CultureInfo.InvariantCulture);
        var millDigit = millNo is >= 1 and <= 4 ? millNo.ToString(CultureInfo.InvariantCulture) : "1";
        var seq = sequenceNumber.ToString("D5", CultureInfo.InvariantCulture);
        return "12" + yy + millDigit + seq;
    }

    /// <summary>Returns the 5-digit sequence when the batch matches the current calendar year and mill; otherwise 0.</summary>
    public static bool TryParseSequenceForCurrentYear(string? bundleNo, int millNo, out int sequence) =>
        TryParseSequence(bundleNo, millNo, out sequence, DateTime.Now);

    /// <summary>Returns the 5-digit sequence when the batch matches the given year and mill.</summary>
    public static bool TryParseSequence(string? bundleNo, int millNo, out int sequence, DateTime? asOf = null)
    {
        sequence = 0;
        if (string.IsNullOrWhiteSpace(bundleNo) || bundleNo.Trim().Length != BatchNoLength)
            return false;

        var s = bundleNo.Trim();
        if (!s.StartsWith("12", StringComparison.Ordinal))
            return false;

        var yy = (asOf ?? DateTime.Now).Year % 100;
        if (!int.TryParse(s.AsSpan(2, 2), NumberStyles.None, CultureInfo.InvariantCulture, out var fileYy))
            return false;
        if (fileYy != yy)
            return false;

        if (!int.TryParse(s.AsSpan(4, 1), NumberStyles.None, CultureInfo.InvariantCulture, out var fileMill))
            return false;
        if (fileMill != millNo)
            return false;

        return int.TryParse(s.AsSpan(5, 5), NumberStyles.None, CultureInfo.InvariantCulture, out sequence);
    }
}
