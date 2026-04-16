namespace NdtBundleService.Services;

/// <summary>Shared helpers for generated CSV filenames (SAP / operator folders).</summary>
internal static class CsvOutputFileNaming
{
    /// <summary>Removes characters invalid in Windows file names and normalizes spaces to underscores.</summary>
    public static string SanitizeToken(string? value, string fallback = "NA")
    {
        if (string.IsNullOrWhiteSpace(value))
            return fallback;
        var s = value.Trim();
        foreach (var c in Path.GetInvalidFileNameChars())
            s = s.Replace(c, '_');
        s = s.Replace(' ', '_');
        return string.IsNullOrEmpty(s) ? fallback : s;
    }

    public static string DateAndTimeParts(DateTime dt) =>
        $"{dt:yyyyMMdd}_{dt:HHmmss}";
}
