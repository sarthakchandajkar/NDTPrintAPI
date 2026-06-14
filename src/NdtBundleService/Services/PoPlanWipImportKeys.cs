namespace NdtBundleService.Services;

/// <summary>
/// Builds stable <c>PO_Plan_WIP.Source_File</c> keys for idempotent folder import
/// (path + last-write ticks; skip re-import when unchanged).
/// </summary>
internal static class PoPlanWipImportKeys
{
    private const int MaxSourceFileLength = 500;

    public static string Format(string filePath, long lastWriteUtcTicks)
    {
        var path = (filePath ?? string.Empty).Trim();
        var suffix = FormattableString.Invariant($"|w:{lastWriteUtcTicks}");
        if (path.Length + suffix.Length <= MaxSourceFileLength)
            return path + suffix;

        var keep = Math.Max(1, MaxSourceFileLength - suffix.Length);
        return path[..keep] + suffix;
    }
}
