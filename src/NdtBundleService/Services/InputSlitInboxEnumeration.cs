namespace NdtBundleService.Services;

/// <summary>
/// SAP Input Slit exports may have no file extension (comma-separated content identical to .csv).
/// Enumerates those plus <c>.csv</c> files. Read-only listing—callers must not move or delete inbox files.
/// </summary>
public static class InputSlitInboxEnumeration
{
    /// <summary>True for extensionless slit exports or <c>.csv</c> / <c>.CSV</c>; excludes common junk names.</summary>
    public static bool IsEligibleInboxFile(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return false;

        var name = Path.GetFileName(path);
        if (string.IsNullOrEmpty(name))
            return false;

        if (name.Equals("desktop.ini", StringComparison.OrdinalIgnoreCase)
            || name.Equals("Thumbs.db", StringComparison.OrdinalIgnoreCase))
            return false;

        var ext = Path.GetExtension(path);
        return string.IsNullOrEmpty(ext) || ext.Equals(".csv", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>All eligible slit inbox files in <paramref name="folder"/> (non-recursive).</summary>
    public static IEnumerable<string> EnumerateFiles(string folder)
    {
        foreach (var path in Directory.EnumerateFiles(folder, "*", SearchOption.TopDirectoryOnly))
        {
            if (IsEligibleInboxFile(path))
                yield return path;
        }
    }
}
