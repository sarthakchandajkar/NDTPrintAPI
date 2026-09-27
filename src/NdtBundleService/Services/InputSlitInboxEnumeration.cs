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

    /// <summary>
    /// Inbox ∪ Accepted, de-duplicated by file name (case-insensitive). Inbox wins when both exist
    /// so live SAP drops are preferred over the Accepted archive during drain transitions.
    /// </summary>
    public static IReadOnlyList<string> EnumerateInboxPreferOverAccepted(
        string? inboxFolder,
        string? acceptedFolder)
    {
        var byName = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        void AddFolder(string? folder, bool overwrite)
        {
            var trimmed = (folder ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(trimmed) || !Directory.Exists(trimmed))
                return;

            foreach (var path in EnumerateFiles(trimmed))
            {
                var name = Path.GetFileName(path);
                if (string.IsNullOrEmpty(name))
                    continue;
                if (overwrite || !byName.ContainsKey(name))
                    byName[name] = Path.GetFullPath(path);
            }
        }

        // Accepted first, then inbox overwrites — inbox preferred.
        AddFolder(acceptedFolder, overwrite: false);
        AddFolder(inboxFolder, overwrite: true);
        return byName.Values.ToList();
    }
}
