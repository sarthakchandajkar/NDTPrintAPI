using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>Scans TM bundle WIP folders for <c>WIP_MM_PO_...</c> filenames.</summary>
public static class WipBundleFolderScanner
{
    public readonly record struct WipBundleFileCandidate(
        int MillNo,
        DateTime StampUtc,
        string PoNumber,
        string SortKey,
        string FileName,
        string FullPath);

    public static IReadOnlyList<WipBundleFileCandidate> Scan(NdtBundleOptions options)
    {
        var candidates = new List<WipBundleFileCandidate>();
        foreach (var folder in ResolveBundleFolders(options))
        {
            ScanFolder(folder, candidates);
        }

        return candidates;
    }

    public static IEnumerable<string> ResolveBundleFolders(NdtBundleOptions options)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var live = options.MillSlitLive ?? new MillSlitLiveOptions();

        foreach (var folder in new[]
                 {
                     live.WipBundleFolder,
                     live.WipBundleAcceptedFolder,
                     options.FgBundleFolder,
                     options.FgBundleAcceptedFolder
                 })
        {
            var trimmed = (folder ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(trimmed) || !seen.Add(trimmed))
                continue;
            yield return trimmed;
        }
    }

    private static void ScanFolder(string folder, List<WipBundleFileCandidate> outCandidates)
    {
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return;

        foreach (var path in Directory.EnumerateFiles(folder))
        {
            var name = Path.GetFileName(path);
            if (!name.StartsWith("WIP_", StringComparison.OrdinalIgnoreCase))
                continue;

            var meta = WipBundleFileName.TryParse(name);
            if (meta is null)
                continue;

            DateTime stampUtc;
            try
            {
                stampUtc = File.GetLastWriteTimeUtc(path);
                if (stampUtc == DateTime.MinValue)
                    stampUtc = File.GetCreationTimeUtc(path);
            }
            catch
            {
                stampUtc = DateTime.UtcNow;
            }

            outCandidates.Add(new WipBundleFileCandidate(
                meta.MillNo,
                stampUtc,
                meta.PoNumber,
                meta.SortKey,
                name,
                path));
        }
    }
}
