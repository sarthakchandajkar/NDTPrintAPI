namespace NdtBundleService.Services;

/// <summary>
/// Picks the upload CSV <c>Slit_No</c>: prefer <c>Context_Slit_No</c> when present and not claimed by
/// another bundle; otherwise the batch's <c>Output_Slit_Row</c> with the latest <c>Slit_Finish_Time</c>
/// that is still unclaimed.
/// </summary>
public static class UploadBundleSlitNoSelection
{
    public static string Pick(
        string? contextSlitNo,
        IReadOnlyList<(string SlitNo, DateTime? SlitFinishTime)> outputSlitsByFinishDesc,
        IReadOnlyCollection<string> claimedByOtherBundles)
    {
        var claimed = claimedByOtherBundles as HashSet<string>
            ?? new HashSet<string>(claimedByOtherBundles ?? Array.Empty<string>(), StringComparer.OrdinalIgnoreCase);

        var context = (contextSlitNo ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(context)
            && !IsPlaceholderSlit(context)
            && !claimed.Contains(context))
        {
            return context;
        }

        foreach (var (slitNo, _) in outputSlitsByFinishDesc
                     .OrderByDescending(x => x.SlitFinishTime.HasValue)
                     .ThenByDescending(x => x.SlitFinishTime)
                     .ThenBy(x => x.SlitNo, StringComparer.OrdinalIgnoreCase))
        {
            var candidate = (slitNo ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(candidate) || IsPlaceholderSlit(candidate))
                continue;
            if (claimed.Contains(candidate))
                continue;
            return candidate;
        }

        return string.Empty;
    }

    private static bool IsPlaceholderSlit(string slitNo) =>
        slitNo is "—" or "-" or "?"
        || string.Equals(slitNo, "n/a", StringComparison.OrdinalIgnoreCase);
}
