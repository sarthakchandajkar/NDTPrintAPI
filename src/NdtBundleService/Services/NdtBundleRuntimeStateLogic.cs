namespace NdtBundleService.Services;

/// <summary>Pure rules for open-bundle remainder (size counts only).</summary>
public static class NdtBundleRuntimeStateLogic
{
    public static bool HasOpenPartialBundle(IReadOnlyDictionary<string, int>? sizeCounts)
    {
        if (sizeCounts is null || sizeCounts.Count == 0)
            return false;

        foreach (var count in sizeCounts.Values)
        {
            if (count > 0)
                return true;
        }

        return false;
    }
}
