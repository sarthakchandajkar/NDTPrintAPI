namespace NdtBundleService.Services;

/// <summary>Rules for when an NDT Input Slit output row should omit the NDT Batch No column.</summary>
public static class NdtBatchNumberRules
{
    /// <summary>
    /// Hollow FG pipes (e.g. Pipe Type FG, Pipe Size 100x40) never produce NDT pipes and do not get a batch number.
    /// </summary>
    public static bool ShouldOmitNdtBatchNumber(string? pipeType, string? pipeSize)
    {
        var type = (pipeType ?? string.Empty).Trim();
        var size = (pipeSize ?? string.Empty).Trim();
        if (string.IsNullOrEmpty(size) || string.IsNullOrEmpty(type))
            return false;

        if (!type.Equals("FG", StringComparison.OrdinalIgnoreCase))
            return false;

        return size.Contains('x', StringComparison.OrdinalIgnoreCase);
    }
}
