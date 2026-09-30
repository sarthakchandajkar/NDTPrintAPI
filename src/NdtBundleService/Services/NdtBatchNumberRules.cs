namespace NdtBundleService.Services;

/// <summary>Rules for when an NDT Input Slit output row should omit the NDT Batch No column.</summary>
public static class NdtBatchNumberRules
{
    /// <summary>
    /// Hollow FG pipes (e.g. Pipe Type FG, Pipe Size 50x50 / 100x40) never produce NDT pipes;
    /// output CSV NDT Batch No is left blank (no <c>10001</c> placeholder).
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
