namespace NdtBundleService.Configuration;

/// <summary>Per-mill NDT Batch No column behaviour for Input Slit output CSVs.</summary>
public sealed class MillCsvBatchModeEntry
{
    /// <summary><c>FillToTarget</c> or <c>Constant</c>.</summary>
    public string Mode { get; set; } = "FillToTarget";

    /// <summary>Literal batch column value when <see cref="Mode"/> is <c>Constant</c>.</summary>
    public string Value { get; set; } = "10001";

    public bool IsConstant =>
        string.Equals(Mode, "Constant", StringComparison.OrdinalIgnoreCase);

    public bool IsFillToTarget =>
        !IsConstant;
}

/// <summary>Helpers for <see cref="NdtBundleOptions.MillCsvBatchMode"/>.</summary>
public static class MillCsvBatchModeResolver
{
    public static MillCsvBatchModeEntry Resolve(NdtBundleOptions options, int millNo)
    {
        if (options.MillCsvBatchMode != null
            && options.MillCsvBatchMode.TryGetValue(millNo.ToString(), out var entry)
            && entry != null)
        {
            return entry;
        }

        // Mills 2–4 default to constant until rolled out; Mill-1 defaults to fill-to-target.
        return millNo is >= 2 and <= 4
            ? new MillCsvBatchModeEntry { Mode = "Constant", Value = "10001" }
            : new MillCsvBatchModeEntry { Mode = "FillToTarget" };
    }
}
