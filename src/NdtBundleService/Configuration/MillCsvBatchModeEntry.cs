namespace NdtBundleService.Configuration;

/// <summary>Per-mill NDT Batch No column behaviour for Input Slit output CSVs.</summary>
public sealed class MillCsvBatchModeEntry
{
    /// <summary><c>FillToTarget</c> or <c>Constant</c>.</summary>
    public string Mode { get; set; } = "FillToTarget";

    /// <summary>Literal batch column value when <see cref="Mode"/> is <c>Constant</c>.</summary>
    public string Value { get; set; } = "10001";

    /// <summary>
    /// CSV batch value for zero-NDT rows (NdtPipes &lt;= 0). Written to CSV only;
    /// SQL <c>Output_Slit_Row.NDT_Batch_No</c> stays NULL (no <c>NDT_Bundle</c> parent).
    /// </summary>
    public string ZeroNdtValue { get; set; } = "10001";

    /// <summary>
    /// CSV batch value for hollow FG rows (Pipe Type FG + size like 50x50).
    /// Default empty — column left blank; SQL <c>Output_Slit_Row.NDT_Batch_No</c> stays NULL
    /// (no <c>NDT_Bundle</c> parent). Set explicitly only if a placeholder is required.
    /// </summary>
    public string HollowFgValue { get; set; } = "";

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

    /// <summary>
    /// Reconcile bundle list / merge only include mills on FillToTarget (real NDT batch numbers).
    /// Constant mills (placeholder like <c>10001</c>) stay hidden until rolled over in config.
    /// </summary>
    public static bool IsIncludedInReconcileBundleList(NdtBundleOptions options, int millNo) =>
        Resolve(options, millNo).IsFillToTarget;

    /// <summary>
    /// Resolves the CSV batch column and whether SQL should link an <c>NDT_Bundle</c> parent.
    /// Constant / zero-NDT / hollow-FG values go to CSV only (<paramref name="LinkBundleParent"/> = false).
    /// </summary>
    public static (string CsvBatchNo, bool LinkBundleParent) ResolveNonFillCsvBatch(
        MillCsvBatchModeEntry entry,
        bool isHollowFg,
        int ndtPipes)
    {
        if (isHollowFg)
            return ((entry.HollowFgValue ?? string.Empty).Trim(), false);

        if (ndtPipes <= 0)
            return (NullToDefault(entry.ZeroNdtValue), false);

        if (entry.IsConstant)
            return (NullToDefault(entry.Value), false);

        return (string.Empty, true);
    }

    private static string NullToDefault(string? value) =>
        string.IsNullOrWhiteSpace(value) ? "10001" : value.Trim();
}
