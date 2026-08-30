using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>Paths and file names for completed NDT bundle artifacts (summary CSV, ZPL tag).</summary>
internal static class NdtBundleOutputPaths
{
    /// <summary>
    /// Folder for <c>NDT_Bundle_{batchNo}.csv</c> and <c>NDT_Bundle_{batchNo}.zpl</c>.
    /// Uses <see cref="NdtBundleOptions.BundleSummaryOutputFolder"/> when set; otherwise <see cref="NdtBundleOptions.OutputBundleFolder"/>.
    /// </summary>
    public static string? ResolveBundleArtifactsFolder(NdtBundleOptions options)
    {
        var configured = (options.BundleSummaryOutputFolder ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(configured))
            return configured;

        var fallback = (options.OutputBundleFolder ?? string.Empty).Trim();
        return string.IsNullOrWhiteSpace(fallback) ? null : fallback;
    }

    public static string GetBundleCsvFileName(string ndtBatchNoFormatted) =>
        $"NDT_Bundle_{ndtBatchNoFormatted}.csv";

    public static string GetBundleZplFileName(string ndtBatchNoFormatted) =>
        $"NDT_Bundle_{ndtBatchNoFormatted}.zpl";

    /// <summary>
    /// After merge tombstone: <c>NDT_Bundle_{live}.csv/.zpl</c> → <c>NDT_Bundle_{live}V.csv/.zpl</c>.
    /// </summary>
    internal static void ArchiveBundleArtifacts(NdtBundleOptions options, string sourceBundleNo, string tombstone)
    {
        var folder = ResolveBundleArtifactsFolder(options);
        if (string.IsNullOrWhiteSpace(folder) || !Directory.Exists(folder))
            return;

        TryMove(GetBundleCsvFileName(sourceBundleNo), GetBundleCsvFileName(tombstone));
        TryMove(GetBundleZplFileName(sourceBundleNo), GetBundleZplFileName(tombstone));

        void TryMove(string fromName, string toName)
        {
            var from = Path.Combine(folder, fromName);
            var to = Path.Combine(folder, toName);
            if (File.Exists(from) && !File.Exists(to))
                File.Move(from, to);
        }
    }

    public static async Task TrySaveBundleZplAsync(
        NdtBundleOptions options,
        string ndtBatchNoFormatted,
        byte[] zplBytes,
        CancellationToken cancellationToken)
    {
        var folder = ResolveBundleArtifactsFolder(options);
        if (string.IsNullOrWhiteSpace(folder))
            return;

        Directory.CreateDirectory(folder);
        var fileName = GetBundleZplFileName(ndtBatchNoFormatted);
        var fullPath = Path.Combine(folder, fileName);
        await File.WriteAllBytesAsync(fullPath, zplBytes, cancellationToken).ConfigureAwait(false);
    }
}
