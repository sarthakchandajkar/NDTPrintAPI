using NdtBundleService.Configuration;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class NdtBundleOutputPathsTests
{
    [Fact]
    public void Bundle_artifact_write_flags_are_off_by_default()
    {
        var options = new NdtBundleOptions();
        Assert.False(options.EnableBundleSummaryCsvFiles);
        Assert.False(options.EnableBundleZplPreviewFiles);
    }

    [Fact]
    public async Task TrySaveBundleZplAsync_does_not_write_when_preview_flag_is_off()
    {
        var dir = CreateTempDir();
        try
        {
            var options = new NdtBundleOptions
            {
                EnableBundleZplPreviewFiles = false,
                BundleSummaryOutputFolder = dir
            };

            var saved = await NdtBundleOutputPaths.TrySaveBundleZplAsync(
                options, "1226100001", "^XA^XZ"u8.ToArray(), CancellationToken.None);

            Assert.False(saved);
            Assert.Empty(Directory.GetFiles(dir));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public async Task TrySaveBundleZplAsync_does_not_write_into_output_bundle_folder()
    {
        var sapPickup = CreateTempDir();
        try
        {
            var options = new NdtBundleOptions
            {
                EnableBundleZplPreviewFiles = true,
                BundleSummaryOutputFolder = "",
                OutputBundleFolder = sapPickup
            };

            var saved = await NdtBundleOutputPaths.TrySaveBundleZplAsync(
                options, "1226100001", "^XA^XZ"u8.ToArray(), CancellationToken.None);

            Assert.False(saved);
            Assert.Empty(Directory.GetFiles(sapPickup));
        }
        finally
        {
            Directory.Delete(sapPickup, recursive: true);
        }
    }

    [Fact]
    public async Task TrySaveBundleZplAsync_writes_when_preview_flag_is_on()
    {
        var dir = CreateTempDir();
        try
        {
            var options = new NdtBundleOptions
            {
                EnableBundleZplPreviewFiles = true,
                BundleSummaryOutputFolder = dir
            };

            var saved = await NdtBundleOutputPaths.TrySaveBundleZplAsync(
                options, "1226100001", "^XA^XZ"u8.ToArray(), CancellationToken.None);

            Assert.True(saved);
            Assert.True(File.Exists(Path.Combine(dir, "NDT_Bundle_1226100001.zpl")));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ResolveBundleSummaryWriteFolder_ignores_output_bundle_fallback()
    {
        var options = new NdtBundleOptions
        {
            BundleSummaryOutputFolder = "",
            OutputBundleFolder = @"Z:\To SAP\TM\NDT\NDT Input Slit\Input Slit"
        };

        Assert.Null(NdtBundleOutputPaths.ResolveBundleSummaryWriteFolder(options));
        Assert.Equal(options.OutputBundleFolder, NdtBundleOutputPaths.ResolveBundleArtifactsFolder(options));
    }

    private static string CreateTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "ndt-bundle-artifacts-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }
}
