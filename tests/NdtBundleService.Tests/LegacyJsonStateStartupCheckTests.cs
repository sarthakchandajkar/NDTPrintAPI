using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class LegacyJsonStateStartupCheckTests
{
    [Fact]
    public async Task Throws_when_leftover_runtime_json_exists_under_output_parent()
    {
        var root = Path.Combine(Path.GetTempPath(), "legacy-json-" + Guid.NewGuid().ToString("N"));
        var output = Path.Combine(root, "Input Slit");
        Directory.CreateDirectory(output);
        var leftover = Path.Combine(root, "NdtBundleRuntimeState-M1.json");
        await File.WriteAllTextAsync(leftover, "{}");

        try
        {
            var sut = new LegacyJsonStateStartupCheck(
                new OptionsMonitorStub(new NdtBundleOptions { OutputBundleFolder = output }),
                NullLogger<LegacyJsonStateStartupCheck>.Instance);

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => sut.StartAsync(CancellationToken.None));
            Assert.Contains("Leftover mill-state JSON", ex.Message, StringComparison.Ordinal);
            Assert.Contains("NdtBundleRuntimeState-M1.json", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Passes_when_no_leftover_files()
    {
        var root = Path.Combine(Path.GetTempPath(), "legacy-json-clean-" + Guid.NewGuid().ToString("N"));
        var output = Path.Combine(root, "Input Slit");
        Directory.CreateDirectory(output);
        try
        {
            var sut = new LegacyJsonStateStartupCheck(
                new OptionsMonitorStub(new NdtBundleOptions { OutputBundleFolder = output }),
                NullLogger<LegacyJsonStateStartupCheck>.Instance);
            await sut.StartAsync(CancellationToken.None);
        }
        finally
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public void FindLeftoverFiles_lists_printer_and_lifecycle_names()
    {
        Assert.Contains("PoLifecycleState.json", LegacyJsonStateStartupCheck.FileNames);
        Assert.Contains("MillPrinterSettings-M4.json", LegacyJsonStateStartupCheck.FileNames);
    }

    private sealed class OptionsMonitorStub(NdtBundleOptions value) : IOptionsMonitor<NdtBundleOptions>
    {
        public NdtBundleOptions CurrentValue { get; } = value;
        public NdtBundleOptions Get(string? name) => CurrentValue;
        public IDisposable? OnChange(Action<NdtBundleOptions, string?> listener) => null;
    }
}
