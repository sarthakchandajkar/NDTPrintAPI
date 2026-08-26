using System.Diagnostics;
using System.Text;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class SplitMillStateFilesTests
{
    [Fact]
    public void Split_script_fails_loudly_on_lifecycle_object_shape()
    {
        var dir = Path.Combine(Path.GetTempPath(), "split-life-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            File.WriteAllText(
                Path.Combine(dir, "NdtBundleRuntimeState.json"),
                """{"version":1,"mills":{"PO|1":{"poNumber":"PO","millNo":1}},"millMaxSequence":{"1":1}}""",
                Encoding.UTF8);
            // Wrong shape: object instead of array — historically wrote [] silently.
            File.WriteAllText(
                Path.Combine(dir, "PoLifecycleState.json"),
                """{"entries":[{"millNo":1,"poNumber":"PO","phase":"Closed"}]}""",
                Encoding.UTF8);

            var (exit, stderr) = RunSplit(dir);
            Assert.NotEqual(0, exit);
            Assert.Contains("must be a JSON array", stderr, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* ignore */ }
        }
    }

    [Fact]
    public void Split_script_validates_entry_counts_in_vs_out()
    {
        var dir = Path.Combine(Path.GetTempPath(), "split-ok-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            File.WriteAllText(
                Path.Combine(dir, "NdtBundleRuntimeState.json"),
                """
                {
                  "version": 1,
                  "millMaxSequence": { "1": 10, "4": 20 },
                  "mills": {
                    "1000000001|1": { "poNumber": "1000000001", "millNo": 1, "batchOffset": 10 },
                    "1000000004|4": { "poNumber": "1000000004", "millNo": 4, "batchOffset": 20 }
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(dir, "PoLifecycleState.json"),
                """
                [
                  { "millNo": 1, "poNumber": "1000000001", "phase": "Closed" },
                  { "millNo": 4, "poNumber": "1000000004", "phase": "Draining" }
                ]
                """,
                Encoding.UTF8);

            var (exit, output) = RunSplit(dir);
            Assert.Equal(0, exit);
            Assert.Contains("Validated runtime slots in/out=2", output, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("lifecycle entries in/out=2", output, StringComparison.OrdinalIgnoreCase);

            var life1 = File.ReadAllText(Path.Combine(dir, "PoLifecycleState-M1.json"));
            var life4 = File.ReadAllText(Path.Combine(dir, "PoLifecycleState-M4.json"));
            Assert.Contains("1000000001", life1, StringComparison.Ordinal);
            Assert.Contains("1000000004", life4, StringComparison.Ordinal);
            Assert.DoesNotContain("1000000004", life1, StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* ignore */ }
        }
    }

    private static (int ExitCode, string Output) RunSplit(string dir)
    {
        var script = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory,
            "..", "..", "..", "..", "..",
            "scripts", "Split-MillStateFiles.ps1"));
        if (!File.Exists(script))
        {
            script = Path.GetFullPath(Path.Combine(
                Directory.GetCurrentDirectory(),
                "scripts", "Split-MillStateFiles.ps1"));
        }

        Assert.True(File.Exists(script), $"Split script not found near test output. Tried: {script}");

        var psi = new ProcessStartInfo
        {
            FileName = "powershell.exe",
            Arguments =
                $"-NoProfile -ExecutionPolicy Bypass -File \"{script}\" " +
                $"-SourceRuntimeStateFile \"{Path.Combine(dir, "NdtBundleRuntimeState.json")}\"",
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        using var proc = Process.Start(psi)!;
        var stdout = proc.StandardOutput.ReadToEnd();
        var stderr = proc.StandardError.ReadToEnd();
        proc.WaitForExit(60_000);
        return (proc.ExitCode, stdout + stderr);
    }
}
