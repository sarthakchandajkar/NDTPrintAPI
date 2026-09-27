using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class InputSlitInboxEnumerationTests
{
    [Fact]
    public void EnumerateInboxPreferOverAccepted_dedupes_by_name_preferring_inbox()
    {
        var root = Path.Combine(Path.GetTempPath(), "slit-enum-" + Guid.NewGuid().ToString("N"));
        var inbox = Path.Combine(root, "inbox");
        var accepted = Path.Combine(root, "accepted");
        Directory.CreateDirectory(inbox);
        Directory.CreateDirectory(accepted);

        try
        {
            File.WriteAllText(Path.Combine(accepted, "shared.csv"), "accepted");
            File.WriteAllText(Path.Combine(inbox, "shared.csv"), "inbox");
            File.WriteAllText(Path.Combine(accepted, "only-accepted.csv"), "a");
            File.WriteAllText(Path.Combine(inbox, "only-inbox.csv"), "i");

            var paths = InputSlitInboxEnumeration.EnumerateInboxPreferOverAccepted(inbox, accepted);
            var byName = paths
                .Where(p => !string.IsNullOrEmpty(Path.GetFileName(p)))
                .ToDictionary(p => Path.GetFileName(p)!, StringComparer.OrdinalIgnoreCase);

            Assert.Equal(3, byName.Count);
            Assert.Equal(Path.GetFullPath(Path.Combine(inbox, "shared.csv")), byName["shared.csv"]);
            Assert.True(byName.ContainsKey("only-accepted.csv"));
            Assert.True(byName.ContainsKey("only-inbox.csv"));
            Assert.Equal("inbox", File.ReadAllText(byName["shared.csv"]));
        }
        finally
        {
            try { Directory.Delete(root, recursive: true); } catch { /* ignore */ }
        }
    }
}
