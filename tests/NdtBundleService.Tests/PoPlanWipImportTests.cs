using NdtBundleService.Configuration;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class PoPlanWipImportKeysTests
{
    [Fact]
    public void Format_includes_path_and_write_ticks()
    {
        const string path = @"\\server\PO Accepted\10001234.csv";
        const long ticks = 638000000000000000L;

        var key = PoPlanWipImportKeys.Format(path, ticks);

        Assert.StartsWith(path, key, StringComparison.Ordinal);
        Assert.EndsWith("|w:638000000000000000", key, StringComparison.Ordinal);
    }

    [Fact]
    public void Format_changes_when_file_write_ticks_change()
    {
        const string path = @"C:\PO Accepted\plan.csv";

        var first = PoPlanWipImportKeys.Format(path, 100L);
        var second = PoPlanWipImportKeys.Format(path, 200L);

        Assert.NotEqual(first, second);
    }

    [Fact]
    public void Format_truncates_overlong_paths_to_fit_source_file_column()
    {
        var path = new string('x', 600);
        var key = PoPlanWipImportKeys.Format(path, 123L);

        Assert.True(key.Length <= 500);
        Assert.EndsWith("|w:123", key, StringComparison.Ordinal);
    }
}

public sealed class PoPlanWipImportSettingsTests
{
    [Fact]
    public void GetImportMinUtc_uses_po_plan_import_cutoff_when_set()
    {
        var options = new NdtBundleOptions
        {
            PoPlanImportMinLastWriteUtc = "2026-06-01T00:00:00Z",
            MinSourceFileLastWriteUtc = "2025-01-01T00:00:00Z"
        };

        var minUtc = PoPlanWipImportSettings.GetImportMinUtc(options);

        Assert.Equal(new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc), minUtc);
    }

    [Fact]
    public void IsEnabled_requires_sql_and_import_flag()
    {
        Assert.False(PoPlanWipImportSettings.IsEnabled(new NdtBundleOptions
        {
            ImportPoPlanWipFromFolder = false,
            UseSqlServerForBundles = true,
            ConnectionString = "Server=.;Database=x;"
        }));

        Assert.True(PoPlanWipImportSettings.IsEnabled(new NdtBundleOptions
        {
            ImportPoPlanWipFromFolder = true,
            UseSqlServerForBundles = true,
            ConnectionString = "Server=.;Database=x;"
        }));
    }
}
