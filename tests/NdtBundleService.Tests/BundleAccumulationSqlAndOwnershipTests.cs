using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class BundleAccumulationSqlAndOwnershipTests
{
    [Fact]
    public void Increment_is_single_statement_MERGE_with_HOLDLOCK_additive_delta()
    {
        Assert.Contains("MERGE dbo.Bundle_Accumulation WITH (HOLDLOCK)", BundleAccumulationSql.IncrementMerge, StringComparison.Ordinal);
        Assert.Contains("t.Pcs + @Delta", BundleAccumulationSql.IncrementMerge, StringComparison.Ordinal);
        Assert.Contains("WHEN MATCHED AND t.Pcs + @Delta > 0 THEN", BundleAccumulationSql.IncrementMerge, StringComparison.Ordinal);
    }

    [Fact]
    public void Schema_script_indexes_support_sweep_max_activity_and_cutover_exists()
    {
        var script = File.ReadAllText(Path.Combine(FindRepoRoot(), "docs", "Bundle_Accumulation_AddTable.sql"));
        Assert.Contains("CK_Bundle_Accumulation_Pcs CHECK (Pcs > 0)", script, StringComparison.Ordinal);
        Assert.Contains("PRIMARY KEY (Mill_No, Po_Number, Size_Key)", script, StringComparison.Ordinal);
        Assert.Contains("IX_Bundle_Accumulation_Mill_Po_Activity", script, StringComparison.Ordinal);
        Assert.Contains("(Mill_No, Po_Number, Last_Activity_Utc)", script, StringComparison.Ordinal);
        Assert.Contains("IX_Bundle_Accumulation_Mill", script, StringComparison.Ordinal);
        Assert.Contains("RequireCleanFillCutover EXISTS", script, StringComparison.Ordinal);
        Assert.Contains("SELECT TOP (1) 1", BundleAccumulationSql.ExistsOpenForMill, StringComparison.Ordinal);
        Assert.Contains("WHERE Mill_No = @Mill", BundleAccumulationSql.ExistsOpenForMill, StringComparison.Ordinal);
        Assert.Contains("SELECT MAX(Last_Activity_Utc)", BundleAccumulationSql.MaxActivityForPo, StringComparison.Ordinal);
    }

    [Fact]
    public void Close_transaction_deletes_armed_accumulation_before_commit()
    {
        var src = File.ReadAllText(Path.Combine(FindRepoRoot(), "src", "NdtBundleService", "Services", "MillSequenceService.cs"));
        var allocate = src.IndexOf("AllocateAndInsertBundleAsync", StringComparison.Ordinal);
        var delete = src.IndexOf("DeleteArmedSizeInTxAsync", allocate, StringComparison.Ordinal);
        var commit = src.IndexOf("CommitAsync", delete, StringComparison.Ordinal);
        Assert.True(allocate >= 0 && delete > allocate && commit > delete);
    }

    [Fact]
    public void Mill_1_cannot_read_or_write_mill_2_accumulation()
    {
        var store = CreateStore(TestMillOwnership.Mill(1));
        Assert.Throws<InvalidOperationException>(() =>
            store.IncrementSizeCount("1000000002", millNo: 2, "Default", 3));
        Assert.Empty(store.GetSizeCounts("1000000002", 2));
        Assert.Null(store.GetLastRecord("1000000002", 2));
    }

    [Fact]
    public void Shared_empty_owned_set_can_write_mill_1()
    {
        var store = CreateStore(TestMillOwnership.Shared());
        store.IncrementSizeCount("1000000001", millNo: 1, "Default", 4);
        Assert.Equal(4, store.GetSizeCounts("1000000001", 1)["Default"]);
    }

    [Fact]
    public void GetSizeCounts_does_not_insert_empty_row()
    {
        var store = CreateStore(TestMillOwnership.Monolith());
        var counts = store.GetSizeCounts("PO-EMPTY", 1);
        Assert.Empty(counts);
        Assert.Equal(0, store.GetRunningTotal("PO-EMPTY", 1));
        Assert.False(store.HasUnsafeOpenStateForFillCutover(1));
    }

    [Fact]
    public void LastRecord_roundtrip_in_memory_is_the_print_context_not_sql_newest_slit()
    {
        var store = CreateStore(TestMillOwnership.Monolith());
        var held = new InputSlitRecord
        {
            PoNumber = "1000060288",
            MillNo = 1,
            SlitNo = "SL-10",
            SlitStartTime = new DateTime(2026, 8, 15, 10, 0, 0, DateTimeKind.Utc)
        };
        store.SetLastRecord("1000060288", 1, held);
        var got = store.GetLastRecord("1000060288", 1);
        Assert.Equal("SL-10", got?.SlitNo);
    }

    [Fact]
    public void MillOwnership_Allows_shared_all_mills_mill_n_only_own()
    {
        Assert.True(TestMillOwnership.Shared().Allows(1));
        Assert.True(TestMillOwnership.Shared().Allows(4));
        Assert.False(TestMillOwnership.Shared().Owns(1));
        Assert.True(TestMillOwnership.Mill(1).Allows(1));
        Assert.False(TestMillOwnership.Mill(1).Allows(2));
        Assert.True(TestMillOwnership.Monolith().Allows(3));
    }

    private static NdtBundleRuntimeStateStore CreateStore(IMillOwnership ownership) =>
        new(
            new OptionsMonitorStub(new NdtBundleOptions { UseSqlServerForBundles = false }),
            ownership,
            NullLogger<NdtBundleRuntimeStateStore>.Instance);

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            if (File.Exists(Path.Combine(dir.FullName, "NDTPrintAPI.sln")) ||
                Directory.Exists(Path.Combine(dir.FullName, "docs")))
                return dir.FullName;
            dir = dir.Parent;
        }

        throw new InvalidOperationException("Repository root not found.");
    }

    private sealed class OptionsMonitorStub(NdtBundleOptions value) : IOptionsMonitor<NdtBundleOptions>
    {
        public NdtBundleOptions CurrentValue { get; } = value;
        public NdtBundleOptions Get(string? name) => CurrentValue;
        public IDisposable? OnChange(Action<NdtBundleOptions, string?> listener) => null;
    }
}
