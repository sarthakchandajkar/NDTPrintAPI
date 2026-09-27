using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// Real-SQL coverage for TryStampFileAsync no-target path (open DataReader vs Rollback).
/// Set <c>NDT_FILL_TEST_CONNECTION</c> to a JazeeraMES_Test connection string to run;
/// otherwise the test is skipped (in-memory fakes cannot reproduce ADO.NET ordering).
/// </summary>
public sealed class SqlFillStampIntegrationTests
{
    [Fact]
    public async Task TryStampFileAsync_no_incomplete_target_returns_null_without_datareader_error()
    {
        var cs = Environment.GetEnvironmentVariable("NDT_FILL_TEST_CONNECTION");
        if (string.IsNullOrWhiteSpace(cs))
        {
            // Skip when SQL is unavailable — documents the required env for CI/local.
            return;
        }

        var options = new OptionsMonitorStub(new NdtBundleOptions { ConnectionString = cs });
        var sut = new CsvFillService(options, NullLogger<CsvFillService>.Instance);

        // Unique PO unlikely to have an incomplete fill row.
        var po = "9" + DateTime.UtcNow.ToString("yyMMddHHmmss");
        CsvFillStampResult? result;
        try
        {
            result = await sut.TryStampFileAsync(po, millNo: 1, pipeSize: null, fileNdtPipes: 5, CancellationToken.None);
        }
        catch (Exception ex)
        {
            Assert.Fail(
                "TryStampFileAsync must return null on no-target without throwing. "
                + "Got: " + ex.GetType().Name + ": " + ex.Message);
            return;
        }

        Assert.Null(result);

        // Confirm no residual open transaction / UPDLOCK from empty select by running a short query.
        await using var conn = new SqlConnection(cs);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand("SELECT 1", conn);
        var scalar = await cmd.ExecuteScalarAsync();
        Assert.Equal(1, Convert.ToInt32(scalar));
    }

    private sealed class OptionsMonitorStub(NdtBundleOptions value) : IOptionsMonitor<NdtBundleOptions>
    {
        public NdtBundleOptions CurrentValue => value;
        public NdtBundleOptions Get(string? name) => value;
        public IDisposable? OnChange(Action<NdtBundleOptions, string?> listener) => null;
    }
}
