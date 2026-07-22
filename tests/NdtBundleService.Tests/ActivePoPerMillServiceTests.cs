using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class ActivePoPerMillServiceTests : IDisposable
{
    private readonly string _tempDir;

    public ActivePoPerMillServiceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ActivePoPerMillTests_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            // Best-effort cleanup.
        }
    }

    [Fact]
    public async Task GetLatestPoByMillAsync_UsesLatestRowPerMillFromNewestInputSlitFile()
    {
        var older = Path.Combine(_tempDir, "older.csv");
        await File.WriteAllTextAsync(older,
            "PO Number,Mill No,NDT Pipes\n" +
            "1000000001,1,5\n");

        await Task.Delay(50);

        var newer = Path.Combine(_tempDir, "newer.csv");
        await File.WriteAllTextAsync(newer,
            "PO Number,Mill No,NDT Pipes\n" +
            "1000000002,1,6\n" +
            "1000000003,2,7\n");

        var service = CreateService(_tempDir, wipPoByMill: new Dictionary<int, string> { [1] = "1999999999" });
        var result = await service.GetLatestPoByMillAsync(CancellationToken.None);

        Assert.Equal("1000000002", result[1]);
        Assert.Equal("1000000003", result[2]);
    }

    [Fact]
    public async Task GetLatestPoByMillAsync_DoesNotLetWipBundleOverwriteSlitPo()
    {
        var path = Path.Combine(_tempDir, "slit.csv");
        await File.WriteAllTextAsync(path,
            "PO Number,Mill No,NDT Pipes\n" +
            "1000000100,3,10\n");

        var service = CreateService(_tempDir, wipPoByMill: new Dictionary<int, string> { [3] = "1000000999" });
        var result = await service.GetLatestPoByMillAsync(CancellationToken.None);

        Assert.Equal("1000000100", result[3]);
    }

    [Fact]
    public async Task GetLatestPoByMillAsync_UsesWipOnlyWhenSlitPoMissing()
    {
        var service = CreateService(_tempDir, wipPoByMill: new Dictionary<int, string> { [4] = "1000000444" });
        var result = await service.GetLatestPoByMillAsync(CancellationToken.None);

        Assert.Equal("1000000444", result[4]);
    }

    private static ActivePoPerMillService CreateService(
        string inputSlitFolder,
        IReadOnlyDictionary<int, string>? wipPoByMill = null)
    {
        var options = Options.Create(new NdtBundleOptions
        {
            InputSlitFolder = inputSlitFolder,
            PreferInputSlitFilesForRunningPo = true,
            UseSqlServerForBundles = false
        });

        return new ActivePoPerMillService(
            options,
            new StubWipRunningPoProvider(wipPoByMill ?? new Dictionary<int, string>()),
            NullLogger<ActivePoPerMillService>.Instance);
    }

    private sealed class StubWipRunningPoProvider(IReadOnlyDictionary<int, string> poByMill) : IWipBundleRunningPoProvider
    {
        public Task<string?> TryGetRunningPoForMillAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult(poByMill.TryGetValue(millNo, out var po) ? po : null);

        public void NotifyPoEndForMill(int millNo, string endedPo) { }

        public bool IsWaitingForNewWipAfterPoEnd(int millNo) => false;

        public bool TryGetPoEndWaitContext(int millNo, out bool waitingForNewWip, out string? endedPo)
        {
            waitingForNewWip = false;
            endedPo = null;
            return true;
        }

        public bool ResumeRunningWipForMill(int millNo) => false;

        public bool TrySetRunningPoFromWipFile(int millNo, string newPo, DateTime wipStampUtc, string wipFileName) => false;
    }
}
