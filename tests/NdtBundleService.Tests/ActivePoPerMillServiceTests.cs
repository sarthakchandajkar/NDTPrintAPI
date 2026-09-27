using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using NdtBundleService.Services.MillInstanceStatus;
using NdtBundleService.Services.PlcHandshake;
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

    [Fact]
    public async Task GetLatestPoByMillAsync_PrefersInboxOverAcceptedSameBasename()
    {
        var inbox = Path.Combine(_tempDir, "inbox");
        var accepted = Path.Combine(_tempDir, "accepted");
        Directory.CreateDirectory(inbox);
        Directory.CreateDirectory(accepted);

        var name = "same_name.csv";
        await File.WriteAllTextAsync(Path.Combine(accepted, name),
            "PO Number,Mill No,NDT Pipes\n" +
            "1000000001,1,5\n");
        // Make Accepted look newer so write-time merge would wrongly prefer it.
        File.SetLastWriteTimeUtc(Path.Combine(accepted, name), DateTime.UtcNow.AddMinutes(5));

        await File.WriteAllTextAsync(Path.Combine(inbox, name),
            "PO Number,Mill No,NDT Pipes\n" +
            "1000000999,1,5\n");
        File.SetLastWriteTimeUtc(Path.Combine(inbox, name), DateTime.UtcNow.AddMinutes(-5));

        var service = CreateService(inbox, acceptedFolder: accepted);
        var result = await service.GetLatestPoByMillAsync(CancellationToken.None);

        Assert.Equal("1000000999", result[1]);
    }

    [Fact]
    public async Task GetLatestPoByMillAsync_MillPublishedRunningPoOverridesSlit()
    {
        var path = Path.Combine(_tempDir, "slit.csv");
        await File.WriteAllTextAsync(path,
            "PO Number,Mill No,NDT Pipes\n" +
            "1000000100,1,10\n");

        var store = new InMemoryMillInstanceStatusStore();
        store.Upsert(
            new PlcHandshakeMillStatus
            {
                MillNo = 1,
                MillName = "Mill-1",
                RunningPoNumber = "1000000888",
                WaitingForNewWip = false,
                RunningPoSource = "Wip",
                RunningPoUpdatedAtUtc = DateTimeOffset.UtcNow,
                LastUpdateUtc = DateTimeOffset.UtcNow
            },
            Guid.NewGuid(),
            "vm",
            "Mill-1");

        var service = CreateService(
            _tempDir,
            useSql: true,
            millStatus: store);
        var result = await service.GetLatestPoByMillAsync(CancellationToken.None);

        Assert.Equal("1000000888", result[1]);
    }

    [Fact]
    public async Task GetLatestPoByMillAsync_MillPublishedWaitingClearsSlitPo()
    {
        var path = Path.Combine(_tempDir, "slit.csv");
        await File.WriteAllTextAsync(path,
            "PO Number,Mill No,NDT Pipes\n" +
            "1000000100,1,10\n");

        var store = new InMemoryMillInstanceStatusStore();
        store.Upsert(
            new PlcHandshakeMillStatus
            {
                MillNo = 1,
                MillName = "Mill-1",
                WaitingForNewWip = true,
                RunningPoSource = "Waiting",
                RunningPoUpdatedAtUtc = DateTimeOffset.UtcNow,
                LastUpdateUtc = DateTimeOffset.UtcNow
            },
            Guid.NewGuid(),
            "vm",
            "Mill-1");

        var service = CreateService(
            _tempDir,
            useSql: true,
            millStatus: store);
        var result = await service.GetLatestPoByMillAsync(CancellationToken.None);

        Assert.False(result.ContainsKey(1));
    }

    private static ActivePoPerMillService CreateService(
        string inputSlitFolder,
        IReadOnlyDictionary<int, string>? wipPoByMill = null,
        string? acceptedFolder = null,
        bool useSql = false,
        IMillInstanceStatusStore? millStatus = null,
        bool waitingMill1 = false)
    {
        var options = Options.Create(new NdtBundleOptions
        {
            InputSlitFolder = inputSlitFolder,
            InputSlitAcceptedFolder = acceptedFolder ?? string.Empty,
            PreferInputSlitFilesForRunningPo = true,
            UseSqlServerForBundles = useSql,
            ConnectionString = useSql ? "Server=.;Database=unused;Trusted_Connection=True;" : string.Empty
        });

        return new ActivePoPerMillService(
            options,
            new StubWipRunningPoProvider(wipPoByMill ?? new Dictionary<int, string>(), waitingMill1),
            millStatus ?? new InMemoryMillInstanceStatusStore(),
            NullLogger<ActivePoPerMillService>.Instance);
    }

    private sealed class StubWipRunningPoProvider(
        IReadOnlyDictionary<int, string> poByMill,
        bool waitingMill1 = false) : IWipBundleRunningPoProvider
    {
        public Task<string?> TryGetRunningPoForMillAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult(poByMill.TryGetValue(millNo, out var po) ? po : null);

        public void NotifyPoEndForMill(int millNo, string endedPo) { }

        public bool IsWaitingForNewWipAfterPoEnd(int millNo) => waitingMill1 && millNo == 1;

        public bool TryGetPoEndWaitContext(int millNo, out bool waitingForNewWip, out string? endedPo)
        {
            waitingForNewWip = IsWaitingForNewWipAfterPoEnd(millNo);
            endedPo = null;
            return true;
        }

        public bool ResumeRunningWipForMill(int millNo) => false;

        public bool TrySetRunningPoFromWipFile(int millNo, string newPo, DateTime wipStampUtc, string wipFileName) => false;
    }
}
