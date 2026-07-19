using Microsoft.Extensions.Logging.Abstractions;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using NdtBundleService.Services.PlcHandshake;
using NdtBundleService.Services.PlcHandshake.PlcPoEnd;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// Locks the exact PLC I/O sequence for Mill-1 M40.6/M40.7 handshake (T11 contract).
/// </summary>
public sealed class PlcHandshakeSequencePinTests
{
    [Fact]
    public async Task Idle_poll_sequence_is_locked()
    {
        var (service, s7) = CreateService();
        s7.SetTrigger(false);
        s7.SetAck(false);

        // First poll primes (trigger false).
        await service.ExecuteHandshakePollForTestsAsync(CancellationToken.None);
        s7.ClearOperations();

        // Second poll = steady idle.
        await service.ExecuteHandshakePollForTestsAsync(CancellationToken.None);

        Assert.Equal(
            new[]
            {
                "R M40.6",
                "R M40.7",
                "R DB251@2",
                "R DB251@4",
                "R DB251@6",
                "R DB251@8",
                "R DB251@10",
                "R DB250.DBX2.0"
            },
            s7.Operations);
    }

    [Fact]
    public async Task Rising_edge_to_ack_complete_sequence_is_locked()
    {
        var (service, s7) = CreateService();
        s7.SetTrigger(false);
        s7.SetAck(false);
        s7.SetDb251Int(8, 6);
        s7.SetDb251Int(6, 5);

        // Prime + re-arm.
        await service.ExecuteHandshakePollForTestsAsync(CancellationToken.None);
        await service.ExecuteHandshakePollForTestsAsync(CancellationToken.None);
        s7.ClearOperations();

        // Rising edge → ack TRUE (T11 start).
        s7.SetTrigger(true);
        await service.ExecuteHandshakePollForTestsAsync(CancellationToken.None);

        Assert.Equal(
            new[]
            {
                "R M40.6",
                "R M40.7",
                "R DB251@2",
                "R DB251@4",
                "R DB251@6",
                "R DB251@8",
                "R DB251@10",
                "R DB251@8",
                "R DB251@6",
                "W M40.7=TRUE",
                "R M40.6",
                "R M40.7",
                "R DB250.DBX2.0"
            },
            s7.Operations);

        s7.ClearOperations();

        // PLC clears M40.6 (T11) → ack FALSE completes handshake.
        s7.SetTrigger(false);
        await service.ExecuteHandshakePollForTestsAsync(CancellationToken.None);

        Assert.Equal(
            new[]
            {
                "R M40.6",
                "R M40.7",
                "R DB251@2",
                "R DB251@4",
                "R DB251@6",
                "R DB251@8",
                "R DB251@10",
                "W M40.7=FALSE",
                "R M40.6",
                "R M40.7",
                "R DB250.DBX2.0"
            },
            s7.Operations);
    }

    [Fact]
    public async Task Startup_recovery_with_M40_6_latched_sequence_is_locked()
    {
        var (service, s7) = CreateService(runPoEndWorkflowOnStartupRecovery: false);
        s7.SetTrigger(true);
        s7.SetAck(false);

        await service.ExecuteHandshakePollForTestsAsync(CancellationToken.None);

        Assert.Equal(
            new[]
            {
                "R M40.6",
                "R M40.7",
                "R DB251@2",
                "R DB251@4",
                "R DB251@6",
                "R DB251@8",
                "R DB251@10",
                "W M40.7=TRUE",
                "R M40.6",
                "R M40.7",
                "R M40.6",
                "R DB250.DBX2.0"
            },
            s7.Operations);
    }

    private static (PlcHandshakeService Service, ScriptedS7ConnectionProvider S7) CreateService(
        bool runPoEndWorkflowOnStartupRecovery = false)
    {
        var mill = new MillConfig
        {
            Name = "Mill-1",
            MillNo = 1,
            PoEndSource = "Plc",
            PlcHandshakeEnabled = true,
            IpAddress = "192.168.0.13",
            Rack = 0,
            Slot = 2,
            CpuType = "S7300",
            TriggerByte = 40,
            TriggerBit = 6,
            AckByte = 40,
            AckBit = 7,
            Hooter = new MillHooterOptions { Enabled = false }
        };

        var handshake = new PlcHandshakeOptions
        {
            Enabled = true,
            PollIntervalMs = 500,
            MinimumTriggerFalsePollsBeforeRearm = 2,
            RecoverLatchedTriggerAtStartup = true,
            RunPoEndWorkflowOnStartupRecovery = runPoEndWorkflowOnStartupRecovery,
            ReadLineRunning = true,
            CountsDbNumber = 251,
            OkCountByteOffset = 2,
            NokCountByteOffset = 4,
            NdtCountByteOffset = 6,
            PoIdByteOffset = 8,
            SlitIdByteOffset = 10,
            LineRunningDbNumber = 250,
            LineRunningByteOffset = 2,
            LineRunningBit = 0,
            Mills = { mill }
        };

        var bundle = new NdtBundleOptions
        {
            PlcHandshake = handshake
        };

        var s7 = new ScriptedS7ConnectionProvider();
        var service = new PlcHandshakeService(
            mill,
            handshake,
            bundle,
            new NoOpPoChangeHandler(),
            new PlcPoEndQueue(),
            new PlcHandshakeStatusRegistry(),
            new PlcConnectionHealth(),
            new NoOpActivePo(),
            hooterValues: null,
            new NoOpWipRunningPo(),
            s7,
            NullLogger<PlcHandshakeService>.Instance);

        return (service, s7);
    }

    private sealed class NoOpPoChangeHandler : IPoChangeHandler
    {
        public Task HandlePoChangeAsync(MillConfig mill, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class NoOpActivePo : IActivePoPerMillService
    {
        public Task<IReadOnlyDictionary<int, string>> GetLatestPoByMillAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<int, string>>(new Dictionary<int, string>());

        public IReadOnlyList<string> GetInputSlitReadFolderPaths() => Array.Empty<string>();
    }

    private sealed class NoOpWipRunningPo : IWipBundleRunningPoProvider
    {
        public Task<string?> TryGetRunningPoForMillAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<string?>(null);

        public void NotifyPoEndForMill(int millNo, string endedPo) { }

        public bool IsWaitingForNewWipAfterPoEnd(int millNo) => false;

        public bool ResumeRunningWipForMill(int millNo) => false;

        public bool TrySetRunningPoFromWipFile(int millNo, string newPo, DateTime wipStampUtc, string wipFileName) => false;
    }
}
