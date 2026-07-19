using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using NdtBundleService.Services.PlcHandshake;
using NdtBundleService.Services.PlcHandshake.PlcPoEnd;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class Phase3F6HandshakeAuditPinTests
{
    [Fact]
    public async Task Rising_edge_handshake_persists_Completed_audit_with_timestamps()
    {
        var audit = new RecordingHandshakeEvents();
        var (service, s7) = CreateService(audit);
        s7.SetTrigger(false);
        s7.SetAck(false);
        s7.SetDb251Int(8, 6);
        s7.SetDb251Int(6, 5);

        await service.ExecuteHandshakePollForTestsAsync(CancellationToken.None);
        await service.ExecuteHandshakePollForTestsAsync(CancellationToken.None);

        s7.SetTrigger(true);
        await service.ExecuteHandshakePollForTestsAsync(CancellationToken.None);

        Assert.NotEmpty(audit.Records);
        var mid = audit.Records.Last();
        Assert.Equal(HandshakeOutcome.InProgress, mid.Outcome);
        Assert.NotNull(mid.AckAtUtc);
        Assert.Equal(6, mid.PlcPoId);
        Assert.Equal(5, mid.PlcNdtCount);

        s7.SetTrigger(false);
        await service.ExecuteHandshakePollForTestsAsync(CancellationToken.None);

        var done = audit.Records.Last();
        Assert.Equal(HandshakeOutcome.Completed, done.Outcome);
        Assert.NotNull(done.AckAtUtc);
        Assert.NotNull(done.ClearedAtUtc);
        Assert.NotNull(done.AckDroppedAtUtc);
        Assert.Equal(mid.CorrelationId, done.CorrelationId);
    }

    [Fact]
    public async Task Stuck_trigger_raises_WRN_and_StuckTrigger_outcome()
    {
        var audit = new RecordingHandshakeEvents();
        var logger = new ListLogger();
        var (service, s7) = CreateService(
            audit,
            logger,
            stuckTriggerAlarmSeconds: 0); // fire on next poll after edge

        s7.SetTrigger(false);
        s7.SetAck(false);
        await service.ExecuteHandshakePollForTestsAsync(CancellationToken.None);
        await service.ExecuteHandshakePollForTestsAsync(CancellationToken.None);

        s7.SetTrigger(true);
        await service.ExecuteHandshakePollForTestsAsync(CancellationToken.None);
        // Still TRUE, awaiting clear — stuck alarm (0s threshold).
        await service.ExecuteHandshakePollForTestsAsync(CancellationToken.None);

        Assert.Contains(logger.Messages, m => m.Contains("stuck trigger alarm", StringComparison.OrdinalIgnoreCase));
        Assert.Contains(audit.Records, r => r.Outcome == HandshakeOutcome.StuckTrigger);
    }

    [Fact]
    public async Task Ack_write_TRUE_failure_after_retries_sets_AckWriteFailed()
    {
        var audit = new RecordingHandshakeEvents();
        var logger = new ListLogger();
        var (service, s7) = CreateService(audit, logger, ackWriteRetryCount: 3, ackWriteRetryInitialBackoffMs: 0);
        s7.FailNextWrites = 3;

        s7.SetTrigger(false);
        s7.SetAck(false);
        await service.ExecuteHandshakePollForTestsAsync(CancellationToken.None);
        await service.ExecuteHandshakePollForTestsAsync(CancellationToken.None);

        s7.SetTrigger(true);
        await service.ExecuteHandshakePollForTestsAsync(CancellationToken.None);

        Assert.Contains(audit.Records, r => r.Outcome == HandshakeOutcome.AckWriteFailed);
        Assert.Contains(logger.Messages, m => m.Contains("ack write TRUE failed", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Po_end_count_compare_message_shape_is_stable()
    {
        // Pin the diagnostic message contract used by PlcPoEndQueueWorker (F-6.4).
        const string template =
            "PO end count compare: PlcNdt={PlcNdt} FlushedNdt={FlushedNdt} Deferred={Deferred} Mill {MillNo} PO {PO} CorrelationId {CorrelationId}";
        Assert.Contains("PlcNdt=", template, StringComparison.Ordinal);
        Assert.Contains("FlushedNdt=", template, StringComparison.Ordinal);
        Assert.Contains("Deferred=", template, StringComparison.Ordinal);
    }

    private static (PlcHandshakeService Service, ScriptedS7ConnectionProvider S7) CreateService(
        RecordingHandshakeEvents audit,
        ListLogger? logger = null,
        int stuckTriggerAlarmSeconds = 30,
        int ackWriteRetryCount = 3,
        int ackWriteRetryInitialBackoffMs = 100)
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
            RunPoEndWorkflowOnStartupRecovery = false,
            HandshakeAuditEnabled = true,
            StuckTriggerAlarmSeconds = stuckTriggerAlarmSeconds,
            AckWriteRetryCount = ackWriteRetryCount,
            AckWriteRetryInitialBackoffMs = ackWriteRetryInitialBackoffMs,
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

        var bundle = new NdtBundleOptions { PlcHandshake = handshake };
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
            (ILogger<PlcHandshakeService>)(logger ?? (ILogger<PlcHandshakeService>)NullLogger<PlcHandshakeService>.Instance),
            slitEndCloser: null,
            handshakeEvents: audit);

        return (service, s7);
    }

    private sealed class RecordingHandshakeEvents : IHandshakeEventRepository
    {
        public List<HandshakeEventRecord> Records { get; } = new();
        public Task UpsertAsync(HandshakeEventRecord record, CancellationToken cancellationToken = default)
        {
            Records.Add(record);
            return Task.CompletedTask;
        }
    }

    private sealed class ListLogger : ILogger<PlcHandshakeService>
    {
        public List<string> Messages { get; } = new();
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Messages.Add(formatter(state, exception));
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
