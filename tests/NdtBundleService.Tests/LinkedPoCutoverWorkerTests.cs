using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Services;
using NdtBundleService.Services.LinkedPo;
using NdtBundleService.Services.PlcHandshake;
using NdtBundleService.Services.PoLifecycle;
using Xunit;

namespace NdtBundleService.Tests;

public sealed class LinkedPoCutoverWorkerTests
{
    [Fact]
    public async Task ProcessAsync_skips_when_no_evidence()
    {
        var opts = Options.Create(new NdtBundleOptions
        {
            EnableLinkedPoCutover = true,
            UseSqlServerForBundles = false,
            PlcHandshake = new PlcHandshakeOptions
            {
                Mills = [new MillConfig { MillNo = 1, PoEndSource = "Plc" }]
            }
        });
        var workflow = new CapturingPoEndWorkflow();
        var wip = new CapturingWipProvider();
        var worker = new LinkedPoCutoverWorker(
            new LinkedPoCutoverQueue(),
            new LinkedPoCutoverEvidence(opts, NullLogger<LinkedPoCutoverEvidence>.Instance),
            workflow,
            wip,
            new TestPoLifecycleStub(),
            TestMillOwnership.Monolith(),
            opts,
            NullLogger<LinkedPoCutoverWorker>.Instance);

        await worker.ProcessAsync(
            new LinkedPoCutoverRequest
            {
                MillNo = 1,
                EndedPo = "1000062750",
                NewPo = "1000062751",
                WipFileName = "WIP_01_1000062751_x.csv",
                WipStampUtc = DateTime.UtcNow,
                Trigger = "WipPoChange"
            },
            CancellationToken.None);

        Assert.Empty(workflow.Calls);
    }

    [Fact]
    public async Task ProcessAsync_runs_po_end_when_91_hint_present()
    {
        var opts = Options.Create(new NdtBundleOptions
        {
            EnableLinkedPoCutover = true,
            UseSqlServerForBundles = false,
            PlcHandshake = new PlcHandshakeOptions
            {
                Mills = [new MillConfig { MillNo = 1, PoEndSource = "Plc" }]
            }
        });
        var workflow = new CapturingPoEndWorkflow();
        var wip = new CapturingWipProvider();
        var worker = new LinkedPoCutoverWorker(
            new LinkedPoCutoverQueue(),
            new LinkedPoCutoverEvidence(opts, NullLogger<LinkedPoCutoverEvidence>.Instance),
            workflow,
            wip,
            new TestPoLifecycleStub(),
            TestMillOwnership.Monolith(),
            opts,
            NullLogger<LinkedPoCutoverWorker>.Instance);

        var stamp = DateTime.UtcNow;
        await worker.ProcessAsync(
            new LinkedPoCutoverRequest
            {
                MillNo = 1,
                EndedPo = "1000062750",
                NewPo = "1000062751",
                SlitNoHint = "2607388_01_91",
                WipFileName = "WIP_01_1000062751_x.csv",
                WipStampUtc = stamp,
                Trigger = "InputSlit"
            },
            CancellationToken.None);

        Assert.Single(workflow.Calls);
        Assert.Equal("1000062750", workflow.Calls[0].Po);
        Assert.Equal(1, workflow.Calls[0].Mill);
        Assert.True(wip.SetRunningCalled);
    }

    private sealed class CapturingPoEndWorkflow : IPoEndWorkflowService
    {
        public List<(string Po, int Mill)> Calls { get; } = new();

        public Task<PoEndWorkflowResult> ExecuteAsync(
            string poNumber,
            int millNo,
            bool advancePoPlanFile,
            CancellationToken cancellationToken,
            Guid? correlationId = null)
        {
            Calls.Add((InputSlitCsvParsing.NormalizePo(poNumber), millNo));
            return Task.FromResult(new PoEndWorkflowResult { PoNumber = poNumber, MillNo = millNo });
        }

        public Task<PoEndWorkflowResult> ExecuteAsync(
            string poNumber,
            int millNo,
            bool advancePoPlanFile,
            CancellationToken cancellationToken,
            Guid? correlationId,
            int? plcNdtCountFinal) =>
            ExecuteAsync(poNumber, millNo, advancePoPlanFile, cancellationToken, correlationId);

        public Task<(int BundlesClosed, int TotalPcs)> CompleteSlitEndDeferredFlushAsync(
            string poNumber,
            int millNo,
            int? plcNdtCountFinal,
            Guid? correlationId,
            CancellationToken cancellationToken) =>
            Task.FromResult((0, 0));
    }

    private sealed class CapturingWipProvider : IWipBundleRunningPoProvider
    {
        public bool SetRunningCalled { get; private set; }

        public Task<string?> TryGetRunningPoForMillAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<string?>(null);

        public void NotifyPoEndForMill(int millNo, string endedPo) { }

        public bool IsWaitingForNewWipAfterPoEnd(int millNo) => false;

        public bool TryGetPoEndWaitContext(int millNo, out bool waitingForNewWip, out string? endedPo)
        {
            waitingForNewWip = false;
            endedPo = null;
            return false;
        }

        public bool ResumeRunningWipForMill(int millNo) => false;

        public bool TrySetRunningPoFromWipFile(int millNo, string newPo, DateTime wipStampUtc, string wipFileName)
        {
            SetRunningCalled = true;
            return true;
        }
    }
}
