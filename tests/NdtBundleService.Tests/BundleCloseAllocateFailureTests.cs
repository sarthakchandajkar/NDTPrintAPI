using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// Failed SQL allocate must not drop pipes. File, PLC slit-end, and PO-end close share the same semantics.
/// </summary>
public sealed class BundleCloseAllocateFailureTests
{
    private static readonly InputSlitRecord Sample = new()
    {
        PoNumber = "1000060163",
        MillNo = 1,
        SlitNo = "1",
        NdtPipes = 11
    };

    [Fact]
    public async Task FileClose_allocate_failure_keeps_pipes_and_retries_same_bundle()
    {
        await AssertAllocateFailThenRetryAsync(async (engine, writer, mill, runtime) =>
        {
            var record = Sample;
            var first = await Record.ExceptionAsync(() =>
                engine.ProcessSlitRecordAsync(record, Write(writer), CancellationToken.None, "Default"));
            Assert.NotNull(first);
            Assert.Equal(11, runtime.GetSizeCounts(record.PoNumber, 1)["Default"]);
            Assert.Equal(0, mill.CurrentSequence);
            Assert.Empty(mill.InsertedBundleNos);

            mill.ThrowOnAllocate = false;
            await engine.ProcessSlitRecordAsync(
                new InputSlitRecord { PoNumber = record.PoNumber, MillNo = 1, NdtPipes = 0 },
                Write(writer),
                CancellationToken.None,
                "Default");

            Assert.Equal(0, runtime.GetSizeCounts(record.PoNumber, 1).GetValueOrDefault("Default"));
            Assert.Equal(1, mill.CurrentSequence);
            Assert.Single(mill.InsertedBundleNos);
        });
    }

    [Fact]
    public async Task PlcClose_allocate_failure_keeps_pipes_and_retries_same_bundle()
    {
        await AssertAllocateFailThenRetryAsync(async (engine, writer, mill, runtime) =>
        {
            runtime.SetSizeCounts(Sample.PoNumber, 1, new Dictionary<string, int> { ["Default"] = 11 });

            var first = await Record.ExceptionAsync(() =>
                engine.CloseBundleFromPlcAsync(
                    Sample.PoNumber, 1, "Default", 11, Write(writer), CancellationToken.None));
            Assert.NotNull(first);
            Assert.Equal(11, runtime.GetSizeCounts(Sample.PoNumber, 1)["Default"]);
            Assert.Equal(0, mill.CurrentSequence);
            Assert.Empty(mill.InsertedBundleNos);

            mill.ThrowOnAllocate = false;
            await engine.CloseBundleFromPlcAsync(
                Sample.PoNumber, 1, "Default", 11, Write(writer), CancellationToken.None);

            Assert.Equal(0, runtime.GetSizeCounts(Sample.PoNumber, 1).GetValueOrDefault("Default"));
            Assert.Equal(1, mill.CurrentSequence);
            Assert.Single(mill.InsertedBundleNos);
        });
    }

    [Fact]
    public async Task PoEnd_allocate_failure_keeps_pipes_and_retries_same_bundle()
    {
        await AssertAllocateFailThenRetryAsync(async (engine, writer, mill, runtime) =>
        {
            runtime.SetSizeCounts(Sample.PoNumber, 1, new Dictionary<string, int> { ["Default"] = 11 });

            var first = await Record.ExceptionAsync(() =>
                engine.HandlePoEndAsync(Sample.PoNumber, 1, Write(writer), CancellationToken.None));
            Assert.NotNull(first);
            Assert.Equal(11, runtime.GetSizeCounts(Sample.PoNumber, 1)["Default"]);
            Assert.Equal(0, mill.CurrentSequence);
            Assert.Empty(mill.InsertedBundleNos);

            mill.ThrowOnAllocate = false;
            await engine.HandlePoEndAsync(Sample.PoNumber, 1, Write(writer), CancellationToken.None);

            Assert.Equal(0, runtime.GetSizeCounts(Sample.PoNumber, 1).GetValueOrDefault("Default"));
            Assert.Equal(1, mill.CurrentSequence);
            Assert.Single(mill.InsertedBundleNos);
        });
    }

    [Fact]
    public async Task FileClose_allocate_failure_then_PoEnd_flushes_same_pipes()
    {
        await AssertAllocateFailThenRetryAsync(async (engine, writer, mill, runtime) =>
        {
            var record = Sample;
            var first = await Record.ExceptionAsync(() =>
                engine.ProcessSlitRecordAsync(record, Write(writer), CancellationToken.None, "Default"));
            Assert.NotNull(first);
            Assert.Equal(11, runtime.GetSizeCounts(record.PoNumber, 1)["Default"]);
            Assert.Empty(mill.InsertedBundleNos);

            mill.ThrowOnAllocate = false;
            await engine.HandlePoEndAsync(record.PoNumber, 1, Write(writer), CancellationToken.None);

            Assert.Equal(0, runtime.GetSizeCounts(record.PoNumber, 1).GetValueOrDefault("Default"));
            Assert.Equal(1, mill.CurrentSequence);
            Assert.Single(mill.InsertedBundleNos);
        });
    }

    [Fact]
    public async Task PlcClose_allocate_failure_then_PoEnd_flushes_same_pipes()
    {
        await AssertAllocateFailThenRetryAsync(async (engine, writer, mill, runtime) =>
        {
            runtime.SetSizeCounts(Sample.PoNumber, 1, new Dictionary<string, int> { ["Default"] = 11 });

            var first = await Record.ExceptionAsync(() =>
                engine.CloseBundleFromPlcAsync(
                    Sample.PoNumber, 1, "Default", 11, Write(writer), CancellationToken.None));
            Assert.NotNull(first);
            Assert.Equal(11, runtime.GetSizeCounts(Sample.PoNumber, 1)["Default"]);
            Assert.Empty(mill.InsertedBundleNos);

            mill.ThrowOnAllocate = false;
            await engine.HandlePoEndAsync(Sample.PoNumber, 1, Write(writer), CancellationToken.None);

            Assert.Equal(0, runtime.GetSizeCounts(Sample.PoNumber, 1).GetValueOrDefault("Default"));
            Assert.Equal(1, mill.CurrentSequence);
            Assert.Single(mill.InsertedBundleNos);
        });
    }

    [Fact]
    public async Task Successful_close_clears_size_so_zero_pipe_slit_does_not_allocate_again()
    {
        var mill = new FakeMillSequence();
        var printer = new CountingTagPrinter();
        var repo = new TrackingRepo();
        var writer = new CsvBundleOutputWriter(
            Options.Create(new NdtBundleOptions { EnableBundleSummaryCsvFiles = false, OutputBundleFolder = Path.GetTempPath() }),
            repo,
            NoOpCsvFillService.Instance,
            NullLogger<CsvBundleOutputWriter>.Instance,
            printer,
            millSequence: mill);

        var runtime = new ProductionLikeRuntime();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        runtime.SetSizeCounts(Sample.PoNumber, 1, new Dictionary<string, int> { ["Default"] = 11 });

        var engine = TestEngineFactory.Create(
            new FormationStub(10),
            new PipeSizeStub(),
            runtime,
            closeTrigger: "File",
            millSequence: mill);

        await engine.ProcessSlitRecordAsync(
            new InputSlitRecord { PoNumber = Sample.PoNumber, MillNo = 1, NdtPipes = 0 },
            Write(writer),
            CancellationToken.None,
            "Default");

        Assert.Equal(0, runtime.GetSizeCounts(Sample.PoNumber, 1).GetValueOrDefault("Default"));
        Assert.Equal(1, mill.CurrentSequence);
        Assert.Single(mill.InsertedBundleNos);

        await engine.ProcessSlitRecordAsync(
            new InputSlitRecord { PoNumber = Sample.PoNumber, MillNo = 1, NdtPipes = 0 },
            Write(writer),
            CancellationToken.None,
            "Default");

        Assert.Equal(1, mill.CurrentSequence);
        Assert.Single(mill.InsertedBundleNos);
    }

    private static Func<InputSlitRecord, int, int, Task> Write(IBundleOutputWriter writer) =>
        async (ctx, _, pcs) =>
        {
            if (pcs <= 0)
                return;
            await writer.WriteBundleAsync(ctx, 0, pcs, CancellationToken.None);
        };

    private static async Task AssertAllocateFailThenRetryAsync(
        Func<NdtBundleEngine, CsvBundleOutputWriter, FakeMillSequence, ProductionLikeRuntime, Task> body)
    {
        var mill = new FakeMillSequence { ThrowOnAllocate = true };
        var printer = new CountingTagPrinter();
        var repo = new TrackingRepo();
        var writer = new CsvBundleOutputWriter(
            Options.Create(new NdtBundleOptions { EnableBundleSummaryCsvFiles = false, OutputBundleFolder = Path.GetTempPath() }),
            repo,
            NoOpCsvFillService.Instance,
            NullLogger<CsvBundleOutputWriter>.Instance,
            printer,
            millSequence: mill);

        var runtime = new ProductionLikeRuntime();
        await runtime.EnsureInitializedAsync(CancellationToken.None);
        var engine = TestEngineFactory.Create(
            new FormationStub(10),
            new PipeSizeStub(),
            runtime,
            millSequence: mill);

        await body(engine, writer, mill, runtime);

        Assert.Equal(1, printer.Calls);
        Assert.Equal(new[] { BundlePrintStatus.Printed }, repo.StatusTransitions);
    }

    private sealed class FakeMillSequence : IMillSequenceService
    {
        public bool ThrowOnAllocate { get; set; }
        public int CurrentSequence { get; private set; }
        public List<string> InsertedBundleNos { get; } = [];
        public bool IsEnabled => true;

        public void SimulateCommitted(string bundleNo)
        {
            CurrentSequence++;
            InsertedBundleNos.Add(bundleNo);
        }

        public Task<(int Sequence, string Formatted)> AllocateAndInsertBundleAsync(
            NdtBundleRecord pending,
            CancellationToken cancellationToken)
        {
            if (ThrowOnAllocate)
                throw new InvalidOperationException("SQL unavailable");

            CurrentSequence++;
            var formatted = NdtBundleSequence.Format(CurrentSequence, pending.MillNo);
            pending.BundleNo = formatted;
            InsertedBundleNos.Add(formatted);
            return Task.FromResult((CurrentSequence, formatted));
        }

        public Task SeedMissingRowsAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<IReadOnlyList<MillSequenceSnapshot>> GetSnapshotsAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<MillSequenceSnapshot>>(Array.Empty<MillSequenceSnapshot>());
        public Task<MillSequenceSnapshot?> GetSnapshotAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<MillSequenceSnapshot?>(new MillSequenceSnapshot(
                millNo,
                CurrentSequence,
                CurrentSequence,
                NdtBundleSequence.Format(CurrentSequence + 1, millNo),
                InsertedBundleNos.Count > 0 ? InsertedBundleNos[^1] : null,
                DateTime.UtcNow,
                "Test",
                null));
        public Task<int> GetLiveMaxSequenceAsync(int millNo, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<int> AllocateNextInTxAsync(
            Microsoft.Data.SqlClient.SqlConnection conn,
            Microsoft.Data.SqlClient.SqlTransaction tx,
            int millNo, string updatedBy, string reason, CancellationToken cancellationToken) =>
            Task.FromResult(0);
        public Task<MillSequenceSetResult> SetCurrentSequenceAsync(
            int millNo, int currentSequence, string reason, string updatedBy, bool forceBelowLiveMax,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();
        public Task<bool> TryRollbackIfHighestInTxAsync(
            Microsoft.Data.SqlClient.SqlConnection conn,
            Microsoft.Data.SqlClient.SqlTransaction tx,
            int millNo, int sourceSequence, string updatedBy, string reason, CancellationToken cancellationToken) =>
            Task.FromResult(false);
        public Task EnsureScanDoesNotExceedTableAsync(int millNo, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }

    private sealed class CountingTagPrinter : INdtTagPrinter
    {
        public int Calls { get; private set; }
        public Task<PrinterSendResult> PrintBundleTagAsync(
            InputSlitRecord record, int batchNumber, int totalNdtPcs, bool isReprint,
            CancellationToken cancellationToken = default)
        {
            Calls++;
            return Task.FromResult(new PrinterSendResult(true));
        }
    }

    private sealed class TrackingRepo : INdtBundleRepository
    {
        public List<string> StatusTransitions { get; } = [];
        public Task RecordBundlePendingPrintAsync(NdtBundleRecord record, CancellationToken cancellationToken)
        {
            StatusTransitions.Add(BundlePrintStatus.Pending);
            return Task.CompletedTask;
        }
        public Task UpdateBundlePrintStatusAsync(string bundleNo, string printStatus, string? printError, CancellationToken cancellationToken)
        {
            StatusTransitions.Add(printStatus);
            return Task.CompletedTask;
        }
        public Task RecordBundleAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<IReadOnlyList<NdtBundleRecord>> GetBundlesAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
        public Task<NdtBundleRecord?> GetByBatchNoAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(null);
        public Task UpdateBundlePipesAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<int> UpdateOutputCsvFilesForBundleAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<IReadOnlyList<(string SlitNo, int NdtPipes)>> GetSlitsForBatchAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string SlitNo, int NdtPipes)>>(Array.Empty<(string, int)>());
        public Task<int> UpdateOutputCsvFilesForSlitAsync(string batchNo, string slitNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task UpdateBundleTotalInDatabaseAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<bool> UpdateBundleSummaryCsvAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.FromResult(false);
        public Task<int> TrySyncBundleTotalFromSlitsAsync(string batchNo, bool forceFromSlits, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<(int RowsRemoved, IReadOnlyList<RemovedSlitRowTraceRef> TraceRefs)> DeletePerSlitOutputRowsForBatchSlitsAsync(
            string batchNo, IReadOnlyList<string> slitNos, CancellationToken cancellationToken) =>
            Task.FromResult((0, (IReadOnlyList<RemovedSlitRowTraceRef>)Array.Empty<RemovedSlitRowTraceRef>()));
        public Task<NdtBundleRecord?> GetLatestPrintedBundleForMillAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(null);
        public Task<bool> HasPrintedBundleForPoAsync(int millNo, string poNumber, CancellationToken cancellationToken) => Task.FromResult(false);
        public Task<int> MarkManualReviewAsync(string poNumber, int millNo, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task TrySetPlcCloseMetadataAsync(int engineBatchSequence, int millNo, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<(string BundleNo, int EngineSequence, int PlcTotal)?> TryGetAwaitingPlcReconBatchAsync(
            string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<(string BundleNo, int EngineSequence, int PlcTotal)?>(null);
        public Task<IReadOnlyList<PlcCsvReconAwaitingBundle>> ListAwaitingPlcReconBatchesAsync(
            string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<PlcCsvReconAwaitingBundle>>(Array.Empty<PlcCsvReconAwaitingBundle>());
        public Task<PlcCsvReconResult?> TryFinalizePlcReconBundleAsync(
            string bundleNo, int slitSum, int reconWindowMinutes, DateTime utcNow, bool force, CancellationToken cancellationToken) =>
            Task.FromResult<PlcCsvReconResult?>(null);
        public Task<IReadOnlyList<PlcCsvReconResult>> TryFinalizeReadyPlcReconBundlesAsync(
            string poNumber, int millNo, int reconWindowMinutes, DateTime utcNow, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<PlcCsvReconResult>>(Array.Empty<PlcCsvReconResult>());
        public Task<PlcCsvReconResult?> TryReconcilePlcClosedBundleAsync(string poNumber, int millNo, int slitSum, CancellationToken cancellationToken) =>
            Task.FromResult<PlcCsvReconResult?>(null);
        public Task<PlcCsvReconResult?> TryForceFinalizeAwaitingReconOnReopenAsync(string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<PlcCsvReconResult?>(null);
        public Task<IReadOnlyList<NdtBundleRecord>> GetStuckPrintsAsync(TimeSpan olderThan, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
    }

    private sealed class ProductionLikeRuntime : INdtBundleRuntimeStateStore
    {
        private readonly Dictionary<string, Dictionary<string, int>> _sizes = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, int> _running = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, InputSlitRecord?> _last = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, int> _inFlight = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, int> _lastAck = new(StringComparer.OrdinalIgnoreCase);
        private static string Key(string po, int mill) => $"{InputSlitCsvParsing.NormalizePo(po)}|{mill}";

        public Task EnsureInitializedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public int GetBatchOffset(string poNumber, int millNo) => 0;
        public int GetRunningTotal(string poNumber, int millNo) => _running.GetValueOrDefault(Key(poNumber, millNo));
        public void ClearRunningTotal(string poNumber, int millNo) => _running[Key(poNumber, millNo)] = 0;
        public void ClearOpenAccumulation(string poNumber, int millNo)
        {
            ClearRunningTotal(poNumber, millNo);
            _sizes[Key(poNumber, millNo)] = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        }
        public DateTime GetLastActivityUtc(string poNumber, int millNo) => DateTime.UtcNow;
        public Task SyncBatchSequencesFromBundlesAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public void ApplySlitContribution(string poNumber, int millNo, int ndtPipes, int threshold, out int totalSoFar)
        {
            var k = Key(poNumber, millNo);
            _running.TryGetValue(k, out var run);
            if (ndtPipes > 0)
                run += ndtPipes;
            _running[k] = run;
            totalSoFar = run;
        }
        public BundleCloseAllocation CloseBundle(string poNumber, int millNo, int closedTotalPcs, int threshold)
        {
            _running[Key(poNumber, millNo)] = 0;
            return new BundleCloseAllocation(0);
        }
        public void AdvanceOnPoEnd(string poNumber, int millNo, int threshold) =>
            _running[Key(poNumber, millNo)] = 0;
        public int GetEngineBatchNo(string poNumber, int millNo) => 0;
        public void SetEngineBatchNo(string poNumber, int millNo, int batchNo) { }
        public Dictionary<string, int> GetSizeCounts(string poNumber, int millNo)
        {
            var k = Key(poNumber, millNo);
            return _sizes.TryGetValue(k, out var d)
                ? new Dictionary<string, int>(d, StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        }
        public void SetSizeCounts(string poNumber, int millNo, IReadOnlyDictionary<string, int> counts) =>
            _sizes[Key(poNumber, millNo)] = new Dictionary<string, int>(counts, StringComparer.OrdinalIgnoreCase);
        public InputSlitRecord? GetLastRecord(string poNumber, int millNo) => _last.GetValueOrDefault(Key(poNumber, millNo));
        public void SetLastRecord(string poNumber, int millNo, InputSlitRecord? record) =>
            _last[Key(poNumber, millNo)] = record;
        public Task SaveAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        public void MarkCloseInFlight(string poNumber, int millNo, int pcs) =>
            _inFlight[Key(poNumber, millNo)] = Math.Max(0, pcs);
        public void ClearCloseInFlight(string poNumber, int millNo) =>
            _inFlight[Key(poNumber, millNo)] = 0;
        public bool HasCloseInFlight(string poNumber, int millNo) =>
            _inFlight.GetValueOrDefault(Key(poNumber, millNo)) > 0;
        public int GetLastAcknowledgedMillSequence(string poNumber, int millNo) =>
            _lastAck.GetValueOrDefault(Key(poNumber, millNo));
        public void SetLastAcknowledgedMillSequence(string poNumber, int millNo, int sequence) =>
            _lastAck[Key(poNumber, millNo)] = Math.Max(0, sequence);
    }

    private sealed class FormationStub : IFormationChartProvider
    {
        private readonly int _threshold;
        public FormationStub(int threshold) => _threshold = threshold;
        public Task<IReadOnlyDictionary<string, FormationChartEntry>> GetFormationChartAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<string, FormationChartEntry>>(
                new Dictionary<string, FormationChartEntry>
                {
                    ["Default"] = new FormationChartEntry { PipeSize = "Default", RequiredNdtPcs = _threshold }
                });
        public void InvalidateCache() { }
    }

    private sealed class PipeSizeStub : IPipeSizeProvider
    {
        public Task<IReadOnlyDictionary<string, string>> GetPipeSizeByPoAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<string, string>>(new Dictionary<string, string>());
        public IReadOnlyDictionary<string, string>? TryGetCachedPipeSizes() => null;
        public Task<string?> TryGetPipeSizeForPoAsync(string poNumber, CancellationToken cancellationToken) =>
            Task.FromResult<string?>(null);
    }
}
