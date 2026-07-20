using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;
using NdtBundleService.Models;
using NdtBundleService.Services;
using NdtBundleService.Services.PlcHandshake;
using NdtBundleService.Services.PlcHandshake.PlcPoEnd;
using NdtBundleService.Services.PoLifecycle;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>P0 2026-07-19: mill-wide allocator, re-arm + PO-end idempotency, stamp corrector, seen-table.</summary>
public sealed class MillWideAllocatorAndPoEndIdempotencyTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _statePath;

    public MillWideAllocatorAndPoEndIdempotencyTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "MillWideAlloc_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        _statePath = Path.Combine(_tempDir, "NdtBundleRuntimeState.json");
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
            // best-effort
        }
    }

    [Fact]
    public async Task Overlapping_POs_across_drain_yield_unique_mill_wide_sequences_by_close_order()
    {
        var store = CreateStore();
        await store.EnsureInitializedAsync(CancellationToken.None);

        // PO-A opens provisional 1
        store.ApplySlitContribution("1000060364", 1, 5, threshold: 15, out var aOpen, out _);
        Assert.Equal(1, aOpen);
        store.SetSizeCounts("1000060364", 1, new Dictionary<string, int> { ["Default"] = 5 });

        // PO-B opens same provisional preview while mill floor still 0; close allocates final 1
        store.ApplySlitContribution("1000060363", 1, 15, threshold: 15, out var bOpen, out _);
        Assert.Equal(1, bOpen);
        var b1 = store.CloseBundle("1000060363", 1, 15, 15);
        Assert.Equal(1, b1.FinalSequence);
        Assert.Equal(1, b1.ProvisionalSequence);

        var b2 = store.CloseBundle("1000060363", 1, 15, 15);
        Assert.Equal(2, b2.FinalSequence);
        var b3 = store.CloseBundle("1000060363", 1, 15, 15);
        Assert.Equal(3, b3.FinalSequence);

        // PO-A drain flush — next mill-wide number (4), not reuse 1/2/3
        var aFlush = store.CloseBundle("1000060364", 1, 5, 15);
        Assert.Equal(4, aFlush.FinalSequence);
        Assert.Equal(1, aFlush.ProvisionalSequence);
        Assert.True(aFlush.NeedsStampCorrection);

        store.ApplySlitContribution("1000060363", 1, 1, threshold: 15, out var bNextOpen, out _);
        Assert.Equal(5, bNextOpen);
        var b5 = store.CloseBundle("1000060363", 1, 15, 15);
        Assert.Equal(5, b5.FinalSequence);
    }

    [Fact]
    public async Task ApplySlitContribution_does_not_burn_sequence_on_threshold_without_close()
    {
        var store = CreateStore();
        await store.EnsureInitializedAsync(CancellationToken.None);

        store.ApplySlitContribution("PO-A", 1, 15, threshold: 15, out var first, out _);
        Assert.Equal(1, first);
        store.ApplySlitContribution("PO-A", 1, 15, threshold: 15, out var second, out _);
        Assert.Equal(1, second); // still provisional 1 — no BatchOffset burn

        var close = store.CloseBundle("PO-A", 1, 20, 15);
        Assert.Equal(1, close.FinalSequence);
        Assert.Equal(1, close.ProvisionalSequence);
        Assert.False(close.NeedsStampCorrection);
    }

    [Fact]
    public void RewriteBatchColumn_replaces_trailing_field()
    {
        var csv = "a,b,c\n1,2,1226100008\n3,4,1226100008\n";
        var rewritten = BundleProvisionalStampCorrector.RewriteBatchColumn(csv, "1226100008", "1226100007");
        Assert.Contains("1226100007", rewritten);
        Assert.DoesNotContain("1226100008", rewritten);
    }

    [Fact]
    public async Task StampCorrector_rewrites_csv_when_sql_returns_no_file_refs()
    {
        var csvPath = Path.Combine(_tempDir, "slit_out.csv");
        await File.WriteAllTextAsync(csvPath, "a,b,c\n1,2,1226100008\n");

        var opts = new TestOptionsMonitor<NdtBundleOptions>(new NdtBundleOptions
        {
            OutputBundleFolder = _tempDir
        });
        var corrector = new BundleProvisionalStampCorrector(
            new EmptyTraceability(),
            opts,
            NullLogger<BundleProvisionalStampCorrector>.Instance);

        await corrector.CorrectAsync("PO-A", 1, provisionalSequence: 8, finalSequence: 7, CancellationToken.None);

        var text = await File.ReadAllTextAsync(csvPath);
        Assert.Contains("1226100007", text);
        Assert.DoesNotContain("1226100008", text);
    }

    [Fact]
    public void Rising_edge_after_ArmAfterTriggerClear_detects_one_false_poll_rearm()
    {
        var tracker = new PlcHandshakeEdgeTracker(new PlcHandshakeOptions
        {
            MinimumTriggerFalsePollsBeforeRearm = 1
        });

        tracker.ArmAfterTriggerClear(isStartup: false);
        Assert.True(tracker.IsRearmedForNextPoChange());
        Assert.True(tracker.TryDetectPoChangeRisingEdge(trigger: true));
    }

    [Fact]
    public void PlcPoEndWorkflowGate_ack_only_when_draining_closed_or_unresolved()
    {
        Assert.Equal(
            PlcPoEndWorkflowGate.Decision.AckOnlySkip,
            PlcPoEndWorkflowGate.Decide(poResolved: false, PoLifecyclePhase.Running));
        Assert.Equal(
            PlcPoEndWorkflowGate.Decision.AckOnlySkip,
            PlcPoEndWorkflowGate.Decide(poResolved: true, PoLifecyclePhase.Draining));
        Assert.Equal(
            PlcPoEndWorkflowGate.Decision.AckOnlySkip,
            PlcPoEndWorkflowGate.Decide(poResolved: true, PoLifecyclePhase.Closed));
        Assert.Equal(
            PlcPoEndWorkflowGate.Decision.RunWorkflow,
            PlcPoEndWorkflowGate.Decide(poResolved: true, PoLifecyclePhase.Running));
    }

    [Fact]
    public void Persistent_relatch_100_cycles_runs_workflow_once_acks_all_bounded_logs()
    {
        var lifecycle = new PoLifecycleService(new TestOptionsMonitor<NdtBundleOptions>(CreatePlcOptions()));
        var limiter = new PlcPoEndAckOnlyRateLimiter(TimeSpan.FromHours(1));
        var workflows = 0;
        var acks = 0;
        var warns = 0;
        const string po = "1000060364";
        const int mill = 1;

        for (var i = 0; i < 100; i++)
        {
            // Each re-latch cycle: handshake acks the bit
            acks++;

            var phase = lifecycle.GetPhase(mill, po);
            var decision = PlcPoEndWorkflowGate.Decide(poResolved: true, phase);
            if (decision == PlcPoEndWorkflowGate.Decision.RunWorkflow)
            {
                workflows++;
                Assert.True(lifecycle.TryMarkDraining(mill, po, DateTime.UtcNow));
            }
            else if (limiter.ShouldLog($"{mill}|{po}|{phase}", DateTime.UtcNow))
            {
                warns++;
            }
        }

        Assert.Equal(1, workflows);
        Assert.Equal(100, acks);
        Assert.Equal(1, warns); // rate-limited after first ack-only WRN
    }

    [Fact]
    public void AckOnlyRateLimiter_bounds_repeated_keys()
    {
        var limiter = new PlcPoEndAckOnlyRateLimiter(TimeSpan.FromMinutes(5));
        var now = DateTime.UtcNow;
        Assert.True(limiter.ShouldLog("1|PO|Draining", now));
        Assert.False(limiter.ShouldLog("1|PO|Draining", now.AddSeconds(10)));
        Assert.True(limiter.ShouldLog("1|PO|Draining", now.AddMinutes(6)));
    }

    [Fact]
    public void Input_Slit_File_Seen_script_is_dedicated_table_not_sentinel_rows()
    {
        var sqlPath = FindRepoFile(Path.Combine("docs", "Input_Slit_File_Seen_AddTable.sql"));
        Assert.True(File.Exists(sqlPath), $"Missing {sqlPath}");
        var sql = File.ReadAllText(sqlPath);
        Assert.Contains("CREATE TABLE dbo.Input_Slit_File_Seen", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("UX_Input_Slit_File_Seen_File_Write", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("INSERT INTO dbo.Input_Slit_Row", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Do NOT use sentinel rows in Input_Slit_Row", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Seen_table_marks_terminal_no_mill_files_so_reconcile_skips()
    {
        var seen = new FakeSeenTraceability();
        var path = Path.Combine(_tempDir, "other_mill.csv");
        var lw = new DateTime(2026, 7, 19, 12, 0, 0, DateTimeKind.Utc);

        Assert.False(await seen.IsInputSlitFileSeenAsync(path, lw, CancellationToken.None));
        await seen.MarkInputSlitFileSeenAsync(path, lw, "NoConfiguredMillRows", CancellationToken.None);
        Assert.True(await seen.IsInputSlitFileSeenAsync(path, lw, CancellationToken.None));
        // Same path + write time stays terminal; a newer write would be a different key
        Assert.False(await seen.IsInputSlitFileSeenAsync(path, lw.AddSeconds(1), CancellationToken.None));
    }

    private static string FindRepoFile(string relativePath)
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, relativePath);
            if (File.Exists(candidate))
                return candidate;
            dir = dir.Parent;
        }

        return Path.GetFullPath(relativePath);
    }

    private static NdtBundleOptions CreatePlcOptions() =>
        new()
        {
            PlcHandshake = new PlcHandshakeOptions
            {
                Enabled = true,
                Mills = [new MillConfig { MillNo = 1, Name = "Mill-1", PoEndSource = "Plc" }]
            }
        };

    private NdtBundleRuntimeStateStore CreateStore()
    {
        var opts = new TestOptionsMonitor<NdtBundleOptions>(new NdtBundleOptions
        {
            EnableNdtBundleRuntimeStatePersistence = true,
            NdtBundleRuntimeStateFile = _statePath,
            OutputBundleFolder = _tempDir,
            SyncRuntimeStateFromPrintedBundlesOnly = false
        });
        return new NdtBundleRuntimeStateStore(
            opts,
            new EmptyBundleRepo(),
            new EmptyActivePo(),
            NullLogger<NdtBundleRuntimeStateStore>.Instance);
    }

    private sealed class TestOptionsMonitor<T> : IOptionsMonitor<T> where T : class
    {
        public TestOptionsMonitor(T value) => CurrentValue = value;
        public T CurrentValue { get; }
        public T Get(string? name) => CurrentValue;
        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }

    private sealed class EmptyTraceability : ITraceabilityRepository
    {
        public Task RecordInputSlitRowsAsync(
            string sourceFile,
            IReadOnlyList<(InputSlitRecord Record, int SourceRowNumber)> rows,
            CancellationToken cancellationToken,
            DateTime? sourceLastWriteTimeUtc = null) => Task.CompletedTask;

        public Task<bool> IsInputSlitFileVersionImportedAsync(
            string sourceFileFullPath,
            DateTime fileLastWriteTimeUtc,
            CancellationToken cancellationToken) => Task.FromResult(false);

        public Task<bool> IsInputSlitFileSeenAsync(
            string sourceFileFullPath,
            DateTime fileLastWriteTimeUtc,
            CancellationToken cancellationToken) => Task.FromResult(false);

        public Task MarkInputSlitFileSeenAsync(
            string sourceFileFullPath,
            DateTime fileLastWriteTimeUtc,
            string reason,
            CancellationToken cancellationToken) => Task.CompletedTask;

        public Task<IReadOnlyList<string>> UpdateOutputSlitBatchNoAsync(
            string poNumber,
            int millNo,
            string oldBatchNo,
            string newBatchNo,
            CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());

        public Task RecordOutputSlitRowsAsync(
            string sourceFile,
            IReadOnlyList<(InputSlitRecord Record, string NdtBatchNo, int SourceRowNumber)> rows,
            CancellationToken cancellationToken) => Task.CompletedTask;

        public Task RecordManualStationRunAsync(
            string poNumber,
            string ndtBatchNo,
            int ndtPcs,
            int okPcs,
            int rejectPcs,
            string workStation,
            DateTime start,
            DateTime end,
            string? hydrotestingType,
            string sourceFile,
            CancellationToken cancellationToken) => Task.CompletedTask;

        public Task RecordNdtProcessConsolidatedAsync(
            string poNumber,
            string ndtBatchNo,
            int ndtPcs,
            int okPcs,
            int visualReject,
            int hydrotestReject,
            int revisualReject,
            DateTime bundleStart,
            DateTime bundleEnd,
            string outputFilePath,
            CancellationToken cancellationToken) => Task.CompletedTask;

        public Task RecordBundleLabelAsync(
            string poNumber,
            int millNo,
            string? specification,
            string? type,
            string? pipeSize,
            string? length,
            CancellationToken cancellationToken) => Task.CompletedTask;

        public Task RecordUploadBundleRowsAsync(
            string generatedFile,
            IReadOnlyList<UploadBundleRow> rows,
            CancellationToken cancellationToken) => Task.CompletedTask;

        public Task DeleteOutputSlitRowsForRemovedOutputLinesAsync(
            string ndtBatchNo,
            IReadOnlyList<RemovedSlitRowTraceRef> refs,
            CancellationToken cancellationToken) => Task.CompletedTask;

        public Task UpsertManualStationRunAsync(
            string poNumber,
            string ndtBatchNo,
            int ndtPcs,
            int okPcs,
            int rejectPcs,
            string workStation,
            DateTime start,
            DateTime end,
            string? hydrotestingType,
            string sourceFile,
            CancellationToken cancellationToken) => Task.CompletedTask;

        public Task<int> UpdateOutputSlitRowNdtPipesByBatchAndSlitAsync(
            string ndtBatchNo,
            string slitNo,
            int ndtPipes,
            CancellationToken cancellationToken) => Task.FromResult(0);

        public Task SyncOutputSlitRowsFromPerSlitCsvForBatchAsync(
            string ndtBatchNo,
            CancellationToken cancellationToken) => Task.CompletedTask;

        public Task UpdateNdtProcessConsolidatedFromStationsAsync(
            string poNumber,
            string ndtBatchNo,
            int ndtPcs,
            int okPcs,
            int visualReject,
            int hydrotestReject,
            int revisualReject,
            DateTime? bundleStart,
            DateTime? bundleEnd,
            string? outputFilePath,
            CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class FakeSeenTraceability
    {
        private readonly HashSet<string> _keys = new(StringComparer.OrdinalIgnoreCase);

        private static string Key(string file, DateTime lw) =>
            $"{file}|{lw:O}";

        public Task<bool> IsInputSlitFileSeenAsync(string sourceFile, DateTime sourceLastWriteTimeUtc, CancellationToken cancellationToken) =>
            Task.FromResult(_keys.Contains(Key(sourceFile, sourceLastWriteTimeUtc)));

        public Task MarkInputSlitFileSeenAsync(string sourceFile, DateTime sourceLastWriteTimeUtc, string reason, CancellationToken cancellationToken)
        {
            _keys.Add(Key(sourceFile, sourceLastWriteTimeUtc));
            return Task.CompletedTask;
        }
    }

    private sealed class EmptyBundleRepo : INdtBundleRepository
    {
        public Task RecordBundleAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RecordBundlePendingPrintAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task UpdateBundlePrintStatusAsync(string bundleNo, string printStatus, string? printError, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<IReadOnlyList<NdtBundleRecord>> GetStuckPrintsAsync(TimeSpan olderThan, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
        public Task<IReadOnlyList<NdtBundleRecord>> GetBundlesAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
        public Task<NdtBundleRecord?> GetByBatchNoAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(null);
        public Task<NdtBundleRecord?> GetLatestPrintedBundleForMillAsync(int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(null);
        public Task<bool> HasPrintedBundleForPoAsync(int millNo, string poNumber, CancellationToken cancellationToken) =>
            Task.FromResult(false);
        public Task<int> MarkManualReviewAsync(string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult(0);
        public Task TrySetPlcCloseMetadataAsync(int engineBatchSequence, int millNo, CancellationToken cancellationToken) =>
            Task.CompletedTask;
        public Task<(string BundleNo, int EngineSequence, int PlcTotal)?> TryGetAwaitingPlcReconBatchAsync(
            string poNumber, int millNo, CancellationToken cancellationToken) =>
            Task.FromResult<(string BundleNo, int EngineSequence, int PlcTotal)?>(null);
        public Task<PlcCsvReconResult?> TryReconcilePlcClosedBundleAsync(string poNumber, int millNo, int slitSum, CancellationToken cancellationToken) =>
            Task.FromResult<PlcCsvReconResult?>(null);
        public Task UpdateBundlePipesAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<int> UpdateOutputCsvFilesForBundleAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task<IReadOnlyList<(string SlitNo, int NdtPipes)>> GetSlitsForBatchAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string SlitNo, int NdtPipes)>>(Array.Empty<(string, int)>());
        public Task<int> UpdateOutputCsvFilesForSlitAsync(string batchNo, string slitNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task UpdateBundleTotalInDatabaseAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<bool> UpdateBundleSummaryCsvAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) =>
            Task.FromResult(false);
        public Task<int> TrySyncBundleTotalFromSlitsAsync(string batchNo, bool forceFromSlits, CancellationToken cancellationToken) =>
            Task.FromResult(0);
        public Task<(int RowsRemoved, IReadOnlyList<RemovedSlitRowTraceRef> TraceRefs)> DeletePerSlitOutputRowsForBatchSlitsAsync(
            string batchNo, IReadOnlyList<string> slitNos, CancellationToken cancellationToken) =>
            Task.FromResult<(int, IReadOnlyList<RemovedSlitRowTraceRef>)>((0, Array.Empty<RemovedSlitRowTraceRef>()));
    }

    private sealed class EmptyActivePo : IActivePoPerMillService
    {
        public Task<IReadOnlyDictionary<int, string>> GetLatestPoByMillAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<int, string>>(new Dictionary<int, string>());

        public IReadOnlyList<string> GetInputSlitReadFolderPaths() => Array.Empty<string>();
    }
}
