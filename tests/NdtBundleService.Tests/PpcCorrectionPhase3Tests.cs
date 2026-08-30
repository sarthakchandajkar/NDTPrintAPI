using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging.Abstractions;
using NdtBundleService.Controllers;
using NdtBundleService.Models;
using NdtBundleService.Services;
using Xunit;

namespace NdtBundleService.Tests;

/// <summary>
/// Phase 3 PPC correction tracking (docs/NDT_Input_Slit_SAP_Status_Design.md):
/// - hard requirement 4: Manual_Review and Ppc_Correction_Pending are distinct indicators that can
///   be open simultaneously on one bundle without ambiguity;
/// - slit reconcile touching SAP-Accepted data auto-creates/updates one Ppc_Correction_Item per
///   Accepted file (old value = pre-reconcile slit value, i.e. what SAP still has);
/// - clearing the last open item removes the derived Ppc_Correction_Pending status.
/// </summary>
public sealed class PpcCorrectionPhase3Tests
{
    private const string BatchNo = "1226100001";
    private const string AcceptedFile = "2604361_01_260726_1000060363.csv";

    [Fact]
    public async Task Bundle_slits_reports_manual_review_and_ppc_pending_simultaneously_as_distinct_flags()
    {
        var ppc = new FakePpcRepo();
        await ppc.UpsertOpenItemAsync(BatchNo, AcceptedFile, "01", 10, 12, CancellationToken.None);

        var controller = CreateController(
            new FakeBundleRepo { ManualReviewFlagged = true },
            new FakeSapRepo(),
            ppc,
            new FakeReconcileSync());

        var result = await controller.GetBundleSlits(BatchNo, CancellationToken.None);

        var ok = Assert.IsType<OkObjectResult>(result);
        var bundle = GetProp(ok.Value!, "Bundle")!;

        // Hard requirement 4: two separately named flags, both true at once, no shared field.
        Assert.Equal(true, GetProp(bundle, "ManualReview"));
        Assert.Equal(true, GetProp(bundle, "PpcCorrectionPending"));
        Assert.Equal(1, GetProp(bundle, "PpcOpenCorrectionCount"));
    }

    [Fact]
    public async Task Reconcile_slit_on_accepted_file_creates_open_ppc_item_with_old_and_corrected_values()
    {
        var ppc = new FakePpcRepo();
        var controller = CreateController(new FakeBundleRepo(), new FakeSapRepo(), ppc, new FakeReconcileSync());

        var result = await controller.ReconcileSlit(
            new ReconcileController.ReconcileSlitRequest { NdtBatchNo = BatchNo, SlitNo = "01", NewNdtPipes = 12 },
            CancellationToken.None);

        var ok = Assert.IsType<OkObjectResult>(result);
        Assert.Equal(1, GetProp(ok.Value!, "PpcCorrectionItemsCreated"));
        Assert.Equal(0, GetProp(ok.Value!, "PpcCorrectionItemsUpdated"));

        var item = Assert.Single(ppc.Items);
        Assert.Equal(BatchNo, item.NdtBatchNo);
        Assert.Equal(AcceptedFile, item.FileName);
        Assert.Equal("01", item.SlitNo);
        Assert.Equal(10, item.OldNdtPipes); // pre-reconcile slit value = what SAP still has
        Assert.Equal(12, item.CorrectedNdtPipes);
        Assert.Equal(PpcCorrectionItem.StatusOpen, item.Status);
    }

    [Fact]
    public async Task Second_reconcile_updates_existing_open_item_instead_of_stacking()
    {
        var ppc = new FakePpcRepo();
        var controller = CreateController(new FakeBundleRepo(), new FakeSapRepo(), ppc, new FakeReconcileSync());

        await controller.ReconcileSlit(
            new ReconcileController.ReconcileSlitRequest { NdtBatchNo = BatchNo, SlitNo = "01", NewNdtPipes = 12 },
            CancellationToken.None);
        var second = await controller.ReconcileSlit(
            new ReconcileController.ReconcileSlitRequest { NdtBatchNo = BatchNo, SlitNo = "01", NewNdtPipes = 14 },
            CancellationToken.None);

        var ok = Assert.IsType<OkObjectResult>(second);
        Assert.Equal(0, GetProp(ok.Value!, "PpcCorrectionItemsCreated"));
        Assert.Equal(1, GetProp(ok.Value!, "PpcCorrectionItemsUpdated"));

        var item = Assert.Single(ppc.Items);
        Assert.Equal(10, item.OldNdtPipes); // original SAP-side value preserved
        Assert.Equal(14, item.CorrectedNdtPipes);
    }

    [Fact]
    public async Task Clearing_last_open_item_removes_derived_ppc_pending_status()
    {
        var ppc = new FakePpcRepo();
        var upsert = await ppc.UpsertOpenItemAsync(BatchNo, AcceptedFile, "01", 10, 12, CancellationToken.None);
        var controller = CreateController(new FakeBundleRepo(), new FakeSapRepo(), ppc, new FakeReconcileSync());

        var cleared = await controller.ClearPpcCorrection(upsert!.Id, null, CancellationToken.None);
        Assert.IsType<OkObjectResult>(cleared);

        // Clearing an already-cleared item is a 404, not a silent success.
        var again = await controller.ClearPpcCorrection(upsert.Id, null, CancellationToken.None);
        Assert.IsType<NotFoundObjectResult>(again);

        var slitsResult = await controller.GetBundleSlits(BatchNo, CancellationToken.None);
        var ok = Assert.IsType<OkObjectResult>(slitsResult);
        var bundle = GetProp(ok.Value!, "Bundle")!;
        Assert.Equal(false, GetProp(bundle, "PpcCorrectionPending"));
        Assert.Equal(0, GetProp(bundle, "PpcOpenCorrectionCount"));
    }

    private static ReconcileController CreateController(
        FakeBundleRepo bundleRepo,
        FakeSapRepo sapRepo,
        FakePpcRepo ppcRepo,
        FakeReconcileSync reconcileSync) =>
        new(
            bundleRepo,
            traceability: null!,
            reconcileSync,
            formationChartProvider: null!,
            pipeSizeProvider: null!,
            reconcileTagService: null!,
            sapRepo,
            ppcRepo,
            new NoOpMerge(),
            new TestOptionsMonitor(),
            NullLogger<ReconcileController>.Instance);

    private sealed class NoOpMerge : IBundleMergeService
    {
        public Task<BundleMergePreview?> TryPreviewAsync(string sourceBundleNo, CancellationToken cancellationToken) =>
            Task.FromResult<BundleMergePreview?>(null);

        public Task<BundleMergeResult> MergeIntoPreviousAsync(
            string sourceBundleNo,
            string reason,
            string updatedBy,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();
    }

    private sealed class TestOptionsMonitor : Microsoft.Extensions.Options.IOptionsMonitor<NdtBundleService.Configuration.NdtBundleOptions>
    {
        public NdtBundleService.Configuration.NdtBundleOptions CurrentValue { get; } = new();
        public NdtBundleService.Configuration.NdtBundleOptions Get(string? name) => CurrentValue;
        public IDisposable OnChange(Action<NdtBundleService.Configuration.NdtBundleOptions, string?> listener) =>
            Microsoft.Extensions.Options.Options.Create(CurrentValue) as IDisposable ?? NullDisp.Instance;
        private sealed class NullDisp : IDisposable { public static readonly NullDisp Instance = new(); public void Dispose() { } }
    }

    private static object? GetProp(object obj, string name) =>
        obj.GetType().GetProperty(name)?.GetValue(obj);

    /// <summary>One bundle with one Accepted-backed slit "01" (10 pipes); CSV/SQL writes succeed.</summary>
    private sealed class FakeBundleRepo : INdtBundleRepository
    {
        public bool ManualReviewFlagged { get; init; }

        public Task<NdtBundleRecord?> GetByBatchNoAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<NdtBundleRecord?>(new NdtBundleRecord
            {
                BundleNo = BatchNo,
                PoNumber = "1000060363",
                MillNo = 1,
                TotalNdtPcs = 10,
                PrintStatus = BundlePrintStatus.Printed
            });

        public Task<IReadOnlyList<(string SlitNo, int NdtPipes)>> GetSlitsForBatchAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string, int)>>(new[] { ("01", 10) });

        public Task<IReadOnlyList<(string SlitNo, string SourceFileName)>> GetSlitSourceFileNamesForBatchAsync(
            string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<(string, string)>>(new[] { ("01", AcceptedFile) });

        public Task<bool> IsManualReviewFlaggedAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult(ManualReviewFlagged);

        public Task<int> UpdateOutputCsvFilesForSlitAsync(string batchNo, string slitNo, int newPipes, CancellationToken cancellationToken) =>
            Task.FromResult(1);

        public Task<int> TrySyncBundleTotalFromSlitsAsync(string batchNo, bool forceFromSlits, CancellationToken cancellationToken) =>
            Task.FromResult(10);

        public Task RecordBundleAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task RecordBundlePendingPrintAsync(NdtBundleRecord record, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task UpdateBundlePrintStatusAsync(string bundleNo, string printStatus, string? printError, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<IReadOnlyList<NdtBundleRecord>> GetStuckPrintsAsync(TimeSpan olderThan, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
        public Task<IReadOnlyList<NdtBundleRecord>> GetBundlesAsync(CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<NdtBundleRecord>>(Array.Empty<NdtBundleRecord>());
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
        public Task UpdateBundlePipesAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<int> UpdateOutputCsvFilesForBundleAsync(string batchNo, int newPipes, CancellationToken cancellationToken) => Task.FromResult(0);
        public Task UpdateBundleTotalInDatabaseAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) => Task.CompletedTask;
        public Task<bool> UpdateBundleSummaryCsvAsync(string batchNo, int newTotalPipes, CancellationToken cancellationToken) =>
            Task.FromResult(false);
        public Task<(int RowsRemoved, IReadOnlyList<RemovedSlitRowTraceRef> TraceRefs)> DeletePerSlitOutputRowsForBatchSlitsAsync(
            string batchNo, IReadOnlyList<string> slitNos, CancellationToken cancellationToken) =>
            Task.FromResult<(int, IReadOnlyList<RemovedSlitRowTraceRef>)>((0, Array.Empty<RemovedSlitRowTraceRef>()));
    }

    private sealed class FakeSapRepo : IOutputSlitSapStatusRepository
    {
        public bool Enabled => true;

        public Task<OutputSlitSapStatusApplyResult> ApplyObservationsAsync(IReadOnlyList<OutputSlitSapStatusObservation> observations, CancellationToken cancellationToken) =>
            Task.FromResult(OutputSlitSapStatusApplyResult.Empty);

        public Task RecordOutputFileWrittenAsync(string fileName, DateTime? fileLastWriteTimeUtc, string outputFolder, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task RecordResubmitDriftSyncedEventAsync(string fileName, string pendingFolder, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task<IReadOnlyDictionary<string, OutputSlitSapFileStatus>> GetStatusesForFilesAsync(
            IReadOnlyCollection<string> fileNames, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyDictionary<string, OutputSlitSapFileStatus>>(
                new Dictionary<string, OutputSlitSapFileStatus>(StringComparer.OrdinalIgnoreCase)
                {
                    [AcceptedFile] = new(OutputSlitSapStatus.Accepted, DateTime.UtcNow, 0)
                });
    }

    /// <summary>In-memory Ppc_Correction_Item store mirroring the SQL upsert/clear semantics.</summary>
    private sealed class FakePpcRepo : IPpcCorrectionRepository
    {
        private long _nextId = 1;
        public List<PpcCorrectionItem> Items { get; } = new();

        public bool Enabled => true;

        public Task<PpcCorrectionUpsertResult?> UpsertOpenItemAsync(
            string batchNo, string fileName, string slitNo, int? oldNdtPipes, int correctedNdtPipes, CancellationToken cancellationToken)
        {
            var slit = ReconcileCsvParsing.NormalizeSlitKey(slitNo);
            var existing = Items.FindIndex(i =>
                i.Status == PpcCorrectionItem.StatusOpen
                && string.Equals(i.NdtBatchNo, batchNo, StringComparison.OrdinalIgnoreCase)
                && string.Equals(i.FileName, fileName, StringComparison.OrdinalIgnoreCase)
                && string.Equals(i.SlitNo, slit, StringComparison.OrdinalIgnoreCase));
            if (existing >= 0)
            {
                Items[existing] = Items[existing] with { CorrectedNdtPipes = correctedNdtPipes, UpdatedAtUtc = DateTime.UtcNow };
                return Task.FromResult<PpcCorrectionUpsertResult?>(new(Items[existing].Id, Created: false));
            }

            var item = new PpcCorrectionItem(
                _nextId++, batchNo, fileName, slit, oldNdtPipes, correctedNdtPipes,
                PpcCorrectionItem.StatusOpen, DateTime.UtcNow, null, null, null, null);
            Items.Add(item);
            return Task.FromResult<PpcCorrectionUpsertResult?>(new(item.Id, Created: true));
        }

        public Task<IReadOnlyList<PpcCorrectionItem>> GetItemsForBatchAsync(string batchNo, bool includeCleared, CancellationToken cancellationToken) =>
            Task.FromResult<IReadOnlyList<PpcCorrectionItem>>(Items
                .Where(i => string.Equals(i.NdtBatchNo, batchNo, StringComparison.OrdinalIgnoreCase)
                            && (includeCleared || i.Status == PpcCorrectionItem.StatusOpen))
                .ToList());

        public Task<int> CountOpenItemsForBatchAsync(string batchNo, CancellationToken cancellationToken) =>
            Task.FromResult(Items.Count(i =>
                i.Status == PpcCorrectionItem.StatusOpen
                && string.Equals(i.NdtBatchNo, batchNo, StringComparison.OrdinalIgnoreCase)));

        public Task<bool> ClearItemAsync(long id, string? clearedBy, string? note, CancellationToken cancellationToken)
        {
            var idx = Items.FindIndex(i => i.Id == id && i.Status == PpcCorrectionItem.StatusOpen);
            if (idx < 0)
                return Task.FromResult(false);
            Items[idx] = Items[idx] with
            {
                Status = PpcCorrectionItem.StatusCleared,
                ClearedAtUtc = DateTime.UtcNow,
                ClearedBy = clearedBy,
                ClearedNote = note
            };
            return Task.FromResult(true);
        }
    }

    private sealed class FakeReconcileSync : IReconcileSyncService
    {
        public Task SyncAfterBundleTotalReconcileAsync(string ndtBatchNo, string poNumber, int newBundleTotalPcs, CancellationToken cancellationToken) =>
            Task.CompletedTask;

        public Task<int> SyncAfterSlitReconcileAsync(string ndtBatchNo, string slitNo, int newNdtPipes, CancellationToken cancellationToken) =>
            Task.FromResult(1);

        public Task SyncAfterManualStationReconcileAsync(
            ManualTagStation station, ManualStationReconcileSnapshot snapshot, string? ndtProcessCsvPath, CancellationToken cancellationToken) =>
            Task.CompletedTask;
    }
}
