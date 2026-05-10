using System.Globalization;
using System.Text.Json;
using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

public enum ManualTagStation
{
    Visual,
    Hydrotesting,
    /// <summary>Same pipeline step as <see cref="Hydrotesting"/>; distinct CSV/station label for Four Head line.</summary>
    FourHeadHydrotesting,
    /// <summary>Same pipeline step as <see cref="Hydrotesting"/>; distinct CSV/station label for Big Hydro line.</summary>
    BigHydrotesting,
    Revisual
}

public interface IManualNdtTagService
{
    /// <param name="operatorStationNumber">For Visual and Revisual only: physical station id (1 or 2). Ignored for hydro stations.</param>
    Task<ManualStationContext> GetContextAsync(ManualTagStation station, string ndtBatchNo, int operatorStationNumber, CancellationToken cancellationToken);
    Task<ManualStationRecordResult> RecordAsync(ManualStationRecordRequest request, CancellationToken cancellationToken);
    /// <summary>Adjust OK/Rejected for a station that was already recorded; may cascade clearing downstream steps and replaces the prior CSV.</summary>
    Task<ManualStationRecordResult> ReconcileAsync(ManualStationRecordRequest request, CancellationToken cancellationToken);
}

public sealed class ManualStationContext
{
    public string NdtBatchNo { get; init; } = string.Empty;
    public string PoNumber { get; init; } = string.Empty;
    public int MillNo { get; init; }
    /// <summary>For Visual/Revisual: physical station (1 or 2). Always 1 for hydro.</summary>
    public int OperatorStationNumber { get; init; } = 1;
    public int IncomingPcs { get; init; }
    public int? AlreadyOkPcs { get; init; }
    public int? AlreadyRejectedPcs { get; init; }
    public int? OutgoingPcs { get; init; }
    /// <summary>True after Visual was reconciled until Hydro is saved again.</summary>
    public bool HydroRedoRequired { get; init; }
    /// <summary>True after Visual or Hydro was reconciled until Revisual is saved again.</summary>
    public bool RevisualRedoRequired { get; init; }
    /// <summary>True when this station already has a saved record (enables Reconcile vs first Save).</summary>
    public bool HasRecordedThisStation { get; init; }
}

public sealed class ManualStationRecordRequest
{
    public ManualTagStation Station { get; init; }
    public string NdtBatchNo { get; init; } = string.Empty;
    public int OkPcs { get; init; }
    public int RejectedPcs { get; init; }
    public string User { get; init; } = string.Empty;
    public DateTime? StartTime { get; init; }
    public DateTime? EndTime { get; init; }
    public bool PrintTag { get; init; }
    /// <summary>For Visual and Revisual: physical station id 1 or 2 (CSV filename and Work Station column). Default 1.</summary>
    public int OperatorStationNumber { get; init; } = 1;
}

public sealed class ManualStationRecordResult
{
    public string NdtBatchNo { get; init; } = string.Empty;
    public ManualTagStation Station { get; init; }
    public int IncomingPcs { get; init; }
    public int OkPcs { get; init; }
    public int RejectedPcs { get; init; }
    public int OutgoingPcs { get; init; }
    public string CsvPath { get; init; } = string.Empty;
    public bool Printed { get; init; }
}

/// <summary>
/// Manual tag printing for Visual/Hydrotesting/Revisual stations.
/// Writes one consolidated CSV in <see cref="NdtBundleOptions.NdtProcessOutputFolder"/> after Revisual completes (not per station).
/// </summary>
public sealed class ManualNdtTagService : IManualNdtTagService
{
    private readonly NdtBundleOptions _options;
    private readonly IZplGenerationToggle _zplToggle;
    private readonly INdtBundleRepository _bundleRepository;
    private readonly IWipLabelProvider _wipLabelProvider;
    private readonly INetworkPrinterSender _sender;
    private readonly ITraceabilityRepository _traceability;
    private readonly ILogger<ManualNdtTagService> _logger;

    private static readonly object StateLock = new();
    private static readonly ConcurrentDictionary<string, FlowState> InMemoryState = new(StringComparer.OrdinalIgnoreCase);

    public ManualNdtTagService(
        IOptions<NdtBundleOptions> options,
        IZplGenerationToggle zplToggle,
        INdtBundleRepository bundleRepository,
        IWipLabelProvider wipLabelProvider,
        INetworkPrinterSender sender,
        ITraceabilityRepository traceability,
        ILogger<ManualNdtTagService> logger)
    {
        _options = options.Value;
        _zplToggle = zplToggle;
        _bundleRepository = bundleRepository;
        _wipLabelProvider = wipLabelProvider;
        _sender = sender;
        _traceability = traceability;
        _logger = logger;
    }

    public async Task<ManualStationContext> GetContextAsync(ManualTagStation station, string ndtBatchNo, int operatorStationNumber, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(ndtBatchNo))
            throw new ArgumentException("NdtBatchNo is required.", nameof(ndtBatchNo));

        var opStation = NormalizeOperatorStationNumber(station, operatorStationNumber);
        var batch = ndtBatchNo.Trim();
        var state = await LoadOrCreateStateAsync(batch, cancellationToken).ConfigureAwait(false);
        var incoming = GetIncomingForStation(state, station);
        var existing = GetExistingForStation(state, station);

        return new ManualStationContext
        {
            NdtBatchNo = batch,
            PoNumber = state.PoNumber,
            MillNo = state.MillNo,
            OperatorStationNumber = opStation,
            IncomingPcs = incoming,
            AlreadyOkPcs = existing?.OkPcs,
            AlreadyRejectedPcs = existing?.RejectedPcs,
            OutgoingPcs = existing?.OkPcs,
            HydroRedoRequired = state.HydroInvalidatedByVisualReconcile && state.Hydrotesting is null,
            RevisualRedoRequired = state.RevisualInvalidatedByUpstreamReconcile && state.Revisual is null,
            HasRecordedThisStation = existing != null
        };
    }

    public async Task<ManualStationRecordResult> RecordAsync(ManualStationRecordRequest request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.NdtBatchNo))
            throw new ArgumentException("NdtBatchNo is required.", nameof(request));
        if (request.OkPcs < 0 || request.RejectedPcs < 0)
            throw new ArgumentException("OK/Rejected pcs must be non-negative.", nameof(request));

        var batch = request.NdtBatchNo.Trim();
        var state = await LoadOrCreateStateAsync(batch, cancellationToken).ConfigureAwait(false);
        var incoming = GetIncomingForStation(state, request.Station);
        var opStation = NormalizeOperatorStationNumber(request.Station, request.OperatorStationNumber);

        if (request.OkPcs + request.RejectedPcs != incoming)
            throw new InvalidOperationException($"OK ({request.OkPcs}) + Rejected ({request.RejectedPcs}) must equal Incoming ({incoming}).");

        var now = DateTime.Now;
        var start = request.StartTime ?? now;
        var end = request.EndTime ?? now;

        SetStationRecord(state, request.Station, new StationRecord
        {
            OkPcs = request.OkPcs,
            RejectedPcs = request.RejectedPcs,
            User = request.User?.Trim() ?? "",
            StartTime = start,
            EndTime = end
        });
        if (IsHydroStation(request.Station))
            state.HydroInvalidatedByVisualReconcile = false;
        if (request.Station == ManualTagStation.Revisual)
            state.RevisualInvalidatedByUpstreamReconcile = false;

        await SaveStateAsync(state, cancellationToken).ConfigureAwait(false);

        string? csvPath = null;
        if (request.Station == ManualTagStation.Revisual)
        {
            csvPath = await WriteConsolidatedNdtProcessCsvAsync(state, cancellationToken).ConfigureAwait(false);
            state.LastNdtProcessCsvPath = csvPath;
            await SaveStateAsync(state, cancellationToken).ConfigureAwait(false);
            await RecordConsolidatedTraceabilityAsync(state, csvPath, cancellationToken).ConfigureAwait(false);
        }

        // Best-effort SQL traceability; do not fail the station flow if SQL is down.
        await _traceability.RecordManualStationRunAsync(
            poNumber: state.PoNumber,
            ndtBatchNo: batch,
            ndtPcs: incoming,
            okPcs: request.OkPcs,
            rejectPcs: request.RejectedPcs,
            workStation: WorkStationColumnValue(request.Station, opStation),
            start: start,
            end: end,
            hydrotestingType: IsHydroStation(request.Station) ? HydrotestingTypeValue(request.Station) : null,
            sourceFile: csvPath ?? string.Empty,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        var printed = false;
        if (request.PrintTag)
        {
            printed = await TryPrintTagAsync(state.PoNumber, state.MillNo, batch, request.OkPcs, now, request.Station, opStation, isReprint: false, cancellationToken).ConfigureAwait(false);
        }

        return new ManualStationRecordResult
        {
            NdtBatchNo = batch,
            Station = request.Station,
            IncomingPcs = incoming,
            OkPcs = request.OkPcs,
            RejectedPcs = request.RejectedPcs,
            OutgoingPcs = request.OkPcs,
            CsvPath = csvPath ?? string.Empty,
            Printed = printed
        };
    }

    public async Task<ManualStationRecordResult> ReconcileAsync(ManualStationRecordRequest request, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(request.NdtBatchNo))
            throw new ArgumentException("NdtBatchNo is required.", nameof(request));
        if (request.OkPcs < 0 || request.RejectedPcs < 0)
            throw new ArgumentException("OK/Rejected pcs must be non-negative.", nameof(request));

        var batch = request.NdtBatchNo.Trim();
        var state = await LoadOrCreateStateAsync(batch, cancellationToken).ConfigureAwait(false);
        var existing = GetExistingForStation(state, request.Station);
        if (existing is null)
            throw new InvalidOperationException(
                $"No existing {StationName(request.Station)} record for batch {batch}. Use Save first, then Reconcile to adjust counts.");

        var opStation = NormalizeOperatorStationNumber(request.Station, request.OperatorStationNumber);
        var incoming = GetIncomingForStation(state, request.Station);
        if (request.OkPcs + request.RejectedPcs != incoming)
            throw new InvalidOperationException($"OK ({request.OkPcs}) + Rejected ({request.RejectedPcs}) must equal Incoming ({incoming}).");

        var now = DateTime.Now;
        var start = request.StartTime ?? now;
        var end = request.EndTime ?? now;

        switch (request.Station)
        {
            case ManualTagStation.Visual:
                TryDeleteCsv(state.LastNdtProcessCsvPath);
                state.Hydrotesting = null;
                state.Revisual = null;
                state.LastNdtProcessCsvPath = null;
                state.HydroInvalidatedByVisualReconcile = true;
                state.RevisualInvalidatedByUpstreamReconcile = true;
                break;
            case ManualTagStation.Hydrotesting:
            case ManualTagStation.FourHeadHydrotesting:
            case ManualTagStation.BigHydrotesting:
                TryDeleteCsv(state.LastNdtProcessCsvPath);
                state.Revisual = null;
                state.LastNdtProcessCsvPath = null;
                state.HydroInvalidatedByVisualReconcile = false;
                state.RevisualInvalidatedByUpstreamReconcile = true;
                break;
            case ManualTagStation.Revisual:
                TryDeleteCsv(state.LastNdtProcessCsvPath);
                state.LastNdtProcessCsvPath = null;
                state.RevisualInvalidatedByUpstreamReconcile = false;
                break;
        }

        SetStationRecord(state, request.Station, new StationRecord
        {
            OkPcs = request.OkPcs,
            RejectedPcs = request.RejectedPcs,
            User = request.User?.Trim() ?? "",
            StartTime = start,
            EndTime = end
        });

        await SaveStateAsync(state, cancellationToken).ConfigureAwait(false);

        string? csvPath = null;
        if (request.Station == ManualTagStation.Revisual)
        {
            csvPath = await WriteConsolidatedNdtProcessCsvAsync(state, cancellationToken).ConfigureAwait(false);
            state.LastNdtProcessCsvPath = csvPath;
            await SaveStateAsync(state, cancellationToken).ConfigureAwait(false);
            await RecordConsolidatedTraceabilityAsync(state, csvPath, cancellationToken).ConfigureAwait(false);
        }

        await _traceability.RecordManualStationRunAsync(
            poNumber: state.PoNumber,
            ndtBatchNo: batch,
            ndtPcs: incoming,
            okPcs: request.OkPcs,
            rejectPcs: request.RejectedPcs,
            workStation: WorkStationColumnValue(request.Station, opStation),
            start: start,
            end: end,
            hydrotestingType: IsHydroStation(request.Station) ? HydrotestingTypeValue(request.Station) : null,
            sourceFile: csvPath ?? string.Empty,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        var printed = false;
        if (request.PrintTag)
        {
            printed = await TryPrintTagAsync(state.PoNumber, state.MillNo, batch, request.OkPcs, now, request.Station, opStation, isReprint: true, cancellationToken).ConfigureAwait(false);
        }

        return new ManualStationRecordResult
        {
            NdtBatchNo = batch,
            Station = request.Station,
            IncomingPcs = incoming,
            OkPcs = request.OkPcs,
            RejectedPcs = request.RejectedPcs,
            OutgoingPcs = request.OkPcs,
            CsvPath = csvPath ?? string.Empty,
            Printed = printed
        };
    }

    private static int NormalizeOperatorStationNumber(ManualTagStation station, int value)
    {
        if (station != ManualTagStation.Visual && station != ManualTagStation.Revisual)
            return 1;
        if (value is < 1 or > 2)
            throw new ArgumentException("OperatorStationNumber must be 1 or 2 for Visual and Revisual stations.");
        return value;
    }

    private string GetNdtProcessOutputFolder()
    {
        var folder = (_options.NdtProcessOutputFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(folder))
            throw new InvalidOperationException("NdtProcessOutputFolder is not configured.");
        return folder;
    }

    private static string StationName(ManualTagStation station) =>
        station switch
        {
            ManualTagStation.Visual => "Visual",
            ManualTagStation.Hydrotesting => "Hydrotesting",
            ManualTagStation.FourHeadHydrotesting => "Four Head Hydrotesting",
            ManualTagStation.BigHydrotesting => "Big Hydrotesting",
            ManualTagStation.Revisual => "Revisual",
            _ => "Manual"
        };

    private static string StationTextForZpl(ManualTagStation station, int operatorStationNumber) =>
        station switch
        {
            ManualTagStation.Visual => $"Visual Station {operatorStationNumber}",
            ManualTagStation.Revisual => $"Revisual Station {operatorStationNumber}",
            ManualTagStation.FourHeadHydrotesting => "Four Head Hydrotesting",
            ManualTagStation.BigHydrotesting => "Big Hydrotesting",
            ManualTagStation.Hydrotesting => "Hydrotesting",
            _ => StationName(station)
        };

    private static bool IsHydroStation(ManualTagStation station) =>
        station is ManualTagStation.Hydrotesting or ManualTagStation.FourHeadHydrotesting or ManualTagStation.BigHydrotesting;

    /// <summary>Work Station column: Visual/Revisual include physical station number; hydro uses Hydrotesting.</summary>
    private static string WorkStationColumnValue(ManualTagStation station, int operatorStationNumber) =>
        station switch
        {
            ManualTagStation.Visual => $"Visual Station {operatorStationNumber}",
            ManualTagStation.Revisual => $"Revisual Station {operatorStationNumber}",
            ManualTagStation.Hydrotesting or ManualTagStation.FourHeadHydrotesting or ManualTagStation.BigHydrotesting => "Hydrotesting",
            _ => StationName(station)
        };

    /// <summary>Hydrotesting-only column; Four Head vs Big, or empty for legacy <see cref="ManualTagStation.Hydrotesting"/>.</summary>
    private static string HydrotestingTypeValue(ManualTagStation station) =>
        station switch
        {
            ManualTagStation.FourHeadHydrotesting => "Four Head Hydrotesting",
            ManualTagStation.BigHydrotesting => "Big Hydrotesting",
            ManualTagStation.Hydrotesting => "",
            _ => ""
        };

    /// <summary>Single CSV after Revisual with Visual/Hydro/Revisual metrics; PO and batch tokens sanitized for the filename.</summary>
    private async Task<string> WriteConsolidatedNdtProcessCsvAsync(FlowState state, CancellationToken cancellationToken)
    {
        var visual = state.Visual ?? throw new InvalidOperationException("Visual station must be recorded before exporting NDT process CSV.");
        var hydro = state.Hydrotesting ?? throw new InvalidOperationException("Hydrotesting station must be recorded before exporting NDT process CSV.");
        var revisual = state.Revisual ?? throw new InvalidOperationException("Revisual station must be recorded before exporting NDT process CSV.");

        var folder = GetNdtProcessOutputFolder();
        Directory.CreateDirectory(folder);

        var now = DateTime.Now;
        var datePart = now.ToString("yyyyMMdd", CultureInfo.InvariantCulture);
        var timePart = now.ToString("HHmmss", CultureInfo.InvariantCulture);
        var safePo = CsvOutputFileNaming.SanitizeToken(state.PoNumber.Trim());
        var safeBatch = CsvOutputFileNaming.SanitizeToken(state.NdtBatchNo);
        var fileName = $"NDT process_{safePo}_{safeBatch}_{datePart}_{timePart}.csv";
        var path = Path.Combine(folder, fileName);

        const string header = "PO Number, NDT BATCH NO, NDT Pcs, OK, Visual Reject, Hydrotest Reject, Re visual Reject, Bundle Start, Bundle End";
        var row = string.Join(", ",
            Escape(state.PoNumber.Trim()),
            Escape(state.NdtBatchNo),
            state.InitialPcs.ToString(CultureInfo.InvariantCulture),
            revisual.OkPcs.ToString(CultureInfo.InvariantCulture),
            visual.RejectedPcs.ToString(CultureInfo.InvariantCulture),
            hydro.RejectedPcs.ToString(CultureInfo.InvariantCulture),
            revisual.RejectedPcs.ToString(CultureInfo.InvariantCulture),
            Escape(FormatBundleDateTime(visual.StartTime)),
            Escape(FormatBundleDateTime(revisual.EndTime)));

        await File.WriteAllLinesAsync(path, new[] { header, row }, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation("Wrote consolidated NDT process CSV: {Path}", path);
        return path;
    }

    private Task RecordConsolidatedTraceabilityAsync(FlowState state, string csvPath, CancellationToken cancellationToken)
    {
        var visual = state.Visual!;
        var hydro = state.Hydrotesting!;
        var revisual = state.Revisual!;
        return _traceability.RecordNdtProcessConsolidatedAsync(
            poNumber: state.PoNumber.Trim(),
            ndtBatchNo: state.NdtBatchNo,
            ndtPcs: state.InitialPcs,
            okPcs: revisual.OkPcs,
            visualReject: visual.RejectedPcs,
            hydrotestReject: hydro.RejectedPcs,
            revisualReject: revisual.RejectedPcs,
            bundleStart: visual.StartTime,
            bundleEnd: revisual.EndTime,
            outputFilePath: csvPath,
            cancellationToken: cancellationToken);
    }

    private static string FormatBundleDateTime(DateTime dt) =>
        dt.ToString("dd.MM.yyyy HH:mm:ss", CultureInfo.InvariantCulture);

    private void TryDeleteCsv(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return;
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
                _logger.LogInformation("Deleted prior manual station CSV: {Path}", path);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not delete prior CSV {Path}", path);
        }
    }

    private async Task<bool> TryPrintTagAsync(string poNumber, int millNo, string ndtBatchNo, int pcs, DateTime date, ManualTagStation station, int operatorStationNumber, bool isReprint, CancellationToken cancellationToken)
    {
        if (!_zplToggle.IsEnabled)
        {
            _logger.LogDebug("NDT tag ZPL and network print are disabled (runtime toggle); CSV only.");
            return false;
        }

        var address = (_options.NdtTagPrinterAddress ?? "").Trim();
        var useAddress = !string.IsNullOrEmpty(address) && !address.Equals("0.0.0.0", StringComparison.OrdinalIgnoreCase);
        if (!useAddress)
        {
            _logger.LogWarning("Printer not configured (NdtTagPrinterAddress). Tag will not be sent.");
            return false;
        }

        var wip = await _wipLabelProvider.GetWipLabelAsync(poNumber, millNo, cancellationToken).ConfigureAwait(false);
        var zplBytes = ZplNdtLabelBuilder.BuildNdtTagZpl(
            ndtBatchNo,
            millNo,
            poNumber,
            wip?.PipeGrade,
            wip?.PipeSize ?? "",
            wip?.PipeThickness ?? "",
            wip?.PipeLength ?? "",
            wip?.PipeWeightPerMeter ?? "",
            wip?.PipeType ?? "",
            date,
            pcs,
            isReprint,
            StationTextForZpl(station, operatorStationNumber));

        await TrySaveZplPreviewAsync(station, operatorStationNumber, ndtBatchNo, zplBytes, cancellationToken).ConfigureAwait(false);
        var sendResult = await _sender.SendAsync(address, _options.NdtTagPrinterPort, zplBytes, cancellationToken).ConfigureAwait(false);
        return sendResult.Success;
    }

    private async Task TrySaveZplPreviewAsync(ManualTagStation station, int operatorStationNumber, string ndtBatchNo, byte[] zplBytes, CancellationToken cancellationToken)
    {
        try
        {
            string folder;
            try
            {
                folder = GetNdtProcessOutputFolder();
            }
            catch (InvalidOperationException)
            {
                return;
            }

            Directory.CreateDirectory(folder);
            var stationTag = station is ManualTagStation.Visual or ManualTagStation.Revisual
                ? $"{StationName(station)}St{operatorStationNumber}"
                : StationName(station).Replace(" ", "");
            var fileName = $"{stationTag}_NDTTag_{ndtBatchNo}_{DateTime.Now:yyyyMMddHHmmss}.zpl";
            var fullPath = Path.Combine(folder, fileName);
            await File.WriteAllBytesAsync(fullPath, zplBytes, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogDebug(ex, "Failed to save ZPL preview for manual tag {BatchNo}.", ndtBatchNo);
        }
    }

    private static string Escape(string value)
    {
        if (value.IndexOfAny(new[] { ',', '"', '\r', '\n' }) >= 0)
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        return value;
    }

    private string GetStateFolder()
    {
        var folder = (_options.OutputBundleFolder ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(folder))
            folder = AppContext.BaseDirectory;
        var stateFolder = Path.Combine(folder, "ManualStationState");
        Directory.CreateDirectory(stateFolder);
        return stateFolder;
    }

    private string GetStatePath(string ndtBatchNo) => Path.Combine(GetStateFolder(), $"{ndtBatchNo}.json");

    private async Task<FlowState> LoadOrCreateStateAsync(string ndtBatchNo, CancellationToken cancellationToken)
    {
        if (!_options.EnableManualStationStateFiles && InMemoryState.TryGetValue(ndtBatchNo, out var memoryState))
            return memoryState;

        if (_options.EnableManualStationStateFiles)
        {
            var path = GetStatePath(ndtBatchNo);
            lock (StateLock)
            {
                if (File.Exists(path))
                {
                    var json = File.ReadAllText(path);
                    var loaded = JsonSerializer.Deserialize<FlowState>(json);
                    if (loaded != null)
                    {
                        InMemoryState[ndtBatchNo] = loaded;
                        return loaded;
                    }
                }
            }
        }

        // Create from bundle record (authoritative PO/Mill/Total)
        var bundle = await _bundleRepository.GetByBatchNoAsync(ndtBatchNo, cancellationToken).ConfigureAwait(false);
        if (bundle is null)
            throw new InvalidOperationException($"Bundle {ndtBatchNo} not found. Ensure the NDT bundle tag has been printed first.");

        var state = new FlowState
        {
            NdtBatchNo = ndtBatchNo,
            PoNumber = bundle.PoNumber,
            MillNo = bundle.MillNo,
            InitialPcs = bundle.TotalNdtPcs
        };
        await SaveStateAsync(state, cancellationToken).ConfigureAwait(false);
        return state;
    }

    private Task SaveStateAsync(FlowState state, CancellationToken cancellationToken)
    {
        InMemoryState[state.NdtBatchNo] = state;
        if (!_options.EnableManualStationStateFiles)
            return Task.CompletedTask;

        var path = GetStatePath(state.NdtBatchNo);
        var json = JsonSerializer.Serialize(state, new JsonSerializerOptions { WriteIndented = true });
        lock (StateLock)
        {
            File.WriteAllText(path, json);
        }
        return Task.CompletedTask;
    }

    private static int GetIncomingForStation(FlowState state, ManualTagStation station)
    {
        return station switch
        {
            ManualTagStation.Visual => state.InitialPcs,
            ManualTagStation.Hydrotesting or ManualTagStation.FourHeadHydrotesting or ManualTagStation.BigHydrotesting =>
                state.Visual?.OkPcs ?? throw new InvalidOperationException("Visual station not recorded yet for this batch."),
            ManualTagStation.Revisual => state.Hydrotesting?.OkPcs ?? throw new InvalidOperationException("Hydrotesting station not recorded yet for this batch."),
            _ => state.InitialPcs
        };
    }

    private static StationRecord? GetExistingForStation(FlowState state, ManualTagStation station) =>
        station switch
        {
            ManualTagStation.Visual => state.Visual,
            ManualTagStation.Hydrotesting or ManualTagStation.FourHeadHydrotesting or ManualTagStation.BigHydrotesting => state.Hydrotesting,
            ManualTagStation.Revisual => state.Revisual,
            _ => null
        };

    private static void SetStationRecord(FlowState state, ManualTagStation station, StationRecord record)
    {
        switch (station)
        {
            case ManualTagStation.Visual:
                state.Visual = record;
                break;
            case ManualTagStation.Hydrotesting:
            case ManualTagStation.FourHeadHydrotesting:
            case ManualTagStation.BigHydrotesting:
                state.Hydrotesting = record;
                break;
            case ManualTagStation.Revisual:
                state.Revisual = record;
                break;
        }
    }

    private sealed class FlowState
    {
        public string NdtBatchNo { get; set; } = string.Empty;
        public string PoNumber { get; set; } = string.Empty;
        public int MillNo { get; set; }
        public int InitialPcs { get; set; }
        public StationRecord? Visual { get; set; }
        public StationRecord? Hydrotesting { get; set; }
        public StationRecord? Revisual { get; set; }

        /// <summary>Last consolidated NDT process CSV (replace/delete on reconcile).</summary>
        public string? LastNdtProcessCsvPath { get; set; }

        /// <summary>Set when Visual is reconciled until Hydro is saved again.</summary>
        public bool HydroInvalidatedByVisualReconcile { get; set; }

        /// <summary>Set when Visual or Hydro is reconciled until Revisual is saved again.</summary>
        public bool RevisualInvalidatedByUpstreamReconcile { get; set; }

        public StationRecord? GetRecord(ManualTagStation station) => GetExistingForStation(this, station);
    }

    private sealed class StationRecord
    {
        public int OkPcs { get; set; }
        public int RejectedPcs { get; set; }
        public string User { get; set; } = string.Empty;
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
    }
}

