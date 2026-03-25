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
    Task<ManualStationContext> GetContextAsync(ManualTagStation station, string ndtBatchNo, CancellationToken cancellationToken);
    Task<ManualStationRecordResult> RecordAsync(ManualStationRecordRequest request, CancellationToken cancellationToken);
}

public sealed class ManualStationContext
{
    public string NdtBatchNo { get; init; } = string.Empty;
    public string PoNumber { get; init; } = string.Empty;
    public int MillNo { get; init; }
    public int IncomingPcs { get; init; }
    public int? AlreadyOkPcs { get; init; }
    public int? AlreadyRejectedPcs { get; init; }
    public int? OutgoingPcs { get; init; }
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
/// Generates a unique NDT Batch Number, prints a ZPL tag, and writes a station CSV to the configured output folder.
/// </summary>
public sealed class ManualNdtTagService : IManualNdtTagService
{
    private readonly NdtBundleOptions _options;
    private readonly INdtBundleRepository _bundleRepository;
    private readonly IWipLabelProvider _wipLabelProvider;
    private readonly INetworkPrinterSender _sender;
    private readonly ILogger<ManualNdtTagService> _logger;

    private static readonly object StateLock = new();
    private static readonly ConcurrentDictionary<string, FlowState> InMemoryState = new(StringComparer.OrdinalIgnoreCase);

    public ManualNdtTagService(
        IOptions<NdtBundleOptions> options,
        INdtBundleRepository bundleRepository,
        IWipLabelProvider wipLabelProvider,
        INetworkPrinterSender sender,
        ILogger<ManualNdtTagService> logger)
    {
        _options = options.Value;
        _bundleRepository = bundleRepository;
        _wipLabelProvider = wipLabelProvider;
        _sender = sender;
        _logger = logger;
    }

    public async Task<ManualStationContext> GetContextAsync(ManualTagStation station, string ndtBatchNo, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(ndtBatchNo))
            throw new ArgumentException("NdtBatchNo is required.", nameof(ndtBatchNo));

        var batch = ndtBatchNo.Trim();
        var state = await LoadOrCreateStateAsync(batch, cancellationToken).ConfigureAwait(false);
        var incoming = GetIncomingForStation(state, station);
        var existing = GetExistingForStation(state, station);

        return new ManualStationContext
        {
            NdtBatchNo = batch,
            PoNumber = state.PoNumber,
            MillNo = state.MillNo,
            IncomingPcs = incoming,
            AlreadyOkPcs = existing?.OkPcs,
            AlreadyRejectedPcs = existing?.RejectedPcs,
            OutgoingPcs = existing?.OkPcs
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

        if (request.OkPcs + request.RejectedPcs != incoming)
            throw new InvalidOperationException($"OK ({request.OkPcs}) + Rejected ({request.RejectedPcs}) must equal Incoming ({incoming}).");

        var now = DateTime.Now;
        var start = request.StartTime ?? now;
        var end = request.EndTime ?? now;

        SetStationRecord(state, request.Station, new StationRecord
        {
            OkPcs = request.OkPcs,
            RejectedPcs = request.RejectedPcs,
            User = string.IsNullOrWhiteSpace(request.User) ? Environment.UserName : request.User.Trim(),
            StartTime = start,
            EndTime = end
        });
        await SaveStateAsync(state, cancellationToken).ConfigureAwait(false);

        var folder = GetFolder(request.Station);
        Directory.CreateDirectory(folder);
        var csvPath = await WriteCsvAsync(folder, request.Station, state.PoNumber, batch, incoming, request.OkPcs, request.RejectedPcs, state.GetRecord(request.Station)!.User, start, end, cancellationToken).ConfigureAwait(false);

        var printed = false;
        if (request.PrintTag)
        {
            printed = await TryPrintTagAsync(state.PoNumber, state.MillNo, batch, request.OkPcs, now, request.Station, cancellationToken).ConfigureAwait(false);
        }

        return new ManualStationRecordResult
        {
            NdtBatchNo = batch,
            Station = request.Station,
            IncomingPcs = incoming,
            OkPcs = request.OkPcs,
            RejectedPcs = request.RejectedPcs,
            OutgoingPcs = request.OkPcs,
            CsvPath = csvPath,
            Printed = printed
        };
    }

    private string GetFolder(ManualTagStation station)
    {
        return station switch
        {
            ManualTagStation.Visual => (_options.VisualNdtOutputFolder ?? string.Empty).Trim(),
            ManualTagStation.Hydrotesting or ManualTagStation.FourHeadHydrotesting or ManualTagStation.BigHydrotesting =>
                (_options.HydrotestingNdtOutputFolder ?? string.Empty).Trim(),
            ManualTagStation.Revisual => (_options.RevisualNdtOutputFolder ?? string.Empty).Trim(),
            _ => (_options.OutputBundleFolder ?? string.Empty).Trim()
        };
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

    private async Task<string> WriteCsvAsync(string folder, ManualTagStation station, string poNumber, string ndtBatchNo, int incomingPcs, int okPcs, int rejectedPcs, string user, DateTime start, DateTime end, CancellationToken cancellationToken)
    {
        var stamp = DateTime.Now.ToString("yyyyMMddHHmmss", CultureInfo.InvariantCulture);
        var fileName = $"{StationName(station)}_NDT_{ndtBatchNo}_{stamp}.csv";
        var path = Path.Combine(folder, fileName);

        // CSV header layout to match spreadsheet style (grouped station header over OK/REJECTION/USER)
        var stationHeader = $"{StationName(station).ToUpperInvariant()} INSPECTION";
        var header1 = $"PO Number,NDT BATCH NO,NDT Pcs,{stationHeader},,,Start Time,End Time";
        var header2 = $",,,OK,REJECTION,USER,,";
        var row = string.Join(",",
            Escape(poNumber.Trim()),
            Escape(ndtBatchNo),
            incomingPcs.ToString(CultureInfo.InvariantCulture),
            okPcs.ToString(CultureInfo.InvariantCulture),
            rejectedPcs.ToString(CultureInfo.InvariantCulture),
            Escape(user),
            Escape(start.ToString("O", CultureInfo.InvariantCulture)),
            Escape(end.ToString("O", CultureInfo.InvariantCulture))
        );

        await File.WriteAllLinesAsync(path, new[] { header1, header2, row }, cancellationToken).ConfigureAwait(false);
        _logger.LogInformation("Wrote manual station CSV: {Path}", path);
        return path;
    }

    private async Task<bool> TryPrintTagAsync(string poNumber, int millNo, string ndtBatchNo, int pcs, DateTime date, ManualTagStation station, CancellationToken cancellationToken)
    {
        if (!_options.EnableNdtTagZplAndPrint)
        {
            _logger.LogDebug("NDT tag ZPL and network print are disabled (NdtBundle:EnableNdtTagZplAndPrint); CSV only.");
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
            isReprint: false);

        await TrySaveZplPreviewAsync(station, ndtBatchNo, zplBytes, cancellationToken).ConfigureAwait(false);
        return await _sender.SendAsync(address, _options.NdtTagPrinterPort, zplBytes, cancellationToken).ConfigureAwait(false);
    }

    private async Task TrySaveZplPreviewAsync(ManualTagStation station, string ndtBatchNo, byte[] zplBytes, CancellationToken cancellationToken)
    {
        try
        {
            var folder = GetFolder(station);
            if (string.IsNullOrWhiteSpace(folder))
                return;

            Directory.CreateDirectory(folder);
            var fileName = $"{StationName(station)}_NDTTag_{ndtBatchNo}_{DateTime.Now:yyyyMMddHHmmss}.zpl";
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

