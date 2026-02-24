using Microsoft.Data.SqlClient;
using NDTPrintApi.Core.Models;

namespace NDTPrintApi.Services;

/// <summary>
/// Full NDT print flow: validate, load bundle from DB, resolve printer, run report via INdtReportPrinter, update DB, optional CSV.
/// Assumed schema: NDT bundles in table M{MillNo}_NDTBundles (e.g. M1_NDTBundles); PlantDevice has DeviceAbbr, DeviceName.
/// </summary>
public class NdtPrintService : INdtPrintService
{
    private const int MillNoMin = 1;
    private const int MillNoMax = 4;

    // Assumed column names for M{MillNo}_NDTBundles: NDTBundle_ID, Bundle_No, PO_Plan_ID, Slit_ID, Batch_No, NDT_Pcs, Status, OprDoneTime, LastReprintDttm
    // Assumed PlantDevice columns: DeviceAbbr, DeviceName

    private readonly IConfiguration _configuration;
    private readonly ILogger<NdtPrintService> _logger;
    private readonly INdtReportPrinter _reportPrinter;

    public NdtPrintService(IConfiguration configuration, ILogger<NdtPrintService> logger, INdtReportPrinter reportPrinter)
    {
        _configuration = configuration;
        _logger = logger;
        _reportPrinter = reportPrinter;
    }

    public async Task<NdtPrintResponse> PrintAsync(NdtPrintRequest request, CancellationToken cancellationToken = default)
    {
        // 1) Validate request
        var validationError = ValidateRequest(request);
        if (validationError != null)
            return validationError;

        var connectionString = GetConnectionString();
        if (string.IsNullOrEmpty(connectionString))
            return new NdtPrintResponse { Success = false, Message = "Database connection string is not configured." };

        // 2) Load NDT bundle from DB
        var bundle = await LoadNdtBundleAsync(connectionString, request, cancellationToken).ConfigureAwait(false);
        if (bundle == null)
            return new NdtPrintResponse { Success = false, Message = "NDT bundle not found." };

        // 3) Get printer name for the mill
        var printerName = await GetWipPrinterNameAsync(connectionString, request.MillNo, cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrEmpty(printerName))
            return new NdtPrintResponse { Success = false, Message = "WIP printer not configured for this mill." };

        // 4) Generate NDT tag and send to printer (via INdtReportPrinter; use Telerik implementation when available)
        try
        {
            await _reportPrinter.PrintAsync(connectionString, bundle.NdtBundleId, request.MillNo, request.Reprint, printerName, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "NDT print failed for BundleNo={BundleNo}, MillNo={MillNo}", request.BundleNo, request.MillNo);
            return new NdtPrintResponse { Success = false, Message = "Print failed: " + ex.Message };
        }

        // 5) Update DB after successful print
        await UpdateDbAfterPrintAsync(connectionString, request, bundle, cancellationToken).ConfigureAwait(false);

        // 6) Optional: write CSV for SAP
        await TryWriteSapCsvAsync(bundle, request.MillNo, cancellationToken).ConfigureAwait(false);

        // 7) Return success response
        var printTime = DateTime.UtcNow;
        return new NdtPrintResponse
        {
            Success = true,
            Message = "NDT tag printed successfully",
            BundleNo = request.BundleNo,
            MillNo = request.MillNo,
            PrintTime = printTime,
            Reprint = request.Reprint
        };
    }

    private static NdtPrintResponse? ValidateRequest(NdtPrintRequest request)
    {
        if (request == null)
            return new NdtPrintResponse { Success = false, Message = "Request body is required." };
        if (string.IsNullOrWhiteSpace(request.BundleNo))
            return new NdtPrintResponse { Success = false, Message = "BundleNo is required." };
        if (request.MillNo < MillNoMin || request.MillNo > MillNoMax)
            return new NdtPrintResponse { Success = false, Message = $"MillNo must be between {MillNoMin} and {MillNoMax}." };
        return null;
    }

    private string? GetConnectionString()
    {
        return _configuration.GetConnectionString("DefaultConnection")
            ?? _configuration.GetConnectionString("ServerConnectionString");
    }

    private static string GetNdtBundlesTableName(int millNo)
    {
        // MillNo validated 1–4; table name pattern M{MillNo}_NDTBundles
        return "M" + millNo + "_NDTBundles";
    }

    private async Task<NdtBundleRow?> LoadNdtBundleAsync(string connectionString, NdtPrintRequest request, CancellationToken cancellationToken)
    {
        var tableName = GetNdtBundlesTableName(request.MillNo);
        // Assumed columns: NDTBundle_ID, Bundle_No, PO_Plan_ID, Slit_ID, Batch_No, NDT_Pcs, Status, OprDoneTime, LastReprintDttm
        string sql;
        SqlParameter[] parameters;
        if (request.NdtBundleId.HasValue)
        {
            sql = $@"SELECT NDTBundle_ID, Bundle_No, PO_Plan_ID, Slit_ID, Batch_No, NDT_Pcs, Status, OprDoneTime, LastReprintDttm
FROM [{tableName}] WITH (NOLOCK) WHERE NDTBundle_ID = @NdtBundleId";
            parameters = new[] { new SqlParameter("@NdtBundleId", request.NdtBundleId.Value) };
        }
        else
        {
            // Table M{MillNo}_NDTBundles is per-mill, so filter by Bundle_No only (no Mill_No column assumed)
            sql = $@"SELECT NDTBundle_ID, Bundle_No, PO_Plan_ID, Slit_ID, Batch_No, NDT_Pcs, Status, OprDoneTime, LastReprintDttm
FROM [{tableName}] WITH (NOLOCK) WHERE Bundle_No = @BundleNo";
            parameters = new[] { new SqlParameter("@BundleNo", request.BundleNo.Trim()) };
        }

        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddRange(parameters);
        await using var reader = await cmd.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            return null;

        return new NdtBundleRow
        {
            NdtBundleId = reader.GetInt32(reader.GetOrdinal("NDTBundle_ID")),
            BundleNo = reader.GetString(reader.GetOrdinal("Bundle_No")),
            PoPlanId = reader.IsDBNull(reader.GetOrdinal("PO_Plan_ID")) ? null : reader.GetInt32(reader.GetOrdinal("PO_Plan_ID")),
            SlitId = reader.IsDBNull(reader.GetOrdinal("Slit_ID")) ? null : reader.GetInt32(reader.GetOrdinal("Slit_ID")),
            BatchNo = reader.IsDBNull(reader.GetOrdinal("Batch_No")) ? null : reader.GetString(reader.GetOrdinal("Batch_No")),
            NdtPcs = reader.IsDBNull(reader.GetOrdinal("NDT_Pcs")) ? null : reader.GetInt32(reader.GetOrdinal("NDT_Pcs")),
            Status = reader.IsDBNull(reader.GetOrdinal("Status")) ? null : reader.GetString(reader.GetOrdinal("Status")),
            OprDoneTime = reader.IsDBNull(reader.GetOrdinal("OprDoneTime")) ? null : reader.GetDateTime(reader.GetOrdinal("OprDoneTime")),
            LastReprintDttm = reader.IsDBNull(reader.GetOrdinal("LastReprintDttm")) ? null : reader.GetDateTime(reader.GetOrdinal("LastReprintDttm"))
        };
    }

    private async Task<string?> GetWipPrinterNameAsync(string connectionString, int millNo, CancellationToken cancellationToken)
    {
        // PlantDevice: DeviceAbbr = 'M' + MillNo + 'WIPPrinter', select DeviceName
        var deviceAbbr = "M" + millNo + "WIPPrinter";
        const string sql = @"SELECT DeviceName FROM PlantDevice WITH (NOLOCK) WHERE DeviceAbbr = @DeviceAbbr";
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@DeviceAbbr", deviceAbbr);
        var value = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return value?.ToString();
    }

    private async Task UpdateDbAfterPrintAsync(string connectionString, NdtPrintRequest request, NdtBundleRow bundle, CancellationToken cancellationToken)
    {
        var tableName = GetNdtBundlesTableName(request.MillNo);
        string sql;
        if (request.Reprint)
        {
            sql = $@"UPDATE [{tableName}] SET LastReprintDttm = GETDATE() WHERE NDTBundle_ID = @NdtBundleId";
        }
        else
        {
            // Status e.g. 'Printed'; column name may vary per schema
            sql = $@"UPDATE [{tableName}] SET Status = 'Printed', OprDoneTime = GETDATE() WHERE NDTBundle_ID = @NdtBundleId";
        }

        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@NdtBundleId", bundle.NdtBundleId);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task TryWriteSapCsvAsync(NdtBundleRow bundle, int millNo, CancellationToken cancellationToken)
    {
        var csvPath = _configuration["NdtPrint:SapCsvPath"];
        if (string.IsNullOrWhiteSpace(csvPath))
            return;

        try
        {
            var directory = Path.GetDirectoryName(csvPath);
            if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                Directory.CreateDirectory(directory);

            // Columns: Batch No, PO No, Mill No, Slit No, NDT Count (per NDT SOW)
            var line = string.Join(",",
                CsvEscape(bundle.BatchNo ?? ""),
                CsvEscape(bundle.PoPlanId?.ToString() ?? ""),
                millNo.ToString(),
                CsvEscape(bundle.SlitId?.ToString() ?? ""),
                (bundle.NdtPcs ?? 0).ToString());
            var contents = "Batch No,PO No,Mill No,Slit No,NDT Count" + Environment.NewLine + line + Environment.NewLine;

            await File.WriteAllTextAsync(csvPath, contents, cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("SAP CSV written to {Path}", csvPath);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to write SAP CSV to {Path}; print succeeded.", csvPath);
        }
    }

    private static string CsvEscape(string value)
    {
        if (value.IndexOfAny(new[] { ',', '"', '\r', '\n' }) >= 0)
            return "\"" + value.Replace("\"", "\"\"") + "\"";
        return value;
    }

    private sealed class NdtBundleRow
    {
        public int NdtBundleId { get; set; }
        public string BundleNo { get; set; } = string.Empty;
        public int? PoPlanId { get; set; }
        public int? SlitId { get; set; }
        public string? BatchNo { get; set; }
        public int? NdtPcs { get; set; }
        public string? Status { get; set; }
        public DateTime? OprDoneTime { get; set; }
        public DateTime? LastReprintDttm { get; set; }
    }
}
