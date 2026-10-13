using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Shared SQL access for mill, station, and PAS-room ZPL printers in <c>dbo.Printer</c>.
/// Keys: <c>MILL_1</c>…<c>MILL_4</c>, <c>MILL_1_WIP</c>, <c>MILL_1_FG</c>, <c>PAS_ROOM</c>,
/// <c>VISUAL_REVISUAL</c>, <c>BIG_HYDRO</c>, <c>FOUR_HEAD_HYDRO</c>.
/// </summary>
internal static class SqlPrinterTable
{
    internal const string KindMill = "Mill";
    internal const string KindStation = "Station";
    internal const string KindPasRoom = "PasRoom";

    internal const string KeyMill1Wip = "MILL_1_WIP";
    internal const string KeyMill1Fg = "MILL_1_FG";
    internal const string KeyPasRoom = "PAS_ROOM";

    internal static string MillKey(int millNo) => $"MILL_{millNo}";

    internal static bool TryParseMillKey(string? printerKey, out int millNo)
    {
        millNo = 0;
        var key = (printerKey ?? string.Empty).Trim().ToUpperInvariant();
        if (key.Length != 6 || !key.StartsWith("MILL_", StringComparison.Ordinal))
            return false;

        return int.TryParse(
                   key.AsSpan(5),
                   System.Globalization.NumberStyles.Integer,
                   System.Globalization.CultureInfo.InvariantCulture,
                   out millNo) &&
               millNo is >= 1 and <= 4;
    }

    internal static IReadOnlyList<(string Key, string Address, int Port)> Load(
        IOptionsMonitor<NdtBundleOptions> options,
        ILogger logger,
        string kind)
    {
        var list = new List<(string Key, string Address, int Port)>();
        try
        {
            using var conn = SqlTraceabilityConnection.Create(options.CurrentValue);
            conn.Open();
            using var cmd = new SqlCommand(
                "SELECT Printer_Key, Address, Port FROM dbo.Printer WHERE Kind = @Kind;",
                conn);
            cmd.Parameters.AddWithValue("@Kind", kind);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                list.Add((
                    reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                    reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                    reader.GetInt32(2)));
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to load Printer rows (Kind={Kind}).", kind);
        }

        return list;
    }

    internal static (string Address, int Port)? TryLoadKey(
        IOptionsMonitor<NdtBundleOptions> options,
        ILogger logger,
        string printerKey)
    {
        try
        {
            using var conn = SqlTraceabilityConnection.Create(options.CurrentValue);
            conn.Open();
            using var cmd = new SqlCommand(
                "SELECT Address, Port FROM dbo.Printer WHERE Printer_Key = @Key;",
                conn);
            cmd.Parameters.AddWithValue("@Key", printerKey);
            using var reader = cmd.ExecuteReader();
            if (!reader.Read())
                return null;
            return (
                reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                reader.GetInt32(1));
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to load Printer key {Key}.", printerKey);
            return null;
        }
    }

    internal static void Upsert(
        IOptionsMonitor<NdtBundleOptions> options,
        IReadOnlyList<(string Key, string Kind, string Address, int Port)> rows)
    {
        using var conn = SqlTraceabilityConnection.Create(options.CurrentValue);
        conn.Open();
        foreach (var row in rows)
        {
            using var cmd = new SqlCommand(@"
MERGE dbo.Printer WITH (HOLDLOCK) AS t
USING (SELECT @Key AS Printer_Key) AS s
ON t.Printer_Key = s.Printer_Key
WHEN MATCHED THEN UPDATE SET
    Kind = @Kind,
    Address = @Address,
    Port = @Port,
    Updated_AtUtc = SYSUTCDATETIME(),
    Updated_By = N'Dashboard'
WHEN NOT MATCHED THEN INSERT (Printer_Key, Kind, Address, Port, Updated_By)
VALUES (@Key, @Kind, @Address, @Port, N'Dashboard');", conn);
            cmd.Parameters.AddWithValue("@Key", row.Key);
            cmd.Parameters.AddWithValue("@Kind", row.Kind);
            cmd.Parameters.AddWithValue("@Address", (row.Address ?? string.Empty).Trim());
            cmd.Parameters.AddWithValue("@Port", row.Port > 0 ? row.Port : 9100);
            cmd.ExecuteNonQuery();
        }
    }
}
