using System.Globalization;
using Microsoft.Data.SqlClient;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

internal static class PoPlanWipRowMapper
{
    internal const string SelectColumns = @"
PO_Plan_WIP_ID,
PO_Number,
Mill_No,
Planned_Month,
Pipe_Grade,
Pipe_Size,
Pipe_Thickness,
Pipe_Length,
Pipe_Weight_Per_Meter,
Pipe_Type,
Output_Itemcode,
Item_Description,
Product_Type,
PO_Specification,
Input_WIP_Itemcode,
Pieces_Per_Bundle,
NDTPcsPerBundle,
Total_Pieces,
Source_File,
ImportedAtUtc";

    internal static PoPlanWipRow Read(SqlDataReader reader)
    {
        static string Str(SqlDataReader r, string name)
        {
            var ord = r.GetOrdinal(name);
            return r.IsDBNull(ord) ? string.Empty : r.GetString(ord).Trim();
        }

        static string IntOrStr(SqlDataReader r, string name)
        {
            var ord = r.GetOrdinal(name);
            if (r.IsDBNull(ord))
                return string.Empty;
            return r.GetFieldType(ord) == typeof(int)
                ? r.GetInt32(ord).ToString(CultureInfo.InvariantCulture)
                : r.GetString(ord).Trim();
        }

        return new PoPlanWipRow
        {
            PoNumber = InputSlitCsvParsing.NormalizePo(Str(reader, "PO_Number")),
            MillNo = reader.IsDBNull(reader.GetOrdinal("Mill_No")) ? 0 : reader.GetInt32(reader.GetOrdinal("Mill_No")),
            PlannedMonth = Str(reader, "Planned_Month"),
            PipeGrade = Str(reader, "Pipe_Grade"),
            PipeSize = Str(reader, "Pipe_Size"),
            PipeThickness = Str(reader, "Pipe_Thickness"),
            PipeLength = Str(reader, "Pipe_Length"),
            PipeWeightPerMeter = Str(reader, "Pipe_Weight_Per_Meter"),
            PipeType = Str(reader, "Pipe_Type"),
            OutputItemcode = Str(reader, "Output_Itemcode"),
            ItemDescription = Str(reader, "Item_Description"),
            ProductType = Str(reader, "Product_Type"),
            PoSpecification = Str(reader, "PO_Specification"),
            InputWipItemcode = Str(reader, "Input_WIP_Itemcode"),
            PiecesPerBundle = IntOrStr(reader, "Pieces_Per_Bundle"),
            NdtPcsPerBundle = IntOrStr(reader, "NDTPcsPerBundle"),
            TotalPieces = Str(reader, "Total_Pieces")
        };
    }

    internal static WipLabelInfo ToWipLabel(PoPlanWipRow row) =>
        new()
        {
            PipeGrade = row.PipeGrade,
            PipeSize = row.PipeSize,
            PipeThickness = row.PipeThickness,
            PipeLength = row.PipeLength,
            PipeWeightPerMeter = row.PipeWeightPerMeter,
            PipeType = row.PipeType
        };
}
