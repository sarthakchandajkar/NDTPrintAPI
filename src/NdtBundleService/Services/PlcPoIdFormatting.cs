using System.Globalization;

namespace NdtBundleService.Services;

/// <summary>Formats PLC PO_Id INT values into MES PO number strings.</summary>
public static class PlcPoIdFormatting
{
    public static string Format(int poId, string? format)
    {
        if (string.IsNullOrWhiteSpace(format))
            return poId.ToString(CultureInfo.InvariantCulture);

        try
        {
            return string.Format(CultureInfo.InvariantCulture, format, poId);
        }
        catch (FormatException)
        {
            return poId.ToString(CultureInfo.InvariantCulture);
        }
    }
}
