namespace NdtBundleService.Services;

/// <summary>
/// Operator-selected reprint destination for Printed Tags / Reconcile flows.
/// Values: <c>wip</c> (Mill-1 WIP), <c>fg</c> (Mill-1 FG), <c>pas</c> (PAS room),
/// <c>mill</c> / null (default mill close-tag printer).
/// </summary>
public static class ReprintPrinterTarget
{
    public const string Wip = "wip";
    public const string Fg = "fg";
    public const string Pas = "pas";
    public const string Mill = "mill";

    public static string Normalize(string? target)
    {
        var t = (target ?? string.Empty).Trim().ToLowerInvariant();
        return t switch
        {
            Wip or "mill1_wip" or "mill_1_wip" => Wip,
            Fg or "mill1_fg" or "mill_1_fg" => Fg,
            Pas or "pas_room" or "pasroom" => Pas,
            Mill or "" or "default" => Mill,
            _ => t
        };
    }

    public static string DisplayName(string? target) =>
        Normalize(target) switch
        {
            Wip => "Mill-1 WIP printer",
            Fg => "Mill-1 FG printer",
            Pas => "PAS Room printer",
            _ => "Mill printer"
        };

    public static bool IsKnown(string? target)
    {
        var n = Normalize(target);
        return n is Wip or Fg or Pas or Mill;
    }

    /// <summary>
    /// Auto-print routing from PO plan <c>Pipe_Type</c>: Mill-1 FG → FG printer, Mill-1 otherwise → WIP;
    /// other mills use the default mill printer.
    /// </summary>
    public static string FromPipeType(int millNo, string? pipeType)
    {
        if (millNo != 1)
            return Mill;

        var type = (pipeType ?? string.Empty).Trim();
        if (type.Equals("FG", StringComparison.OrdinalIgnoreCase))
            return Fg;

        return Wip;
    }
}
