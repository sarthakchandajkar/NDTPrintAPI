namespace NdtBundleService.Services;

/// <summary>
/// Compile-time map from <see cref="ManualTagStation"/> workflows onto the three physical station printers.
/// Visual and Revisual share <see cref="VisualRevisual"/> so Settings cannot split them.
/// </summary>
public static class StationPrinterTarget
{
    public const string VisualRevisual = "VISUAL_REVISUAL";
    public const string BigHydro = "BIG_HYDRO";
    public const string FourHeadHydro = "FOUR_HEAD_HYDRO";

    public static readonly string[] All = [VisualRevisual, BigHydro, FourHeadHydro];

    public static bool IsKnown(string? stationCode) =>
        All.Contains((stationCode ?? string.Empty).Trim(), StringComparer.OrdinalIgnoreCase);

    public static string Normalize(string stationCode)
    {
        var raw = (stationCode ?? string.Empty).Trim();
        foreach (var code in All)
        {
            if (code.Equals(raw, StringComparison.OrdinalIgnoreCase))
                return code;
        }

        throw new ArgumentException($"Unknown station printer code '{stationCode}'.", nameof(stationCode));
    }

    public static string DisplayName(string stationCode) =>
        Normalize(stationCode) switch
        {
            VisualRevisual => "Visual/Revisual",
            BigHydro => "Big Hydro",
            FourHeadHydro => "Four-Head Hydro",
            _ => stationCode
        };

    public static string UnconfiguredMessage(string stationCode) =>
        $"Printer not configured for {DisplayName(stationCode)}";

    /// <summary>
    /// Maps a station workflow to a printer row. Legacy <see cref="ManualTagStation.Hydrotesting"/>
    /// maps to <see cref="BigHydro"/> and sets <paramref name="legacyHydroMapped"/>.
    /// </summary>
    public static string For(ManualTagStation station, out bool legacyHydroMapped)
    {
        legacyHydroMapped = station == ManualTagStation.Hydrotesting;
        return station switch
        {
            ManualTagStation.Visual or ManualTagStation.Revisual => VisualRevisual,
            ManualTagStation.BigHydrotesting => BigHydro,
            ManualTagStation.FourHeadHydrotesting => FourHeadHydro,
            ManualTagStation.Hydrotesting => BigHydro,
            _ => throw new ArgumentOutOfRangeException(nameof(station), station, "Unknown manual tag station.")
        };
    }

    public static string For(ManualTagStation station) => For(station, out _);
}
