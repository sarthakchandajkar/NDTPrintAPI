namespace NdtBundleService.Configuration;

/// <summary>NdtBundle:CloseTrigger — what drives size-based bundle close/print.</summary>
public enum BundleCloseTrigger
{
    File = 0,
    Plc = 1,
    PlcWithFileFallback = 2
}

public static class BundleCloseTriggerParser
{
    public static BundleCloseTrigger Parse(string? value)
    {
        if (string.Equals(value, "File", StringComparison.OrdinalIgnoreCase))
            return BundleCloseTrigger.File;
        if (string.Equals(value, "Plc", StringComparison.OrdinalIgnoreCase))
            return BundleCloseTrigger.Plc;
        return BundleCloseTrigger.PlcWithFileFallback;
    }

    public static string ToConfigValue(BundleCloseTrigger value) => value switch
    {
        BundleCloseTrigger.File => "File",
        BundleCloseTrigger.Plc => "Plc",
        _ => "PlcWithFileFallback"
    };
}

/// <summary>NdtBundle:HooterCountSource — MW56 feed.</summary>
public enum HooterCountSource
{
    App = 0,
    Plc = 1
}

public static class HooterCountSourceParser
{
    public static HooterCountSource Parse(string? value) =>
        string.Equals(value, "Plc", StringComparison.OrdinalIgnoreCase)
            ? HooterCountSource.Plc
            : HooterCountSource.App;
}
