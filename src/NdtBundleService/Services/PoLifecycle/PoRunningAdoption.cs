namespace NdtBundleService.Services.PoLifecycle;

/// <summary>WIP-confirmed running PO detection (reopen executes only from WIP, never slit alone).</summary>
public static class PoRunningAdoption
{
    /// <summary>True when WIP has confirmed this PO as the mill's running PO.</summary>
    public static bool IsWipConfirmedRunning(string poNumber, string? wipRunningPo)
    {
        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        if (string.IsNullOrWhiteSpace(po))
            return false;

        return !string.IsNullOrWhiteSpace(wipRunningPo)
               && InputSlitCsvParsing.PoEquals(po, wipRunningPo);
    }
}
