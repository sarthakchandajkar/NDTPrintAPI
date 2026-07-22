namespace NdtBundleService.Services;

/// <summary>PO plan lookup helpers for slit-file resume-candidate marking.</summary>
public static class PoPlanValidation
{
    public static bool IsKnownPo(PoPlanWipEnrichmentSnapshot? snapshot, string poNumber)
    {
        if (snapshot is null)
            return false;

        var po = InputSlitCsvParsing.NormalizePo(poNumber);
        return !string.IsNullOrWhiteSpace(po)
               && snapshot.ByPo.ContainsKey(po);
    }
}
