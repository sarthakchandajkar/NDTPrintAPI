namespace NdtBundleService.Services;

/// <summary>
/// Resolves the PO used when processing an Input Slit row. The slit CSV PO is authoritative when present;
/// WIP bundle running PO is only a fallback when the slit row has no PO.
/// </summary>
internal static class SlitEffectivePoResolver
{
    internal static string Resolve(string? slitPo, string? wipRunningPo)
    {
        if (!string.IsNullOrWhiteSpace(slitPo))
            return InputSlitCsvParsing.NormalizePo(slitPo);

        if (!string.IsNullOrWhiteSpace(wipRunningPo))
            return InputSlitCsvParsing.NormalizePo(wipRunningPo);

        return string.Empty;
    }
}
