using System.Globalization;
using NdtBundleService.Models;

namespace NdtBundleService.Services;

/// <summary>
/// Resolves formation chart thresholds from PO pipe-size text (handles formatting drift vs chart keys).
/// </summary>
public static class FormationChartLookup
{
    /// <summary>
    /// Normalizes pipe size from PO plan / WIP (e.g. <c>6"</c>, <c>6.0</c>, <c> 4 </c>) to keys used in the formation chart CSV.
    /// </summary>
    public static string NormalizePipeSizeKey(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return string.Empty;

        var s = raw.Trim().Trim('"').Trim();
        s = s.Replace("″", "", StringComparison.Ordinal).Replace("\"", "", StringComparison.Ordinal).Trim();
        if (s.Length == 0)
            return string.Empty;

        if (decimal.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out var d))
        {
            if (d == Math.Truncate(d) && d is >= 0 and < 10000)
                return ((int)d).ToString(CultureInfo.InvariantCulture);
            return d.ToString(CultureInfo.InvariantCulture);
        }

        return s;
    }

    public static FormationChartEntry? ResolveEntry(
        IReadOnlyDictionary<string, FormationChartEntry> formation,
        string? pipeSizeRaw)
    {
        if (formation.Count == 0)
            return null;

        var key = NormalizePipeSizeKey(pipeSizeRaw);
        if (!string.IsNullOrEmpty(key) && formation.TryGetValue(key, out var e))
            return e;

        var trimmed = pipeSizeRaw?.Trim() ?? string.Empty;
        if (trimmed.Length > 0 && formation.TryGetValue(trimmed, out e))
            return e;

        return formation.TryGetValue("Default", out e) ? e : null;
    }

    /// <summary>
    /// Same rules as <see cref="NdtBundleEngine"/>: chart entry (normalized pipe size, then raw, then Default), then minimum 10.
    /// </summary>
    public static int ResolveThreshold(IReadOnlyDictionary<string, FormationChartEntry> formation, string? pipeSizeRaw)
    {
        var entry = ResolveEntry(formation, pipeSizeRaw);
        var sizeThreshold = entry?.RequiredNdtPcs ?? 0;
        if (sizeThreshold <= 0)
            sizeThreshold = 10;
        return sizeThreshold;
    }
}
