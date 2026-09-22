namespace NdtBundleService.Configuration;

/// <summary>
/// Shared → mill HTTP forward for Settings PLC writes (connect / disconnect / test PO-change / open accumulation).
/// Mills keep the S7 handshake; Shared has no PLC loop.
/// </summary>
public sealed class MillInstanceProxyOptions
{
    /// <summary>When true on Shared, Settings PLC write endpoints forward to mill base URLs.</summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Base URLs keyed by mill number (<c>"1"</c>…<c>"4"</c>), e.g. <c>http://127.0.0.1:5001</c>.
    /// </summary>
    public Dictionary<string, string> BaseUrls { get; set; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>HTTP timeout for login + forwarded Settings calls.</summary>
    public int TimeoutSeconds { get; set; } = 45;

    public bool TryGetBaseUrl(int millNo, out Uri? baseUri)
    {
        baseUri = null;
        if (millNo is < 1 or > 4)
            return false;
        if (!BaseUrls.TryGetValue(millNo.ToString(), out var raw) || string.IsNullOrWhiteSpace(raw))
            return false;
        if (!Uri.TryCreate(raw.Trim().TrimEnd('/') + "/", UriKind.Absolute, out var uri))
            return false;
        baseUri = uri;
        return true;
    }
}
