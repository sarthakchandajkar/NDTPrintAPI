namespace NdtBundleService.Configuration;

/// <summary>Password-protected dashboard settings (formation chart, printers). Set <see cref="AdminPassword"/> in production.</summary>
public sealed class DashboardSettingsOptions
{
    /// <summary>When empty, the Settings API is disabled until configured (env: NdtBundle__DashboardSettings__AdminPassword).</summary>
    public string AdminPassword { get; set; } = string.Empty;

    /// <summary>Hours a successful login token remains valid.</summary>
    public int SessionHours { get; set; } = 8;
}
