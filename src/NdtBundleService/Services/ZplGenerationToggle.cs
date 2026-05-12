using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

public sealed class ZplGenerationToggle : IZplGenerationToggle
{
    private readonly IOptionsMonitor<NdtBundleOptions> _options;
    private bool? _manualOverride;

    public ZplGenerationToggle(IOptionsMonitor<NdtBundleOptions> options)
    {
        _options = options;
    }

    /// <summary>Effective ZPL/print flag: dashboard override if set, otherwise <see cref="NdtBundleOptions.EnableNdtTagZplAndPrint"/> from configuration.</summary>
    public bool IsEnabled => _manualOverride ?? _options.CurrentValue.EnableNdtTagZplAndPrint;

    public bool SetEnabled(bool enabled)
    {
        _manualOverride = enabled;
        return enabled;
    }
}
