using Microsoft.Extensions.Options;
using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

public sealed class ZplGenerationToggle : IZplGenerationToggle
{
    private volatile bool _enabled;

    public ZplGenerationToggle(IOptions<NdtBundleOptions> options)
    {
        _enabled = options.Value.EnableNdtTagZplAndPrint;
    }

    public bool IsEnabled => _enabled;

    public bool SetEnabled(bool enabled)
    {
        _enabled = enabled;
        return _enabled;
    }
}
