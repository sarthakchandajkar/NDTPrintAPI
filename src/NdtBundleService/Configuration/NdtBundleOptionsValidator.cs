using Microsoft.Extensions.Options;
using NdtBundleService.Services;

namespace NdtBundleService.Configuration;

/// <summary>Rejects internally inconsistent <see cref="NdtBundleOptions"/> at startup.</summary>
public sealed class NdtBundleOptionsValidator : IValidateOptions<NdtBundleOptions>
{
    public const string MinSourceFileLastWriteUtcExample = "2026-04-05T00:00:00Z";

    public ValidateOptionsResult Validate(string? name, NdtBundleOptions options)
    {
        if (options is null)
            return ValidateOptionsResult.Fail("NdtBundle options are required.");

        if (!SourceFileEligibility.TryParseMinUtcFromRaw(options.MinSourceFileLastWriteUtc, out _))
        {
            return ValidateOptionsResult.Fail(
                FormatMinSourceFileLastWriteUtcError(options.MinSourceFileLastWriteUtc));
        }

        return ValidateOptionsResult.Success;
    }

    internal static string FormatMinSourceFileLastWriteUtcError(string? rawValue) =>
        "NdtBundle:MinSourceFileLastWriteUtc is not a valid UTC timestamp: '"
        + (rawValue ?? string.Empty)
        + "'. Use ISO-8601 (e.g. "
        + MinSourceFileLastWriteUtcExample
        + ") or leave empty for no floor.";
}
