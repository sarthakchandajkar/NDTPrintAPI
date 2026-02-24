using NDTPrintApi.Core.Models;

namespace NDTPrintApi.Services;

/// <summary>
/// Service for printing NDT pipe bundle tags.
/// </summary>
public interface INdtPrintService
{
    /// <summary>
    /// Processes an NDT print request (validate, then perform print and DB update).
    /// </summary>
    /// <param name="request">The print request.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The print result with success/error details.</returns>
    Task<NdtPrintResponse> PrintAsync(NdtPrintRequest request, CancellationToken cancellationToken = default);
}
