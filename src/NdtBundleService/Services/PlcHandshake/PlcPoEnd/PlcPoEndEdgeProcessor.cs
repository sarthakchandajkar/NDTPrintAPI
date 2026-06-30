using Microsoft.Extensions.Logging;
using Serilog.Context;

namespace NdtBundleService.Services.PlcHandshake.PlcPoEnd;

/// <summary>Decoupled PO-change edge handling: ack immediately, enqueue workflow (testable without S7).</summary>
public static class PlcPoEndEdgeProcessor
{
    public sealed record EdgeProcessInput(
        int MillNo,
        string MillName,
        int PoId,
        int NdtCountFinal,
        Guid CorrelationId,
        DateTimeOffset DetectedAtUtc,
        bool StartupRecovery,
        string TriggerAddress);

    public static void ProcessDecoupledEdge(
        EdgeProcessInput input,
        Action beginAckTrue,
        Func<PlcPoEndRequest, bool> tryEnqueue,
        ILogger logger)
    {
        using (LogContext.PushProperty("CorrelationId", input.CorrelationId))
        {
            logger.LogInformation(
                "{MillName}: PO change rising edge detected on {Trigger} — Mill {MillNo}, PO_Id {PoId}, NDT {NdtCount}, CorrelationId {CorrelationId}.",
                input.MillName,
                input.TriggerAddress,
                input.MillNo,
                input.PoId,
                input.NdtCountFinal,
                input.CorrelationId);

            beginAckTrue();

            logger.LogInformation(
                "{MillName}: MES ack sent (TRUE) — Mill {MillNo}, PO_Id {PoId}, CorrelationId {CorrelationId}.",
                input.MillName,
                input.MillNo,
                input.PoId,
                input.CorrelationId);

            var request = new PlcPoEndRequest
            {
                MillNo = input.MillNo,
                PoId = input.PoId,
                NdtCountFinal = input.NdtCountFinal,
                CorrelationId = input.CorrelationId,
                DetectedAtUtc = input.DetectedAtUtc,
                StartupRecovery = input.StartupRecovery
            };

            if (tryEnqueue(request))
            {
                logger.LogInformation(
                    "{MillName}: PO end event enqueued — Mill {MillNo}, PO_Id {PoId}, CorrelationId {CorrelationId}.",
                    input.MillName,
                    input.MillNo,
                    input.PoId,
                    input.CorrelationId);
            }
            else
            {
                logger.LogWarning(
                    "{MillName}: PO end event not enqueued (mill {MillNo} already queued or processing) — PO_Id {PoId}, CorrelationId {CorrelationId}.",
                    input.MillName,
                    input.MillNo,
                    input.PoId,
                    input.CorrelationId);
            }
        }
    }
}
