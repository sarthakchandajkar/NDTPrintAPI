using Microsoft.AspNetCore.Mvc;
using NdtBundleService.Services;

namespace NdtBundleService.Controllers;

[ApiController]
[Route("api/[controller]")]
[Produces("application/json")]
public sealed class ManualTagsController : ControllerBase
{
    private readonly IManualNdtTagService _service;
    private readonly ILogger<ManualTagsController> _logger;

    public ManualTagsController(IManualNdtTagService service, ILogger<ManualTagsController> logger)
    {
        _service = service;
        _logger = logger;
    }

    [HttpGet("{station}/{ndtBatchNo}/context")]
    public async Task<IActionResult> GetContext(string station, string ndtBatchNo, [FromQuery] int operatorStationNumber = 1, CancellationToken cancellationToken = default)
    {
        if (!TryParseStation(station, out var st))
            return BadRequest(new { Message = "Invalid station. Use Visual, Hydrotesting, FourHeadHydrotesting, BigHydrotesting, or Revisual." });

        try
        {
            var ctx = await _service.GetContextAsync(st, ndtBatchNo, operatorStationNumber, cancellationToken).ConfigureAwait(false);

            return Ok(new
            {
                Station = station,
                ctx.NdtBatchNo,
                ctx.PoNumber,
                ctx.MillNo,
                ctx.OperatorStationNumber,
                ctx.IncomingPcs,
                ctx.AlreadyOkPcs,
                ctx.AlreadyRejectedPcs,
                ctx.OutgoingPcs,
                ctx.HydroRedoRequired,
                ctx.RevisualRedoRequired,
                ctx.HasRecordedThisStation
            });
        }
        catch (InvalidOperationException ex)
        {
            return BadRequest(new { Message = ex.Message });
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { Message = ex.Message });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Manual station context failed for station {Station}.", station);
            return StatusCode(500, new { Message = ex.Message });
        }
    }

    [HttpPost("{station}/record")]
    public async Task<IActionResult> Record(string station, [FromBody] RecordManualStationRequest request, CancellationToken cancellationToken)
    {
        if (!TryParseStation(station, out var st))
            return BadRequest(new { Message = "Invalid station. Use Visual, Hydrotesting, FourHeadHydrotesting, BigHydrotesting, or Revisual." });

        if (request is null)
            return BadRequest(new { Message = "Request body is required." });

        try
        {
            var result = await _service.RecordAsync(new ManualStationRecordRequest
            {
                Station = st,
                NdtBatchNo = request.NdtBatchNo ?? string.Empty,
                OkPcs = request.OkPcs,
                RejectedPcs = request.RejectedPcs,
                User = request.User ?? string.Empty,
                StartTime = request.StartTime,
                EndTime = request.EndTime,
                PrintTag = request.PrintTag,
                OperatorStationNumber = request.OperatorStationNumber <= 0 ? 1 : request.OperatorStationNumber
            }, cancellationToken).ConfigureAwait(false);

            return Ok(new
            {
                Message = st == ManualTagStation.Revisual
                    ? "Station recorded; consolidated NDT process CSV generated."
                    : "Station recorded.",
                Station = station,
                result.NdtBatchNo,
                result.IncomingPcs,
                result.OkPcs,
                result.RejectedPcs,
                result.OutgoingPcs,
                result.Printed,
                result.CsvPath
            });
        }
        catch (InvalidOperationException ex)
        {
            return BadRequest(new { Message = ex.Message });
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { Message = ex.Message });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Manual station record failed for station {Station}.", station);
            return StatusCode(500, new { Message = ex.Message });
        }
    }

    [HttpPost("{station}/reconcile")]
    public async Task<IActionResult> Reconcile(string station, [FromBody] RecordManualStationRequest request, CancellationToken cancellationToken)
    {
        if (!TryParseStation(station, out var st))
            return BadRequest(new { Message = "Invalid station. Use Visual, Hydrotesting, FourHeadHydrotesting, BigHydrotesting, or Revisual." });

        if (request is null)
            return BadRequest(new { Message = "Request body is required." });

        try
        {
            var result = await _service.ReconcileAsync(new ManualStationRecordRequest
            {
                Station = st,
                NdtBatchNo = request.NdtBatchNo ?? string.Empty,
                OkPcs = request.OkPcs,
                RejectedPcs = request.RejectedPcs,
                User = request.User ?? string.Empty,
                StartTime = request.StartTime,
                EndTime = request.EndTime,
                PrintTag = request.PrintTag,
                OperatorStationNumber = request.OperatorStationNumber <= 0 ? 1 : request.OperatorStationNumber
            }, cancellationToken).ConfigureAwait(false);

            return Ok(new
            {
                Message = st == ManualTagStation.Revisual
                    ? "Station reconciled; consolidated NDT process CSV replaced."
                    : "Station reconciled. Downstream steps may need to be re-entered if the flow was reset.",
                Station = station,
                result.NdtBatchNo,
                result.IncomingPcs,
                result.OkPcs,
                result.RejectedPcs,
                result.OutgoingPcs,
                result.Printed,
                result.CsvPath
            });
        }
        catch (InvalidOperationException ex)
        {
            return BadRequest(new { Message = ex.Message });
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { Message = ex.Message });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Manual station reconcile failed for station {Station}.", station);
            return StatusCode(500, new { Message = ex.Message });
        }
    }

    private static bool TryParseStation(string raw, out ManualTagStation station)
    {
        station = ManualTagStation.Visual;
        if (string.IsNullOrWhiteSpace(raw)) return false;
        var v = raw.Trim();
        if (v.Equals("Visual", StringComparison.OrdinalIgnoreCase)) { station = ManualTagStation.Visual; return true; }
        if (v.Equals("Hydrotesting", StringComparison.OrdinalIgnoreCase)) { station = ManualTagStation.Hydrotesting; return true; }
        if (v.Equals("FourHeadHydrotesting", StringComparison.OrdinalIgnoreCase)) { station = ManualTagStation.FourHeadHydrotesting; return true; }
        if (v.Equals("BigHydrotesting", StringComparison.OrdinalIgnoreCase)) { station = ManualTagStation.BigHydrotesting; return true; }
        if (v.Equals("Revisual", StringComparison.OrdinalIgnoreCase)) { station = ManualTagStation.Revisual; return true; }
        return false;
    }

    public sealed class RecordManualStationRequest
    {
        public string? NdtBatchNo { get; set; }
        public int OkPcs { get; set; }
        public int RejectedPcs { get; set; }
        public string? User { get; set; }
        public DateTime? StartTime { get; set; }
        public DateTime? EndTime { get; set; }
        public bool PrintTag { get; set; }
        /// <summary>For Visual and Revisual: 1 or 2. Omitted or 0 defaults to 1.</summary>
        public int OperatorStationNumber { get; set; }
    }
}

