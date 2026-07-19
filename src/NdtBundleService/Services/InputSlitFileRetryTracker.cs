using NdtBundleService.Configuration;

namespace NdtBundleService.Services;

/// <summary>
/// Parks Input Slit files that deferred bundling so other files can proceed (F-3 backoff).
/// Used for <see cref="MillPoEndSource.Plc"/> mills only; File mills keep immediate next-poll retry.
/// </summary>
public sealed class InputSlitFileRetryTracker
{
    private sealed class State
    {
        public int StepIndex;
        public DateTime NextAttemptUtc;
        public int LastLoggedStep = -1;
    }

    private readonly Dictionary<string, State> _states = new(StringComparer.OrdinalIgnoreCase);
    private readonly object _lock = new();

    /// <summary>True when the file is still within its backoff window and should be skipped this poll.</summary>
    public bool ShouldSkip(string fileFullPath, DateTime utcNow)
    {
        lock (_lock)
        {
            return _states.TryGetValue(fileFullPath, out var st) && utcNow < st.NextAttemptUtc;
        }
    }

    /// <summary>
    /// Parks the file with the next backoff step. Returns the delay applied and whether a new log line should be emitted
    /// (one line per backoff step).
    /// </summary>
    public (TimeSpan Delay, bool ShouldLog, int StepIndex) Park(
        string fileFullPath,
        DateTime utcNow,
        IReadOnlyList<int> backoffSeconds)
    {
        var steps = NormalizeSteps(backoffSeconds);
        lock (_lock)
        {
            if (!_states.TryGetValue(fileFullPath, out var st))
            {
                st = new State();
                _states[fileFullPath] = st;
            }

            var step = Math.Clamp(st.StepIndex, 0, steps.Count - 1);
            var delay = TimeSpan.FromSeconds(steps[step]);
            st.NextAttemptUtc = utcNow.Add(delay);
            var shouldLog = st.LastLoggedStep != step;
            if (shouldLog)
                st.LastLoggedStep = step;

            if (st.StepIndex < steps.Count - 1)
                st.StepIndex++;

            return (delay, shouldLog, step);
        }
    }

    public void Clear(string fileFullPath)
    {
        lock (_lock)
        {
            _states.Remove(fileFullPath);
        }
    }

    public static IReadOnlyList<int> NormalizeSteps(IReadOnlyList<int>? backoffSeconds)
    {
        if (backoffSeconds is null || backoffSeconds.Count == 0)
            return [5, 30, 120];

        var steps = backoffSeconds.Where(s => s > 0).ToList();
        return steps.Count > 0 ? steps : [5, 30, 120];
    }
}
