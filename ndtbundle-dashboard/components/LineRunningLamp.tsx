export function LineRunningLamp({
  running,
  connected,
  compact,
}: {
  running: boolean | null | undefined;
  connected?: boolean;
  compact?: boolean;
}) {
  if (connected === false) {
    return (
      <span className="inline-flex items-center gap-2" title="PLC disconnected">
        <span className="h-3.5 w-3.5 rounded-full bg-gray-300 border border-gray-400 shrink-0" />
        {!compact && <span className="text-gray-400 text-xs">Offline</span>}
      </span>
    );
  }
  if (running == null) {
    return (
      <span className="inline-flex items-center gap-2" title="Line status unknown">
        <span className="h-3.5 w-3.5 rounded-full bg-gray-300 shrink-0" />
        {!compact && <span className="text-gray-400 text-xs">—</span>}
      </span>
    );
  }
  return (
    <span
      className="inline-flex items-center gap-2"
      title={running ? "Line running (DB250.DBX2.0)" : "Line stopped (DB250.DBX2.0)"}
    >
      <span
        className={`h-3.5 w-3.5 rounded-full shrink-0 border ${
          running
            ? "bg-green-500 border-green-600 shadow-[0_0_8px_2px_rgba(34,197,94,0.55)]"
            : "bg-red-500 border-red-600 shadow-[0_0_8px_2px_rgba(239,68,68,0.45)]"
        }`}
      />
      {!compact && (
        <span className={`text-xs font-semibold ${running ? "text-green-700" : "text-red-700"}`}>
          {running ? "Running" : "Stopped"}
        </span>
      )}
    </span>
  );
}
