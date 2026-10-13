"use client";

/** MW56 / MW58 progress toward the next NDT bundle. */
export function BundleProgressBar({
  accumulated,
  threshold,
  compact = false,
}: {
  accumulated: number | null | undefined;
  threshold: number | null | undefined;
  compact?: boolean;
}) {
  if (threshold == null || threshold <= 0 || accumulated == null) {
    return <span className="text-gray-400 text-xs">—</span>;
  }
  const pct = Math.min(100, Math.round((accumulated / threshold) * 100));
  const near = accumulated > threshold;
  return (
    <div className={compact ? "min-w-[7rem]" : "min-w-[8rem]"}>
      <div className="flex justify-between text-[10px] text-gray-500 mb-0.5 tabular-nums">
        <span>{accumulated}</span>
        <span>/ {threshold}</span>
      </div>
      <div className="h-2 rounded-full bg-gray-200 overflow-hidden">
        <div
          className={`h-full transition-all ${near ? "bg-amber-500" : "bg-primary-500"}`}
          style={{ width: `${pct}%` }}
        />
      </div>
      {near && !compact && (
        <p className="text-[10px] text-amber-700 font-medium mt-0.5">Above threshold — hooter may pulse</p>
      )}
    </div>
  );
}
