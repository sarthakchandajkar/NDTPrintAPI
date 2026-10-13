"use client";

/** Q6.7 hooter output lamp (Shared plc-live / Settings). */
export function HooterOutputBadge({ active }: { active?: boolean }) {
  return (
    <span
      className={`inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-xs font-semibold ${
        active
          ? "bg-amber-100 text-amber-900 ring-2 ring-amber-400 animate-pulse"
          : "bg-gray-100 text-gray-600"
      }`}
      title="Q6.7 hooter output"
    >
      <span className={`h-2 w-2 rounded-full ${active ? "bg-amber-500" : "bg-gray-400"}`} />
      {active ? "HOOTER ON" : "Off"}
    </span>
  );
}
