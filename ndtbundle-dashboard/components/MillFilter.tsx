"use client";

import {
  MILL_FILTER_OPTIONS,
  type MillFilterValue,
  countBundlesByMill,
} from "@/lib/millFilter";
import type { ReconcileBundle } from "@/lib/api";

type MillFilterProps = {
  value: MillFilterValue;
  onChange: (mill: MillFilterValue) => void;
  bundles: ReconcileBundle[];
  className?: string;
};

export function MillFilter({ value, onChange, bundles, className = "" }: MillFilterProps) {
  const counts = countBundlesByMill(bundles);

  return (
    <div className={`flex flex-wrap items-center gap-2 ${className}`}>
      <span className="text-sm font-medium text-gray-700 mr-1">Mill</span>
      {MILL_FILTER_OPTIONS.map((opt) => {
        const active = value === opt.value;
        const count = counts[opt.value];
        return (
          <button
            key={String(opt.value)}
            type="button"
            onClick={() => onChange(opt.value)}
            className={`px-3 py-1.5 rounded-md text-sm font-medium border transition-colors ${
              active
                ? "bg-primary-600 text-white border-primary-600"
                : "bg-white text-gray-700 border-gray-300 hover:bg-gray-50"
            }`}
          >
            {opt.label}
            <span
              className={`ml-1.5 tabular-nums text-xs ${
                active ? "text-primary-100" : "text-gray-500"
              }`}
            >
              ({count})
            </span>
          </button>
        );
      })}
    </div>
  );
}
