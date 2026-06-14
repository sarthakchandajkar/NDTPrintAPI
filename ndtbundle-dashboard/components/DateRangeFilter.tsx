"use client";

import {
  defaultDateRange,
  EMPTY_DATE_RANGE,
  isDateRangeActive,
  type DateRange,
} from "@/lib/dateRangeFilter";

type Props = {
  value: DateRange;
  onChange: (next: DateRange) => void;
  /** Shown when a filter is active, e.g. "12 of 48 rows". */
  summary?: string;
  className?: string;
  /** Hint under the inputs. */
  hint?: string;
};

export function DateRangeFilter({ value, onChange, summary, className = "", hint }: Props) {
  const active = isDateRangeActive(value);

  return (
    <div
      className={`rounded-lg border border-gray-200 bg-white shadow-sm p-4 flex flex-col sm:flex-row sm:flex-wrap sm:items-end gap-4 ${className}`}
    >
      <div className="flex-1 min-w-[10rem]">
        <label className="block text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">
          Start date
        </label>
        <input
          type="date"
          value={value.startDate}
          onChange={(e) => onChange({ ...value, startDate: e.target.value })}
          className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
        />
      </div>
      <div className="flex-1 min-w-[10rem]">
        <label className="block text-xs font-medium text-gray-500 uppercase tracking-wide mb-1">
          End date
        </label>
        <input
          type="date"
          value={value.endDate}
          min={value.startDate || undefined}
          onChange={(e) => onChange({ ...value, endDate: e.target.value })}
          className="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-primary-500 focus:border-primary-500"
        />
      </div>
      <div className="flex flex-wrap gap-2">
        <button
          type="button"
          onClick={() => onChange(defaultDateRange())}
          className="px-3 py-2 text-sm font-medium border border-gray-300 rounded-md text-gray-700 bg-white hover:bg-gray-50"
        >
          Last 30 days
        </button>
        <button
          type="button"
          onClick={() => onChange(EMPTY_DATE_RANGE)}
          className="px-3 py-2 text-sm font-medium border border-gray-300 rounded-md text-gray-700 bg-white hover:bg-gray-50"
        >
          Clear
        </button>
      </div>
      <div className="sm:ml-auto text-sm text-gray-600 min-w-[8rem]">
        {active ? (
          summary ? (
            <span>{summary}</span>
          ) : (
            <span className="text-primary-700 font-medium">Date filter active</span>
          )
        ) : (
          <span className="text-gray-400">Showing all dates</span>
        )}
      </div>
      {hint && <p className="w-full text-xs text-gray-500 -mt-1">{hint}</p>}
    </div>
  );
}
