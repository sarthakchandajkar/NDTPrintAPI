"use client";

import Link from "next/link";
import Image from "next/image";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";
import { api, type PlcStatus, type PrinterStatus } from "@/lib/api";

type NavChild = { href: string; label: string };

type NavEntry =
  | { type: "link"; href: string; label: string }
  | { type: "group"; label: string; prefix: string; children: NavChild[] };

const navEntries: NavEntry[] = [
  { type: "link", href: "/", label: "Summary" },
  {
    type: "group",
    label: "Visual",
    prefix: "/visual",
    children: [
      { href: "/visual/station-1", label: "Station 1" },
      { href: "/visual/station-2", label: "Station 2" },
    ],
  },
  { type: "link", href: "/hydrotesting", label: "Hydrotesting" },
  {
    type: "group",
    label: "Revisual",
    prefix: "/revisual",
    children: [
      { href: "/revisual/station-1", label: "Station 1" },
      { href: "/revisual/station-2", label: "Station 2" },
    ],
  },
  { type: "link", href: "/input-slits", label: "Input Slit Files" },
  { type: "link", href: "/printed-tags", label: "Printed Tags" },
  { type: "link", href: "/reconcile", label: "Reconcile Bundle" },
  { type: "link", href: "/po-end", label: "PO End" },
  { type: "link", href: "/mill3-plc", label: "Mill-3 PLC" },
];

function isGroupActive(prefix: string, pathname: string, children: NavChild[]) {
  return (
    pathname === prefix ||
    pathname.startsWith(prefix + "/") ||
    children.some((c) => pathname === c.href)
  );
}

function plcBadgeClass(plc: PlcStatus | null): string {
  if (!plc) return "bg-gray-100 text-gray-600";
  const drv = plc.driver ?? "Stub";
  if (!plc.plcPoEndEnabled || drv === "Stub") return "bg-gray-100 text-gray-600";
  if (plc.connected) return plc.poEndActive ? "bg-amber-100 text-amber-800" : "bg-green-100 text-green-800";
  return "bg-red-100 text-red-800";
}

function plcLabel(plc: PlcStatus | null): string {
  if (!plc) return "—";
  const drv = plc.driver ?? "Stub";
  if (!plc.plcPoEndEnabled || drv === "Stub") return "Off";
  if (plc.connected) return plc.poEndActive ? "PO End" : "OK";
  return "Offline";
}

function printerBadgeClass(status?: string): string {
  if (status === "Ready") return "bg-green-100 text-green-800";
  if (status === "Unreachable") return "bg-red-100 text-red-800";
  if (status === "Configured") return "bg-amber-100 text-amber-800";
  return "bg-gray-100 text-gray-600";
}

export default function Navbar() {
  const pathname = usePathname();
  const [plc, setPlc] = useState<PlcStatus | null>(null);
  const [printer, setPrinter] = useState<PrinterStatus | null>(null);

  useEffect(() => {
    api.plcStatus().then(setPlc).catch(() => setPlc({ connected: false }));
    api.printerStatus().then(setPrinter).catch(() => setPrinter({ status: "Unknown" }));
  }, []);

  return (
    <header className="bg-white border-b border-gray-200 sticky top-0 z-10 shadow-sm">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between items-center h-32">
          <div className="flex items-center gap-8">
            <Link href="/" className="flex items-center gap-3 shrink-0">
              <Image
                src="/ajspc_logo.png"
                alt="AJSPC"
                width={256}
                height={256}
                className="h-28 w-28 object-contain"
                priority
              />
              <span className="text-lg font-semibold text-gray-900">
                NDT Bundle Dashboard
              </span>
            </Link>
            <nav className="hidden md:flex gap-1 items-center">
              {navEntries.map((entry) => {
                if (entry.type === "link") {
                  const active = pathname === entry.href;
                  return (
                    <Link
                      key={entry.href}
                      href={entry.href}
                      className={`px-3 py-2 rounded-md text-sm font-medium ${
                        active
                          ? "bg-primary-50 text-primary-700"
                          : "text-gray-600 hover:bg-gray-50 hover:text-gray-900"
                      }`}
                    >
                      {entry.label}
                    </Link>
                  );
                }

                const groupActive = isGroupActive(
                  entry.prefix,
                  pathname,
                  entry.children
                );

                return (
                  <div key={entry.label} className="relative group">
                    <span
                      className={`px-3 py-2 rounded-md text-sm font-medium inline-flex items-center gap-1 cursor-default select-none ${
                        groupActive
                          ? "bg-primary-50 text-primary-700"
                          : "text-gray-600 group-hover:bg-gray-50 group-hover:text-gray-900"
                      }`}
                    >
                      {entry.label}
                      <svg
                        className="w-4 h-4 opacity-70"
                        fill="none"
                        stroke="currentColor"
                        viewBox="0 0 24 24"
                        aria-hidden
                      >
                        <path
                          strokeLinecap="round"
                          strokeLinejoin="round"
                          strokeWidth={2}
                          d="M19 9l-7 7-7-7"
                        />
                      </svg>
                    </span>
                    <div
                      className="absolute left-0 top-full z-30 -mt-1 pt-1 min-w-[11rem] opacity-0 invisible group-hover:opacity-100 group-hover:visible group-focus-within:opacity-100 group-focus-within:visible transition-opacity"
                      role="menu"
                      aria-label={`${entry.label} stations`}
                    >
                      <div className="rounded-md border border-gray-200 bg-white shadow-lg py-1">
                        {entry.children.map((child) => {
                          const childActive = pathname === child.href;
                          return (
                            <Link
                              key={child.href}
                              href={child.href}
                              role="menuitem"
                              className={`block px-4 py-2 text-sm ${
                                childActive
                                  ? "bg-primary-50 text-primary-700 font-medium"
                                  : "text-gray-700 hover:bg-gray-50"
                              }`}
                            >
                              {child.label}
                            </Link>
                          );
                        })}
                      </div>
                    </div>
                  </div>
                );
              })}
            </nav>
          </div>
          <div className="flex items-center gap-3">
            <span
              title={
                [plc?.message, plc?.lastPlcError].filter(Boolean).join(" — ") || undefined
              }
              className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${plcBadgeClass(plc)}`}
            >
              PLC: {plcLabel(plc)}
            </span>
            <span
              title={printer?.message}
              className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${printerBadgeClass(printer?.status)}`}
            >
              Printer: {printer?.status ?? "—"}
            </span>
          </div>
        </div>
      </div>
    </header>
  );
}
