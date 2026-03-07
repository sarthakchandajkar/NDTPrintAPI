"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";
import { api, type PlcStatus, type PrinterStatus } from "@/lib/api";

const navItems = [
  { href: "/", label: "Summary" },
  { href: "/input-slits", label: "Input Slit Files" },
  { href: "/printed-tags", label: "Printed Tags" },
  { href: "/reconcile", label: "Reconcile Bundle" },
  { href: "/po-end", label: "PO End" },
];

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
        <div className="flex justify-between items-center h-14">
          <div className="flex items-center gap-8">
            <Link href="/" className="text-lg font-semibold text-gray-900">
              NDT Bundle Dashboard
            </Link>
            <nav className="hidden md:flex gap-1">
              {navItems.map(({ href, label }) => (
                <Link
                  key={href}
                  href={href}
                  className={`px-3 py-2 rounded-md text-sm font-medium ${
                    pathname === href
                      ? "bg-primary-50 text-primary-700"
                      : "text-gray-600 hover:bg-gray-50 hover:text-gray-900"
                  }`}
                >
                  {label}
                </Link>
              ))}
            </nav>
          </div>
          <div className="flex items-center gap-3">
            <span
              className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                plc?.connected ? "bg-green-100 text-green-800" : "bg-gray-100 text-gray-600"
              }`}
            >
              PLC: {plc?.connected ? (plc.poEndActive ? "PO End" : "Connected") : "—"}
            </span>
            <span
              className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                printer?.status === "Ready"
                  ? "bg-green-100 text-green-800"
                  : printer?.status === "NotConfigured"
                    ? "bg-gray-100 text-gray-600"
                    : "bg-amber-100 text-amber-800"
              }`}
            >
              Printer: {printer?.status ?? "—"}
            </span>
          </div>
        </div>
      </div>
    </header>
  );
}
