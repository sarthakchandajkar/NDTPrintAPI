import { NextResponse } from "next/server";

const BACKEND = process.env.NEXT_PUBLIC_API_BASE || "http://127.0.0.1:5000";

/**
 * Proxy POST /api/Test/print-dummy-bundle to the backend.
 * Avoids CORS and ensures the request reaches the backend even if rewrites fail.
 */
export async function POST() {
  try {
    const res = await fetch(`${BACKEND}/api/Test/print-dummy-bundle`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      signal: AbortSignal.timeout(20000),
    });
    const text = await res.text();
    let body: unknown;
    try {
      body = text ? JSON.parse(text) : {};
    } catch {
      body = { message: text };
    }
    return NextResponse.json(body, { status: res.status });
  } catch (err) {
    const message = err instanceof Error ? err.message : "Proxy to backend failed";
    return NextResponse.json(
      { message: `Print request failed: ${message}. Is the backend running on ${BACKEND}?` },
      { status: 502 }
    );
  }
}
