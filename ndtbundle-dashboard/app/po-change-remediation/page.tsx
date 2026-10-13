"use client";

import { useMemo, useState } from "react";
import Link from "next/link";
import {
  api,
  type PoChangeRemediationFinishedBatch,
  type PoChangeRemediationFormingBatch,
  type PoChangeRemediationFormingCandidate,
  type PoChangeRemediationMillStatusResult,
  type PoChangeRemediationPreviewResult,
  type PoChangeRemediationCommitResult,
  type PoChangeRemediationSlitRow,
} from "@/lib/api";
import { getSettingsToken, setSettingsToken } from "@/lib/settingsAuth";

type Step = "edit" | "preview" | "done";

function emptySlit(): PoChangeRemediationSlitRow {
  return {
    slitNo: "",
    ndtPipes: 0,
    rejectedPipes: 0,
    slitStartTime: "",
    slitFinishTime: "",
  };
}

function emptyFinished(): PoChangeRemediationFinishedBatch {
  return {
    bundleNo: "",
    poNumber: "",
    totalNdtPcs: 0,
    reprint: false,
    slits: [emptySlit()],
  };
}

export default function PoChangeRemediationPage() {
  const [step, setStep] = useState<Step>("edit");
  const [millNo, setMillNo] = useState(1);
  const [reason, setReason] = useState("");
  const [reconciledBy, setReconciledBy] = useState("ops");
  const [finished, setFinished] = useState<PoChangeRemediationFinishedBatch[]>([emptyFinished()]);
  const [hasForming, setHasForming] = useState(true);
  const [forming, setForming] = useState<PoChangeRemediationFormingBatch>({
    bundleNo: "",
    poNumber: "",
    targetNdtPcs: null,
    slits: [emptySlit()],
  });
  const [confirmCommit, setConfirmCommit] = useState(false);
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [preview, setPreview] = useState<PoChangeRemediationPreviewResult | null>(null);
  const [commitResult, setCommitResult] = useState<PoChangeRemediationCommitResult | null>(null);
  const [millStatus, setMillStatus] = useState<PoChangeRemediationMillStatusResult | null>(null);
  const [keepCandidate, setKeepCandidate] = useState<string>("");
  const [message, setMessage] = useState<{ type: "ok" | "err"; text: string } | null>(null);

  const body = useMemo(() => {
    return {
      millNo,
      reason: reason.trim(),
      reconciledBy: reconciledBy.trim() || "ops",
      finishedBatches: finished.map((b) => ({
        ...b,
        bundleNo: b.bundleNo.trim(),
        poNumber: b.poNumber.trim(),
        slits: b.slits.filter((s) => s.slitNo.trim()),
      })),
      forming: hasForming
        ? {
            bundleNo: forming.bundleNo.trim(),
            poNumber: forming.poNumber.trim(),
            targetNdtPcs: forming.targetNdtPcs || null,
            slits: forming.slits.filter((s) => s.slitNo.trim()),
          }
        : null,
    };
  }, [millNo, reason, reconciledBy, finished, hasForming, forming]);

  const scanMill = async () => {
    setBusy(true);
    setMessage(null);
    try {
      const res = await api.poChangeRemediationMillStatus(millNo);
      setMillStatus(res);
      if (res.ok === false || (res.errors && res.errors.length > 0)) {
        setMessage({ type: "err", text: (res.errors ?? ["Mill scan failed"]).join(" ") });
        return;
      }
      const suggested = res.suggestedKeepBundleNo ?? res.formingCandidates?.[0]?.bundleNo ?? "";
      setKeepCandidate(suggested);
      const n = res.formingCandidates?.length ?? 0;
      if (n === 0) {
        setMessage({ type: "ok", text: `Mill ${millNo}: no forming/phantom stamps found.` });
      } else if (n === 1) {
        setMessage({
          type: "ok",
          text: `Mill ${millNo}: one forming stamp (${res.formingCandidates?.[0]?.bundleNo}). You can load it into the form.`,
        });
      } else {
        setMessage({
          type: "err",
          text: `Mill ${millNo}: ${n} forming/phantom stamps — pick the keep bundle and Apply to consolidate.`,
        });
      }
    } catch (e) {
      setMessage({ type: "err", text: e instanceof Error ? e.message : "Mill scan failed." });
    } finally {
      setBusy(false);
    }
  };

  const applyKeepForming = (bundleNo?: string) => {
    const keepNo = (bundleNo || keepCandidate || "").trim();
    const candidates = millStatus?.formingCandidates ?? [];
    const keep = candidates.find((c) => (c.bundleNo ?? "").trim() === keepNo);
    if (!keep?.bundleNo) {
      setMessage({ type: "err", text: "Select a keep forming bundle from the mill scan." });
      return;
    }

    const keepPo = (keep.poNumber ?? "").trim();
    const slitMap = new Map<string, PoChangeRemediationSlitRow>();
    const addSlits = (rows: PoChangeRemediationSlitRow[] | undefined) => {
      for (const s of rows ?? []) {
        const key = (s.slitNo ?? "").trim();
        if (!key) continue;
        if (!slitMap.has(key)) slitMap.set(key, { ...s });
      }
    };
    addSlits(keep.slits);
    for (const c of candidates) {
      if ((c.bundleNo ?? "").trim() === keepNo) continue;
      if ((c.poNumber ?? "").trim() !== keepPo) continue;
      addSlits(c.slits);
    }

    const slits = Array.from(slitMap.values());
    setHasForming(true);
    setForming({
      bundleNo: keep.bundleNo ?? "",
      poNumber: keepPo,
      targetNdtPcs: null,
      slits: slits.length > 0 ? slits : [emptySlit()],
    });
    setFinished([]);
    if (!reason.trim()) {
      setReason(`Consolidate dual forming on mill ${millNo}; keep ${keep.bundleNo}`);
    }
    const voidCount = candidates.filter((c) => (c.bundleNo ?? "").trim() !== keepNo).length;
    setMessage({
      type: "ok",
      text: `Loaded keep ${keep.bundleNo} with ${slits.length} same-PO slit(s). Preview will void ${voidCount} other forming/phantom stamp(s). Leave Target empty.`,
    });
  };

  const runPreview = async () => {
    setBusy(true);
    setMessage(null);
    setPreview(null);
    try {
      const res = await api.poChangeRemediationPreview(body);
      setPreview(res);
      if (res.ok === false || (res.errors && res.errors.length > 0)) {
        setMessage({ type: "err", text: (res.errors ?? ["Preview failed"]).join(" ") });
        setStep("edit");
      } else {
        setStep("preview");
      }
    } catch (e) {
      setMessage({ type: "err", text: e instanceof Error ? e.message : "Preview failed." });
    } finally {
      setBusy(false);
    }
  };

  const ensureToken = async (): Promise<string | null> => {
    const existing = getSettingsToken();
    if (existing) return existing;
    if (!password.trim()) {
      setMessage({ type: "err", text: "Enter Settings password to commit." });
      return null;
    }
    const login = await api.settingsLogin(password.trim());
    const token = (login as { token?: string }).token;
    if (!token) {
      setMessage({ type: "err", text: "Settings login failed." });
      return null;
    }
    setSettingsToken(token);
    return token;
  };

  const runCommit = async () => {
    if (!confirmCommit) {
      setMessage({ type: "err", text: "Check the confirm box before commit." });
      return;
    }
    setBusy(true);
    setMessage(null);
    try {
      const token = await ensureToken();
      if (!token) {
        setBusy(false);
        return;
      }
      const res = await api.poChangeRemediationCommit(token, body);
      setCommitResult(res);
      if (res.ok) {
        setStep("done");
        setMessage({ type: "ok", text: res.message ?? "Committed." });
      } else {
        setMessage({ type: "err", text: (res.errors ?? ["Commit failed"]).join(" ") });
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Commit failed.";
      if (/settings login|401|unauthorized/i.test(msg)) {
        setMessage({ type: "err", text: "Settings login required — enter password and retry." });
      } else {
        setMessage({ type: "err", text: msg });
      }
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-xl font-semibold text-gray-900">PO-change Remediation</h1>
        <p className="text-sm text-gray-600 mt-1 max-w-3xl">
          Backfill finished batches after missed PO changes, or fix dual-forming / phantom{" "}
          <code className="text-xs">CsvAdvance</code> stamps: keep <strong>exactly one</strong>{" "}
          forming (or none), consolidate same-PO slits, rewrite NDT CSVs, fix Mill sequence, and void
          extras. Leave forming Target empty for provisional chart size. Prefer committing when the
          mill is quiet.{" "}
          <Link href="/settings" className="text-primary-700 underline">
            Settings unlock
          </Link>{" "}
          is required to commit.
        </p>
      </div>

      {message && (
        <div
          className={`rounded-md px-3 py-2 text-sm ${
            message.type === "ok" ? "bg-green-50 text-green-800" : "bg-red-50 text-red-800"
          }`}
        >
          {message.text}
        </div>
      )}

      {step === "edit" && (
        <div className="space-y-6">
          <div className="flex flex-wrap gap-4 items-end">
            <label className="text-sm">
              <span className="block text-gray-600 mb-1">Mill</span>
              <select
                className="border border-gray-300 rounded-md px-3 py-2 text-sm"
                value={millNo}
                onChange={(e) => setMillNo(Number(e.target.value))}
              >
                {[1, 2, 3, 4].map((m) => (
                  <option key={m} value={m}>
                    Mill {m}
                  </option>
                ))}
              </select>
            </label>
            <label className="text-sm flex-1 min-w-[12rem]">
              <span className="block text-gray-600 mb-1">Reason</span>
              <input
                className="border border-gray-300 rounded-md px-3 py-2 text-sm w-full"
                value={reason}
                onChange={(e) => setReason(e.target.value)}
                placeholder="e.g. Missed PO change 2601→2605"
              />
            </label>
            <label className="text-sm">
              <span className="block text-gray-600 mb-1">By</span>
              <input
                className="border border-gray-300 rounded-md px-3 py-2 text-sm w-40"
                value={reconciledBy}
                onChange={(e) => setReconciledBy(e.target.value)}
              />
            </label>
          </div>

          <section className="space-y-3 border border-amber-200 rounded-md p-3 bg-amber-50/30">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <h2 className="text-base font-medium text-gray-900">
                Dual-forming / phantom cleanup
              </h2>
              <button
                type="button"
                disabled={busy}
                className="px-3 py-1.5 rounded-md border border-amber-300 bg-white text-sm text-amber-900 disabled:opacity-50"
                onClick={() => void scanMill()}
              >
                {busy ? "Scanning…" : "Scan mill forming"}
              </button>
            </div>
            <p className="text-xs text-gray-600">
              Use when two or more forming/phantom stamps exist (e.g. after CSV overshoot opened a
              next stamp). Scan, pick the keep bundle, Apply — slits on same PO are merged; Target
              stays empty; Preview voids the rest.
            </p>
            {millStatus && (millStatus.formingCandidates?.length ?? 0) > 0 && (
              <div className="space-y-2">
                <p className="text-xs text-gray-700">
                  Sequence={millStatus.currentMillSequence}; suggested keep={" "}
                  <span className="font-mono">{millStatus.suggestedKeepBundleNo ?? "—"}</span>
                </p>
                {(millStatus.warnings ?? []).length > 0 && (
                  <ul className="text-xs text-amber-900 list-disc list-inside">
                    {(millStatus.warnings ?? []).map((w) => (
                      <li key={w}>{w}</li>
                    ))}
                  </ul>
                )}
                <table className="min-w-full text-sm border bg-white">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-2 py-1 text-left">Keep</th>
                      <th className="px-2 py-1 text-left">Bundle</th>
                      <th className="px-2 py-1 text-left">PO</th>
                      <th className="px-2 py-1 text-left">Fill</th>
                      <th className="px-2 py-1 text-left">Target/Filled</th>
                      <th className="px-2 py-1 text-left">Slits</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(millStatus.formingCandidates ?? []).map((c: PoChangeRemediationFormingCandidate) => (
                      <tr key={c.bundleNo} className="border-t">
                        <td className="px-2 py-1">
                          <input
                            type="radio"
                            name="keepForming"
                            checked={keepCandidate === (c.bundleNo ?? "")}
                            onChange={() => setKeepCandidate(c.bundleNo ?? "")}
                          />
                        </td>
                        <td className="px-2 py-1 font-mono">{c.bundleNo}</td>
                        <td className="px-2 py-1">{c.poNumber}</td>
                        <td className="px-2 py-1">
                          {c.csvFillState}/{c.printStatus}
                          {c.closeSource ? ` (${c.closeSource})` : ""}
                        </td>
                        <td className="px-2 py-1">
                          {c.targetNdtPcs}/{c.csvFilled}
                        </td>
                        <td className="px-2 py-1">
                          {(c.slits ?? []).map((s) => s.slitNo).join(", ") || "—"} ({c.slitSum ?? 0})
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                <button
                  type="button"
                  className="px-3 py-1.5 rounded-md bg-amber-700 text-white text-sm"
                  onClick={() => applyKeepForming()}
                >
                  Apply keep forming to form
                </button>
              </div>
            )}
          </section>

          <section className="space-y-3">
            <div className="flex items-center justify-between">
              <h2 className="text-base font-medium text-gray-900">Finished batches</h2>
              <button
                type="button"
                className="text-sm text-primary-700"
                onClick={() => setFinished((prev) => [...prev, emptyFinished()])}
              >
                + Add finished batch
              </button>
            </div>
            {finished.map((batch, bi) => (
              <div key={bi} className="border border-gray-200 rounded-md p-3 space-y-3 bg-white">
                <div className="flex flex-wrap gap-3 items-end">
                  <Field
                    label="Bundle No"
                    value={batch.bundleNo}
                    onChange={(v) =>
                      setFinished((prev) => prev.map((b, i) => (i === bi ? { ...b, bundleNo: v } : b)))
                    }
                    placeholder="1226100027"
                  />
                  <Field
                    label="PO"
                    value={batch.poNumber}
                    onChange={(v) =>
                      setFinished((prev) => prev.map((b, i) => (i === bi ? { ...b, poNumber: v } : b)))
                    }
                  />
                  <Field
                    label="Total NDT"
                    type="number"
                    value={String(batch.totalNdtPcs)}
                    onChange={(v) =>
                      setFinished((prev) =>
                        prev.map((b, i) => (i === bi ? { ...b, totalNdtPcs: Number(v) || 0 } : b))
                      )
                    }
                  />
                  <label className="text-sm flex items-center gap-2 pb-2">
                    <input
                      type="checkbox"
                      checked={!!batch.reprint}
                      onChange={(e) =>
                        setFinished((prev) =>
                          prev.map((b, i) => (i === bi ? { ...b, reprint: e.target.checked } : b))
                        )
                      }
                    />
                    Reprint after commit
                  </label>
                  {finished.length > 1 && (
                    <button
                      type="button"
                      className="text-sm text-red-700 pb-2"
                      onClick={() => setFinished((prev) => prev.filter((_, i) => i !== bi))}
                    >
                      Remove
                    </button>
                  )}
                </div>
                <SlitEditor
                  slits={batch.slits}
                  onChange={(slits) =>
                    setFinished((prev) => prev.map((b, i) => (i === bi ? { ...b, slits } : b)))
                  }
                />
              </div>
            ))}
          </section>

          <section className="space-y-3">
            <label className="flex items-center gap-2 text-sm font-medium text-gray-900">
              <input
                type="checkbox"
                checked={hasForming}
                onChange={(e) => setHasForming(e.target.checked)}
              />
              Current forming batch (uncheck = none; voids other forming on mill)
            </label>
            {hasForming && (
              <div className="border border-amber-200 rounded-md p-3 space-y-3 bg-amber-50/40">
                <div className="flex flex-wrap gap-3">
                  <Field
                    label="Bundle No"
                    value={forming.bundleNo}
                    onChange={(v) => setForming((f) => ({ ...f, bundleNo: v }))}
                    placeholder="1226100028"
                  />
                  <Field
                    label="PO"
                    value={forming.poNumber}
                    onChange={(v) => setForming((f) => ({ ...f, poNumber: v }))}
                  />
                  <Field
                    label="Target (optional — leave empty)"
                    type="number"
                    value={forming.targetNdtPcs == null ? "" : String(forming.targetNdtPcs)}
                    onChange={(v) =>
                      setForming((f) => ({
                        ...f,
                        targetNdtPcs: v.trim() === "" ? null : Number(v) || null,
                      }))
                    }
                  />
                </div>
                <p className="text-xs text-amber-900">
                  Leave Target empty so provisional formation-chart size is used. A small target with
                  more slits causes CsvOvershoot and can open another phantom stamp.
                </p>
                <SlitEditor
                  slits={forming.slits}
                  onChange={(slits) => setForming((f) => ({ ...f, slits }))}
                />
              </div>
            )}
          </section>

          <button
            type="button"
            disabled={busy || !reason.trim()}
            onClick={() => void runPreview()}
            className="px-4 py-2 rounded-md bg-primary-600 text-white text-sm disabled:opacity-50"
          >
            {busy ? "Previewing…" : "Preview"}
          </button>
        </div>
      )}

      {step === "preview" && preview && (
        <div className="space-y-4">
          <h2 className="text-base font-medium">Preview</h2>
          <p className="text-sm text-gray-700">
            Mill {preview.millNo}: sequence {preview.currentMillSequence} →{" "}
            <strong>{preview.proposedMillSequence}</strong>
          </p>
          {(preview.warnings ?? []).length > 0 && (
            <ul className="text-sm text-amber-800 bg-amber-50 rounded-md px-3 py-2 list-disc list-inside">
              {(preview.warnings ?? []).map((w) => (
                <li key={w}>{w}</li>
              ))}
            </ul>
          )}
          <PlanTable title="Finished" rows={preview.finishedPlans ?? []} />
          {preview.formingPlan && (
            <PlanTable title="Forming" rows={[preview.formingPlan]} />
          )}
          {(preview.orphansToVoid ?? []).length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-red-800 mb-1">Will void (extra forming)</h3>
              <table className="min-w-full text-sm border">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-2 py-1 text-left">Bundle</th>
                    <th className="px-2 py-1 text-left">PO</th>
                    <th className="px-2 py-1 text-left">Fill</th>
                    <th className="px-2 py-1 text-left">Print</th>
                  </tr>
                </thead>
                <tbody>
                  {(preview.orphansToVoid ?? []).map((o) => (
                    <tr key={o.bundleNo} className="border-t">
                      <td className="px-2 py-1 font-mono">{o.bundleNo}</td>
                      <td className="px-2 py-1">{o.poNumber}</td>
                      <td className="px-2 py-1">
                        {o.csvFillState} ({o.csvFilled})
                        {(o.slitCount ?? 0) > 0
                          ? ` · ${o.slitCount} slits: ${(o.slitNos ?? []).join(", ")}`
                          : ""}
                      </td>
                      <td className="px-2 py-1">{o.printStatus}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          {(preview.csvFiles ?? []).length > 0 && (
            <p className="text-xs text-gray-600">
              CSVs: {(preview.csvFiles ?? []).join(", ")}
            </p>
          )}

          <div className="flex flex-wrap gap-4 items-end border-t pt-4">
            <label className="text-sm flex items-center gap-2">
              <input
                type="checkbox"
                checked={confirmCommit}
                onChange={(e) => setConfirmCommit(e.target.checked)}
              />
              I confirm this remediation (voids extra forming; overwrites CSVs)
            </label>
            {!getSettingsToken() && (
              <label className="text-sm">
                <span className="block text-gray-600 mb-1">Settings password</span>
                <input
                  type="password"
                  className="border border-gray-300 rounded-md px-3 py-2 text-sm"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                />
              </label>
            )}
            <button
              type="button"
              className="px-3 py-2 text-sm border rounded-md"
              onClick={() => setStep("edit")}
              disabled={busy}
            >
              Back
            </button>
            <button
              type="button"
              disabled={busy || !confirmCommit}
              onClick={() => void runCommit()}
              className="px-4 py-2 rounded-md bg-primary-600 text-white text-sm disabled:opacity-50"
            >
              {busy ? "Committing…" : "Commit"}
            </button>
          </div>
        </div>
      )}

      {step === "done" && commitResult && (
        <div className="space-y-3">
          <h2 className="text-base font-medium text-green-800">Committed</h2>
          <p className="text-sm">{commitResult.message}</p>
          <p className="text-sm text-gray-700">
            Sequence={commitResult.millSequence}; voided:{" "}
            {(commitResult.voidedBundleNos ?? []).join(", ") || "(none)"}; CSVs:{" "}
            {(commitResult.csvFilesWritten ?? []).length}
          </p>
          {(commitResult.reprints ?? []).length > 0 && (
            <ul className="text-sm list-disc list-inside">
              {(commitResult.reprints ?? []).map((r) => (
                <li key={r.bundleNo}>
                  Reprint {r.bundleNo}: {r.success ? "OK" : r.message}
                </li>
              ))}
            </ul>
          )}
          <button
            type="button"
            className="px-4 py-2 rounded-md bg-primary-600 text-white text-sm"
            onClick={() => {
              setStep("edit");
              setPreview(null);
              setCommitResult(null);
              setConfirmCommit(false);
              setMessage(null);
            }}
          >
            Start another
          </button>
        </div>
      )}
    </div>
  );
}

function Field({
  label,
  value,
  onChange,
  placeholder,
  type = "text",
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  type?: string;
}) {
  return (
    <label className="text-sm">
      <span className="block text-gray-600 mb-1">{label}</span>
      <input
        type={type}
        className="border border-gray-300 rounded-md px-3 py-2 text-sm w-40"
        value={value}
        placeholder={placeholder}
        onChange={(e) => onChange(e.target.value)}
      />
    </label>
  );
}

function SlitEditor({
  slits,
  onChange,
}: {
  slits: PoChangeRemediationSlitRow[];
  onChange: (rows: PoChangeRemediationSlitRow[]) => void;
}) {
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <span className="text-xs font-medium text-gray-600 uppercase tracking-wide">Slits</span>
        <button
          type="button"
          className="text-xs text-primary-700"
          onClick={() => onChange([...slits, emptySlit()])}
        >
          + Slit
        </button>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs">
          <thead>
            <tr className="text-left text-gray-500">
              <th className="pr-2 py-1">Slit No</th>
              <th className="pr-2 py-1">NDT</th>
              <th className="pr-2 py-1">Rej</th>
              <th className="pr-2 py-1">Start (dd.MM.yyyy HH:mm:ss)</th>
              <th className="pr-2 py-1">Finish</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {slits.map((s, i) => (
              <tr key={i}>
                <td className="pr-2 py-1">
                  <input
                    className="border rounded px-2 py-1 w-28"
                    value={s.slitNo}
                    onChange={(e) =>
                      onChange(slits.map((row, j) => (j === i ? { ...row, slitNo: e.target.value } : row)))
                    }
                  />
                </td>
                <td className="pr-2 py-1">
                  <input
                    type="number"
                    className="border rounded px-2 py-1 w-16"
                    value={s.ndtPipes}
                    onChange={(e) =>
                      onChange(
                        slits.map((row, j) =>
                          j === i ? { ...row, ndtPipes: Number(e.target.value) || 0 } : row
                        )
                      )
                    }
                  />
                </td>
                <td className="pr-2 py-1">
                  <input
                    type="number"
                    className="border rounded px-2 py-1 w-14"
                    value={s.rejectedPipes}
                    onChange={(e) =>
                      onChange(
                        slits.map((row, j) =>
                          j === i ? { ...row, rejectedPipes: Number(e.target.value) || 0 } : row
                        )
                      )
                    }
                  />
                </td>
                <td className="pr-2 py-1">
                  <input
                    className="border rounded px-2 py-1 w-44"
                    value={s.slitStartTime}
                    onChange={(e) =>
                      onChange(
                        slits.map((row, j) =>
                          j === i ? { ...row, slitStartTime: e.target.value } : row
                        )
                      )
                    }
                  />
                </td>
                <td className="pr-2 py-1">
                  <input
                    className="border rounded px-2 py-1 w-44"
                    value={s.slitFinishTime}
                    onChange={(e) =>
                      onChange(
                        slits.map((row, j) =>
                          j === i ? { ...row, slitFinishTime: e.target.value } : row
                        )
                      )
                    }
                  />
                </td>
                <td>
                  {slits.length > 1 && (
                    <button
                      type="button"
                      className="text-red-600"
                      onClick={() => onChange(slits.filter((_, j) => j !== i))}
                    >
                      ×
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function PlanTable({
  title,
  rows,
}: {
  title: string;
  rows: {
    bundleNo?: string;
    poNumber?: string;
    action?: string;
    totalOrFilled?: number;
    targetNdtPcs?: number | null;
    slitCount?: number;
    reprint?: boolean;
  }[];
}) {
  return (
    <div>
      <h3 className="text-sm font-medium mb-1">{title}</h3>
      <table className="min-w-full text-sm border">
        <thead className="bg-gray-50">
          <tr>
            <th className="px-2 py-1 text-left">Bundle</th>
            <th className="px-2 py-1 text-left">PO</th>
            <th className="px-2 py-1 text-left">Action</th>
            <th className="px-2 py-1 text-left">Pcs</th>
            <th className="px-2 py-1 text-left">Slits</th>
            <th className="px-2 py-1 text-left">Reprint</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.bundleNo} className="border-t">
              <td className="px-2 py-1 font-mono">{r.bundleNo}</td>
              <td className="px-2 py-1">{r.poNumber}</td>
              <td className="px-2 py-1">{r.action}</td>
              <td className="px-2 py-1">
                {r.totalOrFilled}
                {r.targetNdtPcs != null ? ` / ${r.targetNdtPcs}` : ""}
              </td>
              <td className="px-2 py-1">{r.slitCount}</td>
              <td className="px-2 py-1">{r.reprint ? "yes" : ""}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
