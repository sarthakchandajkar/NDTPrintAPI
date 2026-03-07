"use client";

import { useEffect, useState } from "react";
import { api, type InputSlitFile, type InputSlitContent } from "@/lib/api";

export default function InputSlitsPage() {
  const [files, setFiles] = useState<InputSlitFile[]>([]);
  const [selectedFile, setSelectedFile] = useState<string | null>(null);
  const [content, setContent] = useState<InputSlitContent | null>(null);
  const [loading, setLoading] = useState(true);
  const [contentLoading, setContentLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = async () => {
    setLoading(true);
    setError(null);
    try {
      const list = await api.inputSlitFiles();
      setFiles(Array.isArray(list) ? list : []);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load files");
      setFiles([]);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refresh();
  }, []);

  useEffect(() => {
    if (!selectedFile) {
      setContent(null);
      return;
    }
    setContentLoading(true);
    api
      .inputSlitContent(selectedFile)
      .then(setContent)
      .catch(() => setContent(null))
      .finally(() => setContentLoading(false));
  }, [selectedFile]);

  return (
    <div className="space-y-6">
      <div className="flex justify-between items-center">
        <h1 className="text-2xl font-bold text-gray-900">Input Slit Files</h1>
        <button
          onClick={refresh}
          className="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50"
        >
          Refresh
        </button>
      </div>

      {error && (
        <div className="rounded-md bg-red-50 border border-red-200 p-4 text-red-700 text-sm">
          {error}
        </div>
      )}

      <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
        <h2 className="px-5 py-3 bg-primary-50 text-gray-900 font-semibold border-b border-gray-200">
          CSV Files (Input Slit Folder)
        </h2>
        {loading ? (
          <p className="px-5 py-8 text-gray-500">Loading...</p>
        ) : files.length === 0 ? (
          <p className="px-5 py-8 text-gray-500 text-sm">No CSV files in input slit folder.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead>
                <tr className="bg-gray-50">
                  <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">File Name</th>
                  <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">Last Modified</th>
                  <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">Size</th>
                  <th className="px-5 py-2 text-left text-xs font-medium text-gray-500 uppercase">View</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {files.map((f) => (
                  <tr key={f.fileName} className="hover:bg-gray-50">
                    <td className="px-5 py-2 text-sm text-gray-900">{f.fileName}</td>
                    <td className="px-5 py-2 text-sm text-gray-600">
                      {f.lastModified ? new Date(f.lastModified).toLocaleString() : "—"}
                    </td>
                    <td className="px-5 py-2 text-sm text-gray-600">{f.size ?? 0} B</td>
                    <td className="px-5 py-2">
                      <button
                        onClick={() => setSelectedFile(selectedFile === f.fileName ? null : f.fileName ?? null)}
                        className="text-primary-600 hover:text-primary-700 text-sm font-medium"
                      >
                        {selectedFile === f.fileName ? "Hide" : "View content"}
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {selectedFile && (
        <div className="bg-white rounded-lg border border-gray-200 shadow-sm overflow-hidden">
          <h2 className="px-5 py-3 bg-primary-50 text-gray-900 font-semibold border-b border-gray-200">
            Content: {selectedFile}
          </h2>
          {contentLoading ? (
            <p className="px-5 py-8 text-gray-500">Loading content...</p>
          ) : content?.rows?.length ? (
            <div className="overflow-x-auto max-h-96 overflow-y-auto">
              <table className="min-w-full divide-y divide-gray-200 text-sm">
                <thead className="bg-gray-50 sticky top-0">
                  <tr>
                    {(content.headers ?? []).map((h, i) => (
                      <th key={i} className="px-3 py-2 text-left font-medium text-gray-500 whitespace-nowrap">
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {content.rows.map((row, ri) => (
                    <tr key={ri} className="hover:bg-gray-50">
                      {row.map((cell, ci) => (
                        <td key={ci} className="px-3 py-2 text-gray-700 whitespace-nowrap">
                          {cell}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="px-5 py-8 text-gray-500 text-sm">No rows or failed to load.</p>
          )}
        </div>
      )}
    </div>
  );
}
