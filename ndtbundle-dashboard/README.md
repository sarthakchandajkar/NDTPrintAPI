# NDT Bundle Dashboard

Next.js dashboard for the NDT Bundle Service. White + light green theme.

## Prerequisites

- Node.js 18+
- NdtBundleService running (e.g. `http://localhost:5000`)

## Setup

```bash
cd ndtbundle-dashboard
npm install
```

## Configure API URL

By default the dashboard calls `http://localhost:5000`. To use another URL:

- Create `.env.local` with:
  ```
  NEXT_PUBLIC_API_BASE=http://localhost:5000
  ```
- Or run with: `NEXT_PUBLIC_API_BASE=http://your-api:5000 npm run dev`

Then use the env in the API client (see below).

## Run

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000).

## Build

```bash
npm run build
npm start
```

## Pages

- **Summary** – WIP, NDT pipe count, bundle file list, quick PO End link
- **Input Slit Files** – Table of input CSV files; view content per file
- **Printed Tags** – Table of printed bundles with Reprint action
- **Reconcile Bundle** – Select bundle, enter correct NDT pipes, reconcile and reprint
- **PO End** – Simulate PO End (PO Number + Mill No)

## Backend

Ensure NdtBundleService has CORS enabled and these endpoints:

- `GET /api/Test/wip-info`
- `GET /api/Test/ndt-summary?poNumber=&millNo=`
- `GET /api/Test/bundles`
- `POST /api/Test/po-end`
- `GET /api/Reconcile/bundles`
- `POST /api/Reconcile/reconcile`
- `POST /api/Reconcile/reprint`
- `GET /api/InputSlits/files`
- `GET /api/InputSlits/files/:fileName/content`
- `GET /api/Status/plc`
- `GET /api/Status/printer`
