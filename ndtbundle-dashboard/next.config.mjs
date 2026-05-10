/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  async rewrites() {
    // Proxy /api to the backend so the browser makes same-origin requests (no CORS).
    return [
      { source: "/api/:path*", destination: "http://127.0.0.1:5000/api/:path*" },
    ];
  },
};

export default nextConfig;
