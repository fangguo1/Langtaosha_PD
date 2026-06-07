import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const backendTarget = process.env.VITE_BACKEND_PROXY_TARGET || "http://127.0.0.1:5173";
const backendApiToken = process.env.VITE_BACKEND_API_TOKEN || "";
const legacyPageProxyHeaders = {
  "X-Langtaosha-Legacy-Page-Proxy": "1",
};
const apiProxyHeaders = backendApiToken
  ? {
      Authorization: `Bearer ${backendApiToken}`,
    }
  : undefined;

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5004,
    strictPort: true,
    proxy: {
      "^/$": {
        target: backendTarget,
        changeOrigin: true,
        headers: legacyPageProxyHeaders,
      },
      "/search": {
        target: backendTarget,
        changeOrigin: true,
        headers: legacyPageProxyHeaders,
      },
      "/api": {
        target: backendTarget,
        changeOrigin: true,
        headers: apiProxyHeaders,
      },
      "/study": {
        target: backendTarget,
        changeOrigin: true,
        headers: legacyPageProxyHeaders,
      },
      "/future": {
        target: backendTarget,
        changeOrigin: true,
        headers: legacyPageProxyHeaders,
      },
      "/show_page": {
        target: backendTarget,
        changeOrigin: true,
        headers: legacyPageProxyHeaders,
      },
      "/grant_trends": {
        target: backendTarget,
        changeOrigin: true,
        headers: legacyPageProxyHeaders,
      },
      "/span-matcher": {
        target: backendTarget,
        changeOrigin: true,
        headers: legacyPageProxyHeaders,
      },
      "/feedback-review": {
        target: backendTarget,
        changeOrigin: true,
        headers: legacyPageProxyHeaders,
      },
      "/static": {
        target: backendTarget,
        changeOrigin: true,
        headers: legacyPageProxyHeaders,
      },
    },
  },
});
