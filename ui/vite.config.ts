import { defineConfig } from 'vite';

// The backend accepts only same-origin loopback requests, so the dev proxy presents as the backend origin.
const backend = 'http://127.0.0.1:8765';

export default defineConfig({
  build: { outDir: '../static', emptyOutDir: true },
  server: { proxy: { '/api': { target: backend, changeOrigin: true, headers: { origin: backend } } } },
});
