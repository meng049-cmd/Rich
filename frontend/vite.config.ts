import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

function injectBackendSecret(proxy: any) {
  const addSecret = (proxyReq: any) => {
    const secret = process.env.BACKEND_PROXY_SECRET;
    if (secret) proxyReq.setHeader('x-backend-proxy-secret', secret);
  };
  proxy.on('proxyReq', addSecret);
  proxy.on('proxyReqWs', addSecret);
}

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/api': { target: 'http://localhost:8000', configure: injectBackendSecret },
      '/ws': { target: 'ws://localhost:8000', ws: true, configure: injectBackendSecret },
    },
  },
});

