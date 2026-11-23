import { defineConfig, loadEnv } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  return {
    plugins: [react()],
    server: {
      host: '127.0.0.1',
      port: 5173,
      strictPort: true,
      proxy: {
        '/api/v1': {
          target: env.API_PROXY_TARGET || 'http://127.0.0.1:8013',
          changeOrigin: true,
        },
      },
    },
    preview: { host: '127.0.0.1', port: 5173, strictPort: true },
    build: {
      rollupOptions: {
        output: {
          manualChunks(id) {
            if (id.includes('/node_modules/zrender/')) return 'chart-renderer';
            if (id.includes('/node_modules/echarts/')) return 'chart-engine';
          },
        },
      },
    },
  };
});
