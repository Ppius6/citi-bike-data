import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      // Mirrors the nginx /api proxy used in the docker-compose deployment,
      // so the same relative fetch('/api/...') calls work in `npm run dev` too.
      '/api': 'http://localhost:8000',
    },
  },
})
