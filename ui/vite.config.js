import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  build: {
    outDir: '../libsqlnow/static/',
  },
  server: {
    proxy: {
      '/api': 'http://localhost:8080',
      '/tables.json': 'http://localhost:8080',
      '/table.json': 'http://localhost:8080',
      '/query.json': 'http://localhost:8080',
      '/outputs': 'http://localhost:8080',
    },
  },
})
