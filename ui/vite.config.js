import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: '../libsqlnow/static/',
  },
  server: {
    proxy: {
      '/tables.json': 'http://localhost:8080',
      '/table.json': 'http://localhost:8080',
      '/query.json': 'http://localhost:8080',
      '/outputs': 'http://localhost:8080',
    },
  },
})
