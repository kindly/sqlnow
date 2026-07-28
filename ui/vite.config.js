import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  build: {
    // outDir is outside the vite root, so vite will not empty it: without this
    // every build leaves the previous build's hashed bundles behind.
    outDir: '../libsqlnow/static/',
    emptyOutDir: true,
  },
  test: {
    environment: 'jsdom',
    setupFiles: ['./src/test-setup.js'],
    include: ['src/**/*.test.js'],
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
