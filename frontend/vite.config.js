import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],

  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },

  build: {
    // Raise the warning threshold — App.jsx is intentionally large.
    chunkSizeWarningLimit: 1200,

    rollupOptions: {
      output: {
        // Manual chunk split: heavy libs go in separate cached chunks.
        // Browsers cache vendor chunks independently → repeat visits skip those downloads.
        manualChunks(id) {
          if (id.includes('framer-motion')) return 'vendor-framer'
          if (id.includes('@dnd-kit')) return 'vendor-dndkit'
          if (id.includes('lucide-react')) return 'vendor-lucide'
          if (id.includes('node_modules')) return 'vendor'
        },
      },
    },

    // CSS code splitting — each entry gets only the CSS it needs.
    cssCodeSplit: true,

    // Assets <4 kB inlined as base64 (avoids extra round-trips for tiny icons).
    assetsInlineLimit: 4096,

    // Vite v8 uses Oxc as the default minifier (esbuild is deprecated in v8).
    // 'oxc' is the modern default — fastest builds, no separate install needed.
    minify: 'oxc',

    // Disable source maps in production for smaller output.
    sourcemap: false,
  },
})

