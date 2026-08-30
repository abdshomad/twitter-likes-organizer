import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['packages/*/src/**/*.spec.ts'],
    exclude: ['cordis/**', 'node_modules/**', 'dist/**'],
  },
})
