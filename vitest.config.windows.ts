import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    name: 'windows-native',
    environment: 'node',
    include: ['test/windows/**/*.test.ts'],
    pool: 'forks',
    maxWorkers: 1,
    fileParallelism: false,
    testTimeout: 120_000,
    hookTimeout: 120_000,
  },
})
