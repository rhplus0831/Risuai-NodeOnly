import { defineConfig } from 'vitest/config'

const RSS_SENSITIVE_EXPORT_TEST = 'test/compat/full-export-import-race.test.ts'

export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: 'compat',
          environment: 'node',
          include: ['test/compat/**/*.test.ts'],
          exclude: [RSS_SENSITIVE_EXPORT_TEST],
          sequence: { groupOrder: 0 },
          // Hang guards only — these tests spawn real servers, and a full parallel
          // run on a small CI runner can push a legitimately-passing file past 30 s.
          testTimeout: 120_000,
          hookTimeout: 120_000,
        },
      },
      {
        test: {
          name: 'compat-export-rss',
          environment: 'node',
          include: [RSS_SENSITIVE_EXPORT_TEST],
          // Raw child-process RSS deltas are sensitive to concurrent-suite GC
          // pressure. Keep the measured 48 MiB folding guards unchanged, but
          // run this file only after the other real-server compat tests finish.
          sequence: { groupOrder: 1 },
          pool: 'forks',
          maxWorkers: 1,
          fileParallelism: false,
          testTimeout: 120_000,
          hookTimeout: 120_000,
        },
      },
    ],
  },
})
