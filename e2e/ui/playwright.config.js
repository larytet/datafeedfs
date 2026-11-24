// Browser tests of the web console (src/ui). `npm test` builds the binary, starts two instances and runs the specs.
//   edit      the mounted filesystem with the console, editing on: status, charts, a real extraction, config editing
//   readonly  console only, editing off, token required
// PW_CHROME=/usr/bin/google-chrome uses an installed Chrome; otherwise run `npx playwright install chromium` once.
const { defineConfig } = require('@playwright/test');

const launchOptions = process.env.PW_CHROME ? { executablePath: process.env.PW_CHROME } : {};
module.exports = defineConfig({
  testDir: './tests',
  workers: 1,
  fullyParallel: false,
  timeout: 90_000,
  expect: { timeout: 15_000 },
  retries: process.env.CI ? 1 : 0,
  reporter: [['list'], ['html', { open: 'never' }]],
  use: { launchOptions, trace: 'retain-on-failure', screenshot: 'only-on-failure' },
  projects: [
    { name: 'edit', testMatch: /edit\.spec\.js/, use: { baseURL: 'http://127.0.0.1:18088' } },
    { name: 'readonly', testMatch: /readonly\.spec\.js/, use: { baseURL: 'http://127.0.0.1:18089' } },
  ],
  webServer: [
    { command: 'node start-server.js edit', url: 'http://127.0.0.1:18088/api/info', reuseExistingServer: false, timeout: 60_000, gracefulShutdown: { signal: 'SIGTERM', timeout: 15_000 } },
    { command: 'node start-server.js readonly', url: 'http://127.0.0.1:18089/api/info', reuseExistingServer: false, timeout: 60_000, gracefulShutdown: { signal: 'SIGTERM', timeout: 15_000 } },
  ],
});
