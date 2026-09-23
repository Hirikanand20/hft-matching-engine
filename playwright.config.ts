import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  timeout: 30000,
  fullyParallel: false, // gateway shares port 3001
  use: {
    baseURL: 'http://localhost:5173',
  },
  webServer: [
    {
      command: 'npm run dev --prefix frontend',
      port: 5173,
      reuseExistingServer: true,
      cwd: '.',
    },
    {
      command: 'node gateway/gateway.js',
      port: 3001,
      reuseExistingServer: true, // let playwright start/stop it
      env: { NODE_ENV: 'production' }
    }
  ]
});