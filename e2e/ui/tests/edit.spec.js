// The mounted filesystem with the console and editing on (port 18088, see ../start-server.js).
const { test, expect } = require('@playwright/test');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const state = JSON.parse(fs.readFileSync(path.join(__dirname, '..', '.state', 'edit.json'), 'utf8'));
const parquet = path.join(state.mountPath, 'tick', 'AAPL', '2026-11-20.parquet');
const field = (page, id) => page.getByTestId(`field-${id}`);

test.describe.configure({ mode: 'serial' });

test('the status page loads with the mount, cache and settings', async ({ page }) => {
  await page.goto('/');
  await expect(page).toHaveTitle('datafeedfs console');
  await expect(page.getByTestId('mount-pill')).toContainText(state.mounted ? 'Mounted' : 'Console only');
  await expect(page.locator('#editpill')).toHaveText('Editing on');
  await expect(page.getByTestId('s-run')).toHaveText('0 / 2');
  await expect(page.getByTestId('kinds')).toContainText('Nothing is cached yet');
  await expect(page.getByTestId('details')).toContainText('this machine');
  await expect(page.getByTestId('details')).toContainText('block on miss');
  await expect(page.getByTestId('fails')).toContainText('No failures');
});

test('the page follows the browser theme', async ({ page }) => {
  await page.goto('/');
  await page.emulateMedia({ colorScheme: 'dark' });
  await expect(page.locator('body')).toHaveCSS('background-color', 'rgb(14, 19, 28)');
  await expect(page.locator('body')).toHaveCSS('color', 'rgb(230, 234, 242)');
  await page.emulateMedia({ colorScheme: 'light' });
  await expect(page.locator('body')).toHaveCSS('background-color', 'rgb(243, 245, 249)');
  await expect(page.locator('body')).toHaveCSS('color', 'rgb(20, 26, 38)');
});

test('a real extraction shows up as a running pass, then as cache', async ({ page }) => {
  test.skip(!state.mounted, 'FUSE is not usable here: the mount is needed to start a pass');
  const reader = spawn('cat', [parquet]);
  let size = 0;
  reader.stdout.on('data', (chunk) => (size += chunk.length));
  const finished = new Promise((resolve) => reader.on('exit', resolve));
  await page.goto('/?refresh=0.3'); // the pass over the small test pcap takes about 2 s: poll faster than the default 3 s to see it run
  await expect(page.getByTestId('s-run')).toHaveText('1 / 2', { timeout: 30_000 }); // after the batch window the pass leaves "waiting" and runs
  await expect(page.getByTestId('passes')).toContainText('tick 2026-11-20');
  await finished;
  expect(size).toBeGreaterThan(10_000);
  await expect(page.getByTestId('passes')).toContainText('No pass is running', { timeout: 20_000 });
  await expect(page.getByTestId('kinds')).toContainText('tick');
  await expect(page.getByTestId('s-cache')).not.toHaveText('0 B');
  await expect(page.getByTestId('s-fail')).toHaveText('0');
});

test('the performance tab draws charts and lists the finished pass', async ({ page }) => {
  if (state.mounted) fs.readFileSync(parquet); // a second read of the same file is a cache hit
  await page.goto('/#performance');
  for (const id of ['chart-net', 'chart-dsk']) {
    await expect(page.getByTestId(id).locator('svg path.ln')).not.toHaveCount(0, { timeout: 45_000 });
  }
  if (state.mounted) {
    await expect(page.getByTestId('chart-dur').locator('svg rect')).not.toHaveCount(0);
    await expect(page.getByTestId('done')).toContainText('tick 2026-11-20');
    await expect(page.getByTestId('done')).toContainText('ok');
    await expect(page.locator('#now-hit')).toContainText('%', { timeout: 45_000 });
  }
  await page.getByRole('button', { name: '5 min' }).first().click();
  await expect(page.getByRole('button', { name: '5 min' }).first()).toHaveAttribute('aria-pressed', 'true');
  await expect(page.getByRole('button', { name: '15 min' }).first()).toHaveAttribute('aria-pressed', 'false');
});

test('the load tab shows the host, CPU by group, slots and processes', async ({ page }) => {
  await page.goto('/#load');
  await expect(page.locator('#l-cpus')).toHaveText(/^\d+$/, { timeout: 30_000 });
  await expect(page.getByTestId('chart-cpu').locator('svg path.area')).not.toHaveCount(0, { timeout: 45_000 });
  await expect(page.getByTestId('chart-lav').locator('svg path.ln')).not.toHaveCount(0);
  await expect(page.getByTestId('slots')).toContainText('idle slot');
  await expect(page.locator('#lg-cpu')).toContainText('itch_tob');
  await expect(page.locator('#l-disk')).toHaveText(/%$/);
});

test('a value is edited in place, reviewed, saved to disk, and flagged for restart', async ({ page }) => {
  await page.goto('/#configuration');
  const save = page.getByRole('button', { name: 'Save config.toml' });
  await expect(save).toBeVisible();
  await expect(save).toBeDisabled();
  await expect(page.getByTestId('savemsg')).toContainText('No unsaved changes');

  const parallel = field(page, 'extract.max_parallel');
  await parallel.getByRole('button', { name: 'Edit max_parallel' }).click();
  const input = parallel.getByRole('textbox', { name: 'max_parallel' });
  await input.fill('abc');
  await expect(parallel).toContainText('Enter a whole number.');
  await input.press('Enter');
  await expect(input).toBeVisible(); // an invalid value does not close the editor
  await input.fill('3');
  await input.press('Enter');
  await expect(parallel.locator('.val')).toContainText('3');
  await expect(parallel).toContainText('was 2');
  await expect(page.locator('#cfgbadge')).toHaveText('1');
  await expect(save).toBeEnabled();
  expect(fs.readFileSync(state.configPath, 'utf8')).toContain('max_parallel = 2'); // nothing is written before Save

  await page.getByRole('button', { name: 'Review changes' }).click();
  await expect(page.getByTestId('diff')).toContainText('- max_parallel = 2');
  await expect(page.getByTestId('diff')).toContainText('+ max_parallel = 3');

  await save.click();
  await expect(page.locator('#toast')).toContainText('Saved config.toml. It takes effect after a restart.');
  expect(fs.readFileSync(state.configPath, 'utf8')).toContain('max_parallel = 3');
  await expect(page.getByTestId('restart-banner')).toContainText('docker compose restart');
  await expect(page.locator('#remountbtn')).toBeHidden(); // ui.allow_restart is off
  await expect(save).toBeDisabled();

  await page.goto('/#status');
  await expect(page.getByTestId('s-run')).toHaveText(/\/ 2$/); // the running process still has the old value
  await page.reload();
  await page.goto('/#configuration');
  await expect(field(page, 'extract.max_parallel').locator('.val')).toContainText('3');
  await expect(page.locator('#cfgbadge')).toBeHidden();
});

test('Escape cancels an edit', async ({ page }) => {
  await page.goto('/#configuration');
  const window = field(page, 'extract.batch_window_seconds');
  await window.getByRole('button', { name: 'Edit batch_window_seconds' }).click();
  const input = window.getByRole('textbox', { name: 'batch_window_seconds' });
  await input.fill('9');
  await input.press('Escape');
  await expect(window.locator('.val')).toContainText('0.3');
  await expect(page.locator('#cfgbadge')).toBeHidden();
});

test('a switch and a dropdown edit the file text, and Reset undoes them', async ({ page }) => {
  await page.goto('/#configuration');
  const wholeDay = page.getByRole('switch', { name: 'whole_day' });
  await expect(wholeDay).toHaveAttribute('aria-checked', 'false');
  await wholeDay.click();
  await expect(wholeDay).toHaveAttribute('aria-checked', 'true');
  const onMiss = field(page, 'extract.on_miss');
  await onMiss.getByRole('button', { name: 'Edit on_miss' }).click();
  await onMiss.getByRole('combobox').selectOption('async');
  await expect(onMiss).toContainText('EAGAIN'); // the advisory for async
  await expect(page.locator('#cfgbadge')).toHaveText('2');

  await page.getByRole('tab', { name: 'config.toml' }).click();
  const text = await page.locator('#raw').inputValue();
  expect(text).toContain('whole_day = true');
  expect(text).toContain('on_miss = "async"');
  expect(text).toContain('# config of the browser tests'); // the form edits keep the file's comments

  await page.getByRole('tab', { name: /Configuration/ }).click();
  await field(page, 'extract.whole_day').getByRole('button', { name: 'Reset' }).click();
  await field(page, 'extract.on_miss').getByRole('button', { name: 'Reset' }).click();
  await expect(page.locator('#cfgbadge')).toBeHidden();
  await expect(wholeDay).toHaveAttribute('aria-checked', 'false');
});

test('the raw editor flags syntax errors and the server rejects invalid values', async ({ page }) => {
  await page.goto('/#config-toml');
  const raw = page.locator('#raw');
  const original = await raw.inputValue();
  await expect(page.getByTestId('rawstate')).toHaveText('Saved');

  await raw.fill('[extract\nmax_parallel = 3');
  await expect(page.getByTestId('rawstate')).toHaveText('Syntax error');
  await expect(page.getByRole('button', { name: 'Save config.toml' })).toBeDisabled();
  await page.getByRole('tab', { name: /Configuration/ }).click();
  await expect(page.getByTestId('cfgerr')).toContainText('syntax error on line 1');

  await page.getByRole('tab', { name: 'config.toml' }).click();
  await raw.fill(original.replace('[cache]\n', '[cache]\nevict_to = 5\n'));
  await expect(page.getByTestId('rawstate')).toHaveText('Rejected'); // valid TOML, refused by the server's validation
  await expect(page.locator('#rawmsg')).toContainText('evict_to');
  await expect(page.getByRole('button', { name: 'Save config.toml' })).toBeDisabled();
  await page.getByRole('tab', { name: /Configuration/ }).click();
  await expect(page.getByTestId('cfgerr')).toContainText('The server rejected this config');

  await page.getByRole('button', { name: 'Discard' }).click();
  await expect(page.getByTestId('cfgerr')).toBeHidden();
  await page.getByRole('tab', { name: 'config.toml' }).click();
  await expect(page.getByTestId('rawstate')).toHaveText('Saved');
  await expect(raw).toHaveValue(original);
});

test('a save is refused when the file changed on disk, and Reload brings the new text', async ({ page }) => {
  await page.goto('/#configuration');
  const window = field(page, 'extract.batch_window_seconds');
  await window.getByRole('button', { name: 'Edit batch_window_seconds' }).click();
  await window.getByRole('textbox', { name: 'batch_window_seconds' }).fill('0.5');
  await window.getByRole('textbox', { name: 'batch_window_seconds' }).press('Enter');
  fs.appendFileSync(state.configPath, '\n# edited elsewhere\n');
  await page.getByRole('button', { name: 'Save config.toml' }).click();
  await expect(page.getByTestId('cfgerr')).toContainText('changed on disk');
  await page.getByRole('button', { name: 'Reload from disk' }).click();
  await expect(page.getByTestId('cfgerr')).toBeHidden();
  await expect(window.locator('.val')).toContainText('0.3');
  await expect(page.locator('#cfgbadge')).toBeHidden();
  await page.getByRole('tab', { name: 'config.toml' }).click();
  await expect(page.locator('#raw')).toHaveValue(/# edited elsewhere/);
});

test('the API refuses cross-origin writes and exposes the documented shape', async ({ request }) => {
  const status = await (await request.get('/api/status')).json();
  expect(Object.keys(status)).toEqual(expect.arrayContaining(['mount', 'passes', 'cache', 'listing', 'settings']));
  expect(status.passes.slots).toBe(2);
  const config = await (await request.get('/api/config')).json();
  expect(config.text).toContain('[ui]');
  expect(config.etag).not.toBe(config.applied_etag); // saved above, not yet restarted
  const crossOrigin = await request.put('/api/config', { headers: { 'X-Requested-With': 'datafeedfs-ui', Origin: 'http://evil.test' }, data: { text: config.text, etag: config.etag } });
  expect(crossOrigin.status()).toBe(403);
  const noHeader = await request.put('/api/config', { data: { text: config.text, etag: config.etag } });
  expect(noHeader.status()).toBe(403);
  const restart = await request.post('/api/restart', { headers: { 'X-Requested-With': 'datafeedfs-ui' } });
  expect(restart.status()).toBe(403);
});
