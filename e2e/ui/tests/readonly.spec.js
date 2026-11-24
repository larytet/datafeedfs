// Console only, editing off, token required (port 18089, see ../start-server.js).
const { test, expect } = require('@playwright/test');

const TOKEN = 's3cret';
const auth = { Authorization: `Bearer ${TOKEN}`, 'X-Requested-With': 'datafeedfs-ui' };

async function unlock(page, hash = '') {
  await page.goto('/' + hash);
  await page.locator('#tokeninp').fill(TOKEN);
  await page.locator('#tokeninp').press('Enter');
  await expect(page.locator('#lock')).toBeHidden();
}

test('the console asks for the token and rejects a wrong one', async ({ page }) => {
  await page.goto('/');
  await expect(page.getByRole('heading', { name: 'This console needs a token' })).toBeVisible();
  await page.locator('#tokeninp').fill('nope');
  await page.locator('#tokeninp').press('Enter');
  await expect(page.locator('#lockerr')).toHaveText('Wrong token.');
  await page.locator('#tokeninp').fill(TOKEN);
  await page.locator('#tokeninp').press('Enter');
  await expect(page.locator('#lock')).toBeHidden();
  await expect(page.getByTestId('mount-pill')).toContainText('Console only');
  await expect(page.locator('#editpill')).toHaveText('Read-only console');
  await expect(page.getByTestId('s-run')).toHaveText('0 / 2');
});

test('the token survives a reload in the same tab', async ({ page }) => {
  await unlock(page);
  await page.reload();
  await expect(page.locator('#lock')).toBeHidden();
  await expect(page.getByTestId('s-run')).toHaveText('0 / 2');
});

test('fields, the raw editor and the save bar are read-only', async ({ page }) => {
  await unlock(page, '#configuration');
  await expect(page.locator('#cfgro')).toBeVisible();
  await expect(page.locator('[data-edit]')).toHaveCount(0);
  await expect(page.locator('.val.ro').first()).toBeVisible();
  await expect(page.getByRole('switch', { name: 'whole_day' })).toBeDisabled();
  await expect(page.locator('#savebar')).toBeHidden();
  await page.getByRole('tab', { name: 'config.toml' }).click();
  await expect(page.locator('#raw')).toHaveAttribute('readonly', '');
  await expect(page.locator('#raw')).toHaveValue(/allow_edit = false/);
  await expect(page.locator('#savebar')).toBeHidden();
});

test('the token field shows a mask, not the secret', async ({ page }) => {
  await unlock(page, '#configuration');
  const token = page.getByTestId('field-ui.token');
  await expect(token).toContainText('••••••••');
  await expect(token).not.toContainText(TOKEN);
});

test('the API needs the token and refuses writes while editing is off', async ({ request }) => {
  expect((await request.get('/api/status')).status()).toBe(401);
  expect((await request.get('/api/status', { headers: { Authorization: 'Bearer wrong' } })).status()).toBe(401);
  expect((await request.get('/api/status', { headers: auth })).status()).toBe(200);
  const config = await (await request.get('/api/config', { headers: auth })).json();
  const put = await request.put('/api/config', { headers: auth, data: { text: config.text, etag: config.etag } });
  expect(put.status()).toBe(403);
  expect((await put.json()).error).toContain('allow_edit');
  const restart = await request.post('/api/restart', { headers: auth });
  expect(restart.status()).toBe(403);
  expect((await request.get('/api/info')).status()).toBe(200); // the page needs this before it has a token
});
