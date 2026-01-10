const path = require('path');
const fs = require('fs');
const os = require('os');

const { createServer } = require('../app.cjs');

async function fetchJson(url, { method = 'GET', headers, body } = {}) {
  const res = await fetch(url, {
    method,
    headers: {
      'content-type': 'application/json',
      ...(headers || {}),
    },
    body: body ? JSON.stringify(body) : undefined,
  });

  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : undefined;
  } catch (_) {
    json = text;
  }
  return { status: res.status, json };
}

async function main() {
  const dataDir = path.join(process.cwd(), 'electron/server/.tmp-smoke-auth');
  fs.rmSync(dataDir, { recursive: true, force: true });
  fs.mkdirSync(dataDir, { recursive: true });

  const fastify = await createServer({
    host: '127.0.0.1',
    port: 0,
    isPackaged: false,
    distDir: null,
    dataDir,
  });

  const address = fastify.server.address();
  const baseUrl = `http://127.0.0.1:${address.port}/api`;

  try {
    const login = await fetchJson(`${baseUrl}/auth/login`, {
      method: 'POST',
      body: { username: 'owner', password: 'owner' },
    });
    console.log('login', login);
    if (login.status !== 200) throw new Error('login failed');

    const token = login.json.token;
    const authHeader = { Authorization: `Bearer ${token}` };

    const list1 = await fetchJson(`${baseUrl}/users`, { headers: authHeader });
    console.log('users:list', list1.status, Array.isArray(list1.json) ? list1.json.length : list1.json);

    const uname = `u_${Date.now()}`;
    const created = await fetchJson(`${baseUrl}/users`, {
      method: 'POST',
      headers: authHeader,
      body: { username: uname, password: 'abcd', nama: 'User Smoke', role: 'staff', avatar: '#22c55e' },
    });
    console.log('users:create', created);
    if (created.status !== 201) throw new Error('create user failed');

    const reset = await fetchJson(`${baseUrl}/users/${created.json.id}/reset-password`, {
      method: 'POST',
      headers: authHeader,
      body: { newPassword: 'abcd2' },
    });
    console.log('users:reset-password', reset);
    if (reset.status !== 200) throw new Error('reset password failed');

    const loginNew = await fetchJson(`${baseUrl}/auth/login`, {
      method: 'POST',
      body: { username: uname, password: 'abcd2' },
    });
    console.log('login:new-user', loginNew);
    if (loginNew.status !== 200) throw new Error('new user login failed');

    const changeSelf = await fetchJson(`${baseUrl}/auth/change-password`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${loginNew.json.token}` },
      body: { currentPassword: 'abcd2', newPassword: 'abcd3' },
    });
    console.log('auth:change-password', changeSelf);
    if (changeSelf.status !== 200) throw new Error('change password failed');

    const loginNew2 = await fetchJson(`${baseUrl}/auth/login`, {
      method: 'POST',
      body: { username: uname, password: 'abcd3' },
    });
    console.log('login:new-user-after-change', loginNew2.status);
    if (loginNew2.status !== 200) throw new Error('login after change failed');

    console.log('SMOKE OK');
  } finally {
    await fastify.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
