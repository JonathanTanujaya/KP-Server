const path = require('path');
const { createServer } = require('../app.cjs');

async function req(base, method, route, body) {
  const res = await fetch(base + route, {
    method,
    headers: body ? { 'content-type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });

  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = text;
  }

  return { status: res.status, json };
}

(async () => {
  const dataDir = path.join(__dirname, '..', '..', '..', '.localdata-smoke2');

  const fastify = await createServer({
    host: '127.0.0.1',
    port: 0,
    isPackaged: false,
    distDir: path.join(__dirname, '..', '..', '..', 'dist'),
    dataDir,
  });

  const port = fastify.server.address().port;
  const base = `http://127.0.0.1:${port}/api`;

  console.log('health', await req(base, 'GET', '/health'));

  console.log(
    'create kategori',
    await req(base, 'POST', '/categories', { kode_kategori: 'KATZZZ', nama_kategori: 'Kategori Z' })
  );
  console.log('update kategori', await req(base, 'PUT', '/categories/KATZZZ', { nama_kategori: 'Kategori Z Updated' }));
  console.log('delete kategori', await req(base, 'DELETE', '/categories/KATZZZ'));

  console.log('create area', await req(base, 'POST', '/areas', { kode_area: 'AREAZZ', nama_area: 'Area Z' }));
  console.log(
    'create customer',
    await req(base, 'POST', '/customers', {
      kode_customer: 'CUSZZZ',
      nama_customer: 'Customer Z',
      kode_area: 'AREAZZ',
    })
  );
  console.log('delete area (should 409)', await req(base, 'DELETE', '/areas/AREAZZ'));
  console.log('delete customer', await req(base, 'DELETE', '/customers/CUSZZZ'));
  console.log('delete area', await req(base, 'DELETE', '/areas/AREAZZ'));

  await fastify.close();
  console.log('done');
})().catch((e) => {
  console.error(e);
  process.exit(1);
});
