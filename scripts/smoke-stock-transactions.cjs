/* eslint-disable no-console */
const { createServer } = require('../app.cjs');

async function main() {
  const host = '127.0.0.1';
  const port = 0;
  const dataDir = require('path').join(__dirname, '..', '..', '..', '.tmp-smoke');

  const server = await createServer({ host, port, isPackaged: false, distDir: null, dataDir });
  const address = server.server.address();
  const baseUrl = `http://${host}:${address.port}`;

  async function req(method, path, body) {
    const res = await fetch(`${baseUrl}${path}`, {
      method,
      headers: body ? { 'content-type': 'application/json' } : undefined,
      body: body ? JSON.stringify(body) : undefined,
    });
    const text = await res.text();
    let json;
    try {
      json = text ? JSON.parse(text) : null;
    } catch {
      json = text;
    }
    return { status: res.status, json };
  }

  try {
    const health = await req('GET', '/api/health');
    console.log('health', health);

    // Ensure we have at least one item
    const items = await req('GET', '/api/items');
    if (items.status !== 200 || !Array.isArray(items.json) || items.json.length === 0) {
      throw new Error('No items available for smoke test');
    }
    const item = items.json[0];

    const now = new Date();
    const yyyy = now.getFullYear();
    const mm = String(now.getMonth() + 1).padStart(2, '0');
    const dd = String(now.getDate()).padStart(2, '0');
    const tanggal = `${yyyy}-${mm}-${dd}`;

    const noIn = `PO-SMOKE-${Date.now()}`;
    const inRes = await req('POST', '/api/stock-in', {
      no_faktur: noIn,
      tanggal,
      kode_supplier: null,
      catatan: 'smoke stock-in',
      items: [{ kode_barang: item.kode_barang, jumlah: 2 }],
    });
    console.log('stock-in', inRes);

    const noOut = `SL-SMOKE-${Date.now()}`;
    const outRes = await req('POST', '/api/stock-out', {
      no_faktur: noOut,
      tanggal,
      kode_customer: null,
      catatan: 'smoke stock-out',
      items: [{ kode_barang: item.kode_barang, jumlah: 1 }],
    });
    console.log('stock-out', outRes);

    const opnameNo = `SO-SMOKE-${Date.now()}`;
    const afterItem = (await req('GET', '/api/items')).json.find((x) => x.kode_barang === item.kode_barang);
    const opnameRes = await req('POST', '/api/stock-opname', {
      no_opname: opnameNo,
      tanggal_opname: tanggal,
      catatan: 'smoke opname',
      detail_items: [{ kode_barang: item.kode_barang, stok_fisik: Number(afterItem.stok) }],
    });
    console.log('stock-opname', opnameRes);

    const ledger = await req('GET', `/api/ledger?kode_barang=${encodeURIComponent(item.kode_barang)}&limit=20`);
    console.log('ledger', ledger);
  } finally {
    await server.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
