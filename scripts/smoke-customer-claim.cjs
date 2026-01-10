/* eslint-disable no-console */
const path = require('path');
const { createServer } = require('../app.cjs');

async function main() {
  const host = '127.0.0.1';
  const port = 0;
  const dataDir = path.join(__dirname, '..', '..', '..', '.tmp-smoke');

  const server = await createServer({ host, port, isPackaged: false, distDir: null, dataDir });
  const address = server.server.address();
  const baseUrl = `http://${host}:${address.port}`;

  async function req(method, urlPath, body) {
    const res = await fetch(`${baseUrl}${urlPath}`, {
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
    const itemsRes = await req('GET', '/api/items');
    if (itemsRes.status !== 200 || !Array.isArray(itemsRes.json) || itemsRes.json.length === 0) {
      throw new Error('No items available');
    }
    const item = itemsRes.json[0];

    const customersRes = await req('GET', '/api/customers');
    const customer = Array.isArray(customersRes.json) && customersRes.json.length ? customersRes.json[0] : null;

    const now = new Date();
    const yyyy = now.getFullYear();
    const mm = String(now.getMonth() + 1).padStart(2, '0');
    const dd = String(now.getDate()).padStart(2, '0');
    const tanggal = `${yyyy}-${mm}-${dd}`;

    // Ensure stock exists by doing a stock-in of 1
    const noIn = `PO-CLAIM-SMOKE-${Date.now()}`;
    await req('POST', '/api/stock-in', {
      no_faktur: noIn,
      tanggal,
      kode_supplier: null,
      catatan: 'seed for claim',
      items: [{ kode_barang: item.kode_barang, jumlah: 1 }],
    });

    const noClaim = `CC-SMOKE-${Date.now()}`;
    const claim = await req('POST', '/api/customer-claims', {
      no_claim: noClaim,
      tanggal,
      kode_customer: customer?.kode_customer || null,
      catatan: 'smoke claim',
      items: [{ kode_barang: item.kode_barang, jumlah: 1 }],
    });
    console.log('claim', claim);

    const ledger = await req('GET', `/api/ledger?kode_barang=${encodeURIComponent(item.kode_barang)}&limit=10`);
    console.log('ledger', ledger);
  } finally {
    await server.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
