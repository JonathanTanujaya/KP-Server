function parsePositiveInt(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  const intVal = Math.trunc(n);
  if (intVal <= 0) return null;
  return intVal;
}

function registerCustomerClaimRoutes(fastify, { db }) {
  fastify.get('/api/customer-claims', async (request) => {
    const limit = Math.min(500, Math.max(1, Number(request.query?.limit ?? 100)));
    return db.all(
      `SELECT id,
              no_claim,
              tanggal,
              customer_kode,
              catatan,
              created_at,
              updated_at
       FROM t_customer_claim
       ORDER BY tanggal DESC, id DESC
       LIMIT ?`,
      [limit]
    );
  });

  fastify.get('/api/customer-claims/:id', async (request, reply) => {
    const id = Number(request.params?.id);
    if (!Number.isFinite(id)) return reply.code(400).send({ error: 'id is required' });

    const header = db.get(
      `SELECT id,
              no_claim,
              tanggal,
              customer_kode,
              catatan,
              created_at,
              updated_at
       FROM t_customer_claim
       WHERE id = ?`,
      [id]
    );
    if (!header) return reply.code(404).send({ error: 'not found' });

    const items = db.all(
      `SELECT d.id,
              d.barang_kode AS kode_barang,
              b.nama_barang,
              b.satuan,
              d.qty AS jumlah
       FROM t_customer_claim_detail d
       JOIN m_barang b ON b.kode_barang = d.barang_kode
       WHERE d.customer_claim_id = ?
       ORDER BY d.id ASC`,
      [id]
    );

    return reply.send({ ...header, items });
  });

  fastify.post('/api/customer-claims', async (request, reply) => {
    const body = request.body || {};
    const no_claim = String(body.no_claim ?? '').trim();
    const tanggal = String(body.tanggal ?? '').trim();
    const customer_kode = body.kode_customer != null ? String(body.kode_customer).trim() : body.customer_kode != null ? String(body.customer_kode).trim() : null;
    const catatan = body.catatan != null ? String(body.catatan) : null;
    const items = Array.isArray(body.items) ? body.items : [];

    if (!no_claim) return reply.code(400).send({ error: 'no_claim is required' });
    if (!tanggal) return reply.code(400).send({ error: 'tanggal is required' });
    if (items.length === 0) return reply.code(400).send({ error: 'items is required' });

    if (customer_kode) {
      const cust = db.get('SELECT id FROM m_customer WHERE kode = ?', [customer_kode]);
      if (!cust) return reply.code(400).send({ error: 'kode_customer not found' });
    }

    try {
      const result = db.transaction((tx) => {
        const headerRes = tx.run(
          `INSERT INTO t_customer_claim (no_claim, tanggal, customer_kode, catatan)
           VALUES (?, ?, ?, ?)`,
          [no_claim, tanggal, customer_kode || null, catatan]
        );
        const headerId = headerRes.lastInsertRowid;

        for (const it of items) {
          const kode_barang = String(it.kode_barang ?? '').trim();
          const jumlah = parsePositiveInt(it.jumlah ?? it.qty);

          if (!kode_barang) throw new Error('kode_barang is required');
          if (!jumlah) throw new Error('jumlah must be > 0');

          const barang = tx.get('SELECT stok FROM m_barang WHERE kode_barang = ?', [kode_barang]);
          if (!barang) {
            const e = new Error('kode_barang not found');
            e.statusCode = 400;
            e.meta = { kode_barang };
            throw e;
          }

          if (Number(barang.stok) < jumlah) {
            const e = new Error('stok tidak mencukupi');
            e.statusCode = 409;
            e.meta = { kode_barang, stok: barang.stok, diminta: jumlah };
            throw e;
          }

          tx.run(
            `INSERT INTO t_customer_claim_detail (customer_claim_id, barang_kode, qty)
             VALUES (?, ?, ?)`,
            [headerId, kode_barang, jumlah]
          );

          tx.run('UPDATE m_barang SET stok = stok - ? WHERE kode_barang = ?', [jumlah, kode_barang]);

          const after = tx.get('SELECT stok FROM m_barang WHERE kode_barang = ?', [kode_barang]);
          tx.run(
            `INSERT INTO t_kartu_stok (waktu, ref_type, ref_no, barang_kode, qty_in, qty_out, stok_after, keterangan)
             VALUES (?, 'CLAIM_OUT', ?, ?, 0, ?, ?, ?)`,
            [tanggal, no_claim, kode_barang, jumlah, after?.stok ?? null, catatan]
          );
        }

        return { headerId };
      });

      const created = db.get(
        `SELECT id,
                no_claim,
                tanggal,
                customer_kode,
                catatan,
                created_at,
                updated_at
         FROM t_customer_claim
         WHERE id = ?`,
        [result.headerId]
      );

      return reply.code(201).send(created);
    } catch (err) {
      const msg = String(err?.message || 'internal error');
      if (msg.includes('UNIQUE')) return reply.code(409).send({ error: 'no_claim already exists' });
      if (err?.statusCode) return reply.code(err.statusCode).send({ error: msg, ...(err.meta ? { meta: err.meta } : {}) });
      fastify.log.error(err);
      return reply.code(500).send({ error: 'internal error' });
    }
  });
}

module.exports = { registerCustomerClaimRoutes };
