function parseIntOrNull(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

function registerStockOpnameRoutes(fastify, { db }) {
  fastify.get('/api/stock-opname', async (request) => {
    const limit = Math.min(500, Math.max(1, Number(request.query?.limit ?? 100)));
    return db.all(
      `SELECT id,
              no_opname,
              tanggal,
              catatan,
              created_at,
              updated_at
       FROM t_stok_opname
       ORDER BY tanggal DESC, id DESC
       LIMIT ?`,
      [limit]
    );
  });

  fastify.get('/api/stock-opname/:id', async (request, reply) => {
    const id = Number(request.params?.id);
    if (!Number.isFinite(id)) return reply.code(400).send({ error: 'id is required' });

    const header = db.get(
      `SELECT id,
              no_opname,
              tanggal,
              catatan,
              created_at,
              updated_at
       FROM t_stok_opname
       WHERE id = ?`,
      [id]
    );
    if (!header) return reply.code(404).send({ error: 'not found' });

    const items = db.all(
      `SELECT d.id,
              d.barang_kode AS kode_barang,
              b.nama_barang,
              b.satuan,
              d.stok_sistem,
              d.stok_fisik,
              d.selisih,
              d.keterangan
       FROM t_stok_opname_detail d
       JOIN m_barang b ON b.kode_barang = d.barang_kode
       WHERE d.stok_opname_id = ?
       ORDER BY d.id ASC`,
      [id]
    );

    return reply.send({ ...header, items });
  });

  fastify.post('/api/stock-opname', async (request, reply) => {
    const body = request.body || {};

    const no_opname = String(body.no_opname ?? '').trim();
    const tanggal = String(body.tanggal_opname ?? body.tanggal ?? '').trim();
    const catatan = body.catatan != null ? String(body.catatan) : null;

    const detail_items = Array.isArray(body.detail_items) ? body.detail_items : Array.isArray(body.items) ? body.items : [];

    if (!no_opname) return reply.code(400).send({ error: 'no_opname is required' });
    if (!tanggal) return reply.code(400).send({ error: 'tanggal is required' });
    if (detail_items.length === 0) return reply.code(400).send({ error: 'detail_items is required' });

    try {
      const result = db.transaction((tx) => {
        const headerRes = tx.run(
          `INSERT INTO t_stok_opname (no_opname, tanggal, catatan)
           VALUES (?, ?, ?)`,
          [no_opname, tanggal, catatan]
        );
        const headerId = headerRes.lastInsertRowid;

        for (const it of detail_items) {
          const kode_barang = String(it.kode_barang ?? '').trim();
          const stok_fisik = parseIntOrNull(it.stok_fisik);
          const keterangan = it.keterangan != null ? String(it.keterangan) : null;

          if (!kode_barang) throw new Error('kode_barang is required');
          if (stok_fisik == null) throw new Error('stok_fisik is required');
          if (stok_fisik < 0) {
            const e = new Error('stok_fisik must be >= 0');
            e.statusCode = 400;
            e.meta = { kode_barang, stok_fisik };
            throw e;
          }

          const barang = tx.get('SELECT stok FROM m_barang WHERE kode_barang = ?', [kode_barang]);
          if (!barang) {
            const e = new Error('kode_barang not found');
            e.statusCode = 400;
            e.meta = { kode_barang };
            throw e;
          }

          const stok_sistem = Number(barang.stok) || 0;
          const selisih = stok_fisik - stok_sistem;

          tx.run(
            `INSERT INTO t_stok_opname_detail (stok_opname_id, barang_kode, stok_sistem, stok_fisik, selisih, keterangan)
             VALUES (?, ?, ?, ?, ?, ?)`,
            [headerId, kode_barang, stok_sistem, stok_fisik, selisih, keterangan]
          );

          // Apply adjustment to item stock
          tx.run('UPDATE m_barang SET stok = ? WHERE kode_barang = ?', [stok_fisik, kode_barang]);

          const qty_in = selisih > 0 ? selisih : 0;
          const qty_out = selisih < 0 ? Math.abs(selisih) : 0;

          tx.run(
            `INSERT INTO t_kartu_stok (waktu, ref_type, ref_no, barang_kode, qty_in, qty_out, stok_after, keterangan)
             VALUES (?, 'ADJ', ?, ?, ?, ?, ?, ?)`,
            [tanggal, no_opname, kode_barang, qty_in, qty_out, stok_fisik, keterangan || catatan]
          );
        }

        return { headerId };
      });

      const created = db.get(
        `SELECT id,
                no_opname,
                tanggal,
                catatan,
                created_at,
                updated_at
         FROM t_stok_opname
         WHERE id = ?`,
        [result.headerId]
      );

      return reply.code(201).send(created);
    } catch (err) {
      const msg = String(err?.message || 'internal error');
      if (msg.includes('UNIQUE')) return reply.code(409).send({ error: 'no_opname already exists' });
      if (err?.statusCode) return reply.code(err.statusCode).send({ error: msg, ...(err.meta ? { meta: err.meta } : {}) });
      fastify.log.error(err);
      return reply.code(500).send({ error: 'internal error' });
    }
  });
}

module.exports = { registerStockOpnameRoutes };
