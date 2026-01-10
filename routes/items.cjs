function registerItemRoutes(fastify, { db }) {
  fastify.get('/api/items', async () => {
    return db.all(
      `SELECT id,
              kode_barang,
              nama_barang,
              kategori_kode AS kategori_id,
              satuan,
              stok,
              stok_minimal,
              harga_beli,
              harga_jual,
              created_at,
              updated_at
       FROM m_barang
       ORDER BY nama_barang ASC`
    );
  });

  fastify.put('/api/items/:kode_barang', async (request, reply) => {
    const kodeParam = String(request.params?.kode_barang ?? '').trim();
    const body = request.body || {};

    const kodeBody = body.kode_barang;
    const nama_barang = body.nama_barang;
    const kategori_id = body.kategori_id ?? null;
    const satuan = body.satuan ?? null;
    const stok = body.stok;
    const stok_minimal = body.stok_minimal;
    const harga_beli = body.harga_beli ?? null;
    const harga_jual = body.harga_jual ?? null;

    if (!kodeParam) return reply.code(400).send({ error: 'kode_barang is required' });
    if (kodeBody != null && String(kodeBody).trim() && String(kodeBody).trim() !== kodeParam) {
      return reply.code(400).send({ error: 'kode_barang cannot be changed' });
    }
    if (!nama_barang || typeof nama_barang !== 'string' || !nama_barang.trim()) {
      return reply.code(400).send({ error: 'nama_barang is required' });
    }

    const exists = db.get('SELECT id FROM m_barang WHERE kode_barang = ?', [kodeParam]);
    if (!exists) return reply.code(404).send({ error: 'not found' });

    if (kategori_id) {
      const katExists = db.get('SELECT id FROM m_kategori WHERE kode = ?', [String(kategori_id).trim()]);
      if (!katExists) return reply.code(400).send({ error: 'kategori_id not found' });
    }

    const stokNum = stok == null ? undefined : Number(stok);
    const stokMinNum = stok_minimal == null ? undefined : Number(stok_minimal);
    if (stokNum != null && Number.isFinite(stokNum) && stokNum < 0) {
      return reply.code(400).send({ error: 'stok must be >= 0' });
    }
    if (stokMinNum != null && Number.isFinite(stokMinNum) && stokMinNum < 0) {
      return reply.code(400).send({ error: 'stok_minimal must be >= 0' });
    }

    try {
      // Keep existing values when a field is omitted
      const current = db.get(
        'SELECT kategori_kode, satuan, stok, stok_minimal, harga_beli, harga_jual FROM m_barang WHERE kode_barang = ?',
        [kodeParam]
      );

      db.run(
        `UPDATE m_barang
         SET nama_barang = ?,
             kategori_kode = ?,
             satuan = ?,
             stok = ?,
             stok_minimal = ?,
             harga_beli = ?,
             harga_jual = ?
         WHERE kode_barang = ?`,
        [
          nama_barang.trim(),
          kategori_id ? String(kategori_id).trim() : null,
          satuan ?? current.satuan,
          stokNum == null || !Number.isFinite(stokNum) ? current.stok : stokNum,
          stokMinNum == null || !Number.isFinite(stokMinNum) ? current.stok_minimal : stokMinNum,
          harga_beli ?? current.harga_beli,
          harga_jual ?? current.harga_jual,
          kodeParam,
        ]
      );

      const updated = db.get(
        `SELECT id,
                kode_barang,
                nama_barang,
                kategori_kode AS kategori_id,
                satuan,
                stok,
                stok_minimal,
                harga_beli,
                harga_jual,
                created_at,
                updated_at
         FROM m_barang
         WHERE kode_barang = ?`,
        [kodeParam]
      );

      return reply.send(updated);
    } catch (err) {
      fastify.log.error(err);
      return reply.code(500).send({ error: 'internal error' });
    }
  });

  fastify.delete('/api/items/:kode_barang', async (request, reply) => {
    const kodeParam = String(request.params?.kode_barang ?? '').trim();
    if (!kodeParam) return reply.code(400).send({ error: 'kode_barang is required' });

    const exists = db.get('SELECT id FROM m_barang WHERE kode_barang = ?', [kodeParam]);
    if (!exists) return reply.code(404).send({ error: 'not found' });

    try {
      db.run('DELETE FROM m_barang WHERE kode_barang = ?', [kodeParam]);
      return reply.code(204).send();
    } catch (err) {
      fastify.log.error(err);
      return reply.code(500).send({ error: 'internal error' });
    }
  });

  fastify.post('/api/items', async (request, reply) => {
    const body = request.body || {};

    const kode_barang = body.kode_barang;
    const nama_barang = body.nama_barang;
    const kategori_id = body.kategori_id ?? null;
    const satuan = body.satuan ?? null;
    const stok = Number.isFinite(body.stok) ? body.stok : Number(body.stok ?? 0);
    const stok_minimal = Number.isFinite(body.stok_minimal) ? body.stok_minimal : Number(body.stok_minimal ?? 0);
    const harga_beli = body.harga_beli ?? null;
    const harga_jual = body.harga_jual ?? null;

    if (!kode_barang || typeof kode_barang !== 'string' || !kode_barang.trim()) {
      return reply.code(400).send({ error: 'kode_barang is required' });
    }

    if (!nama_barang || typeof nama_barang !== 'string' || !nama_barang.trim()) {
      return reply.code(400).send({ error: 'nama_barang is required' });
    }

    try {
      const result = db.run(
        `INSERT INTO m_barang (kode_barang, nama_barang, kategori_kode, satuan, stok, stok_minimal, harga_beli, harga_jual)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [
          kode_barang.trim(),
          nama_barang.trim(),
          kategori_id,
          satuan,
          Number.isFinite(stok) ? stok : 0,
          Number.isFinite(stok_minimal) ? stok_minimal : 0,
          harga_beli,
          harga_jual,
        ]
      );

      const created = db.get(
        `SELECT id,
                kode_barang,
                nama_barang,
                kategori_kode AS kategori_id,
                satuan,
                stok,
                stok_minimal,
                harga_beli,
                harga_jual,
                created_at,
                updated_at
         FROM m_barang
         WHERE id = ?`,
        [result.lastInsertRowid]
      );

      return reply.code(201).send(created);
    } catch (err) {
      if (err && String(err.message || '').includes('UNIQUE')) {
        return reply.code(409).send({ error: 'kode_barang already exists' });
      }
      fastify.log.error(err);
      return reply.code(500).send({ error: 'internal error' });
    }
  });
}

module.exports = { registerItemRoutes };
