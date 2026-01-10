function registerCategoryRoutes(fastify, { db }) {
  fastify.get('/api/categories', async () => {
    return db.all(
      'SELECT id, kode AS kode_kategori, nama AS nama_kategori, created_at, updated_at FROM m_kategori ORDER BY nama ASC'
    );
  });

  fastify.put('/api/categories/:kode', async (request, reply) => {
    const kodeParam = String(request.params?.kode ?? '').trim();
    const body = request.body || {};
    const kodeBody = body.kode_kategori ?? body.kode;
    const nama = body.nama_kategori ?? body.nama;

    if (!kodeParam) return reply.code(400).send({ error: 'kode_kategori is required' });

    if (kodeBody != null && String(kodeBody).trim() && String(kodeBody).trim() !== kodeParam) {
      return reply.code(400).send({ error: 'kode_kategori cannot be changed' });
    }

    if (!nama || typeof nama !== 'string' || !nama.trim()) {
      return reply.code(400).send({ error: 'nama_kategori is required' });
    }

    const exists = db.get('SELECT id FROM m_kategori WHERE kode = ?', [kodeParam]);
    if (!exists) return reply.code(404).send({ error: 'not found' });

    try {
      db.run('UPDATE m_kategori SET nama = ? WHERE kode = ?', [nama.trim(), kodeParam]);
      const updated = db.get(
        'SELECT id, kode AS kode_kategori, nama AS nama_kategori, created_at, updated_at FROM m_kategori WHERE kode = ?',
        [kodeParam]
      );
      return reply.send(updated);
    } catch (err) {
      fastify.log.error(err);
      return reply.code(500).send({ error: 'internal error' });
    }
  });

  fastify.delete('/api/categories/:kode', async (request, reply) => {
    const kodeParam = String(request.params?.kode ?? '').trim();
    if (!kodeParam) return reply.code(400).send({ error: 'kode_kategori is required' });

    const exists = db.get('SELECT id FROM m_kategori WHERE kode = ?', [kodeParam]);
    if (!exists) return reply.code(404).send({ error: 'not found' });

    const usedByItems = Number(
      db.get('SELECT COUNT(*) AS cnt FROM m_barang WHERE kategori_kode = ?', [kodeParam])?.cnt ?? 0
    );
    if (usedByItems > 0) {
      return reply.code(409).send({ error: 'kategori is used by items' });
    }

    try {
      db.run('DELETE FROM m_kategori WHERE kode = ?', [kodeParam]);
      return reply.code(204).send();
    } catch (err) {
      fastify.log.error(err);
      return reply.code(500).send({ error: 'internal error' });
    }
  });

  fastify.post('/api/categories', async (request, reply) => {
    const body = request.body || {};
    const kode = body.kode_kategori ?? body.kode;
    const nama = body.nama_kategori ?? body.nama;

    if (!kode || typeof kode !== 'string' || !kode.trim()) {
      return reply.code(400).send({ error: 'kode_kategori is required' });
    }

    if (!nama || typeof nama !== 'string' || !nama.trim()) {
      return reply.code(400).send({ error: 'nama_kategori is required' });
    }

    try {
      const result = db.run('INSERT INTO m_kategori (kode, nama) VALUES (?, ?)', [kode.trim(), nama.trim()]);
      const created = db.get(
        'SELECT id, kode AS kode_kategori, nama AS nama_kategori, created_at, updated_at FROM m_kategori WHERE id = ?',
        [result.lastInsertRowid]
      );
      return reply.code(201).send(created);
    } catch (err) {
      if (err && String(err.message || '').includes('UNIQUE')) {
        return reply.code(409).send({ error: 'kode_kategori already exists' });
      }
      fastify.log.error(err);
      return reply.code(500).send({ error: 'internal error' });
    }
  });
}

module.exports = { registerCategoryRoutes };
