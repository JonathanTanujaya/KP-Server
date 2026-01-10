function registerAreaRoutes(fastify, { db }) {
  fastify.get('/api/areas', async () => {
    const rows = db.all(
      'SELECT id, kode AS kode_area, nama AS nama_area, created_at, updated_at FROM m_area ORDER BY nama ASC'
    );
    return rows;
  });

  fastify.put('/api/areas/:kode', async (request, reply) => {
    const kodeParam = String(request.params?.kode ?? '').trim();
    const body = request.body || {};
    const kodeBody = body.kode_area ?? body.kode;
    const nama = body.nama_area ?? body.nama;

    if (!kodeParam) {
      return reply.code(400).send({ error: 'kode is required' });
    }

    if (kodeBody != null && String(kodeBody).trim() && String(kodeBody).trim() !== kodeParam) {
      return reply.code(400).send({ error: 'kode cannot be changed' });
    }

    if (!nama || typeof nama !== 'string' || !nama.trim()) {
      return reply.code(400).send({ error: 'nama is required' });
    }

    const exists = db.get('SELECT id FROM m_area WHERE kode = ?', [kodeParam]);
    if (!exists) return reply.code(404).send({ error: 'not found' });

    try {
      db.run('UPDATE m_area SET nama = ? WHERE kode = ?', [nama.trim(), kodeParam]);
      const updated = db.get(
        'SELECT id, kode AS kode_area, nama AS nama_area, created_at, updated_at FROM m_area WHERE kode = ?',
        [kodeParam]
      );
      return reply.send(updated);
    } catch (err) {
      fastify.log.error(err);
      return reply.code(500).send({ error: 'internal error' });
    }
  });

  fastify.delete('/api/areas/:kode', async (request, reply) => {
    const kodeParam = String(request.params?.kode ?? '').trim();
    if (!kodeParam) return reply.code(400).send({ error: 'kode is required' });

    const exists = db.get('SELECT id FROM m_area WHERE kode = ?', [kodeParam]);
    if (!exists) return reply.code(404).send({ error: 'not found' });

    const usedByCustomers = Number(
      db.get('SELECT COUNT(*) AS cnt FROM m_customer WHERE area_kode = ?', [kodeParam])?.cnt ?? 0
    );
    if (usedByCustomers > 0) {
      return reply.code(409).send({ error: 'area is used by customers' });
    }

    try {
      db.run('DELETE FROM m_area WHERE kode = ?', [kodeParam]);
      return reply.code(204).send();
    } catch (err) {
      fastify.log.error(err);
      return reply.code(500).send({ error: 'internal error' });
    }
  });

  fastify.post('/api/areas', async (request, reply) => {
    const body = request.body || {};
    const kode = body.kode_area ?? body.kode;
    const nama = body.nama_area ?? body.nama;

    if (!kode || typeof kode !== 'string' || !kode.trim()) {
      return reply.code(400).send({ error: 'kode is required' });
    }

    if (!nama || typeof nama !== 'string' || !nama.trim()) {
      return reply.code(400).send({ error: 'nama is required' });
    }

    try {
      const result = db.run('INSERT INTO m_area (kode, nama) VALUES (?, ?)', [kode.trim(), nama.trim()]);
      const created = db.get(
        'SELECT id, kode AS kode_area, nama AS nama_area, created_at, updated_at FROM m_area WHERE id = ?',
        [result.lastInsertRowid]
      );

      return reply.code(201).send(created);
    } catch (err) {
      if (err && String(err.message || '').includes('UNIQUE')) {
        return reply.code(409).send({ error: 'kode already exists' });
      }
      fastify.log.error(err);
      return reply.code(500).send({ error: 'internal error' });
    }
  });
}

module.exports = { registerAreaRoutes };
