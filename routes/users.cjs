const { hashPassword } = require('../auth/password.cjs');

function requireOwner(session, reply) {
  if (!session) return false;
  if (session.user.role !== 'owner') {
    reply.code(403).send({ error: 'forbidden' });
    return false;
  }
  return true;
}

function registerUserRoutes(fastify, { db }) {
  // Auth routes must be registered before this to decorate fastify.auth
  fastify.get('/api/users', async (request, reply) => {
    const session = fastify.auth?.requireAuth(request, reply);
    if (!session) return;

    if (!requireOwner(session, reply)) return;

    const rows = db.all(
      `SELECT id, username, nama, role, avatar, must_change_password, is_active, created_at, updated_at
       FROM m_user
       ORDER BY role ASC, nama ASC`
    );

    return rows.map((r) => ({
      id: r.id,
      username: r.username,
      nama: r.nama,
      role: r.role,
      avatar: r.avatar,
      mustChangePassword: Boolean(r.must_change_password),
      isActive: Boolean(r.is_active),
      created_at: r.created_at,
      updated_at: r.updated_at,
    }));
  });

  fastify.post('/api/users', async (request, reply) => {
    const session = fastify.auth?.requireAuth(request, reply);
    if (!session) return;
    if (!requireOwner(session, reply)) return;

    const body = request.body || {};
    const username = String(body.username ?? '').trim();
    const password = String(body.password ?? '');
    const nama = String(body.nama ?? '').trim();
    const role = String(body.role ?? 'staff').trim();
    const avatar = body.avatar ?? null;

    if (!username) return reply.code(400).send({ error: 'username is required' });
    if (!password || password.length < 4) return reply.code(400).send({ error: 'password must be at least 4 characters' });
    if (!nama) return reply.code(400).send({ error: 'nama is required' });
    if (!['admin', 'staff'].includes(role)) return reply.code(400).send({ error: 'role must be admin or staff' });

    try {
      const result = db.run(
        `INSERT INTO m_user (username, password_hash, nama, role, avatar, must_change_password, is_active)
         VALUES (?, ?, ?, ?, ?, 0, 1)`,
        [username, hashPassword(password), nama, role, avatar]
      );

      const created = db.get(
        `SELECT id, username, nama, role, avatar, must_change_password, is_active, created_at, updated_at
         FROM m_user
         WHERE id = ?`,
        [result.lastInsertRowid]
      );

      return reply.code(201).send({
        id: created.id,
        username: created.username,
        nama: created.nama,
        role: created.role,
        avatar: created.avatar,
        mustChangePassword: Boolean(created.must_change_password),
        isActive: Boolean(created.is_active),
        created_at: created.created_at,
        updated_at: created.updated_at,
      });
    } catch (err) {
      if (err && String(err.message || '').includes('UNIQUE')) {
        return reply.code(409).send({ error: 'username already exists' });
      }
      fastify.log.error(err);
      return reply.code(500).send({ error: 'internal error' });
    }
  });

  fastify.put('/api/users/:id', async (request, reply) => {
    const session = fastify.auth?.requireAuth(request, reply);
    if (!session) return;
    if (!requireOwner(session, reply)) return;

    const id = Number(request.params?.id);
    if (!Number.isFinite(id)) return reply.code(400).send({ error: 'invalid id' });

    const body = request.body || {};
    const nama = body.nama != null ? String(body.nama).trim() : undefined;
    const role = body.role != null ? String(body.role).trim() : undefined;
    const avatar = body.avatar != null ? body.avatar : undefined;
    const isActive = body.isActive != null ? (body.isActive ? 1 : 0) : undefined;

    const existing = db.get('SELECT id, role FROM m_user WHERE id = ?', [id]);
    if (!existing) return reply.code(404).send({ error: 'not found' });
    if (existing.role === 'owner') return reply.code(400).send({ error: 'cannot modify owner user' });

    if (role != null && !['admin', 'staff'].includes(role)) {
      return reply.code(400).send({ error: 'role must be admin or staff' });
    }

    db.run(
      `UPDATE m_user
       SET nama = COALESCE(?, nama),
           role = COALESCE(?, role),
           avatar = COALESCE(?, avatar),
           is_active = COALESCE(?, is_active)
       WHERE id = ?`,
      [
        nama === undefined ? null : nama,
        role === undefined ? null : role,
        avatar === undefined ? null : avatar,
        isActive === undefined ? null : isActive,
        id,
      ]
    );

    const updated = db.get(
      `SELECT id, username, nama, role, avatar, must_change_password, is_active, created_at, updated_at
       FROM m_user
       WHERE id = ?`,
      [id]
    );

    return reply.send({
      id: updated.id,
      username: updated.username,
      nama: updated.nama,
      role: updated.role,
      avatar: updated.avatar,
      mustChangePassword: Boolean(updated.must_change_password),
      isActive: Boolean(updated.is_active),
      created_at: updated.created_at,
      updated_at: updated.updated_at,
    });
  });

  fastify.delete('/api/users/:id', async (request, reply) => {
    const session = fastify.auth?.requireAuth(request, reply);
    if (!session) return;
    if (!requireOwner(session, reply)) return;

    const id = Number(request.params?.id);
    if (!Number.isFinite(id)) return reply.code(400).send({ error: 'invalid id' });

    const existing = db.get('SELECT id, role FROM m_user WHERE id = ?', [id]);
    if (!existing) return reply.code(404).send({ error: 'not found' });
    if (existing.role === 'owner') return reply.code(400).send({ error: 'cannot delete owner user' });

    db.run('DELETE FROM m_user WHERE id = ?', [id]);
    return reply.code(204).send();
  });

  // Owner resets another user's password
  fastify.post('/api/users/:id/reset-password', async (request, reply) => {
    const session = fastify.auth?.requireAuth(request, reply);
    if (!session) return;
    if (!requireOwner(session, reply)) return;

    const id = Number(request.params?.id);
    if (!Number.isFinite(id)) return reply.code(400).send({ error: 'invalid id' });

    const body = request.body || {};
    const newPassword = String(body.newPassword ?? '');
    if (!newPassword || newPassword.length < 4) {
      return reply.code(400).send({ error: 'newPassword must be at least 4 characters' });
    }

    const existing = db.get('SELECT id, role FROM m_user WHERE id = ?', [id]);
    if (!existing) return reply.code(404).send({ error: 'not found' });
    if (existing.role === 'owner') return reply.code(400).send({ error: 'cannot reset owner password here' });

    db.run('UPDATE m_user SET password_hash = ?, must_change_password = 0 WHERE id = ?', [hashPassword(newPassword), id]);
    return reply.send({ ok: true });
  });
}

module.exports = { registerUserRoutes };
