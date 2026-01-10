const { verifyPassword, hashPassword } = require("../auth/password.cjs");
const crypto = require("crypto");

function issueToken() {
  return crypto.randomBytes(24).toString("hex");
}

function getBearerToken(request) {
  const header =
    request.headers?.authorization || request.headers?.Authorization;
  if (!header) return null;
  const value = String(header);
  const match = value.match(/^Bearer\s+(.+)$/i);
  return match ? match[1] : null;
}

function createAuthService({ db, sessions }) {
  async function getUserById(id) {
    return await db.get(
      `SELECT id, username, nama, role, avatar, must_change_password, is_active
       FROM m_user
       WHERE id = ?`,
      [id]
    );
  }

  async function authenticate(request) {
    const token = getBearerToken(request);
    if (!token) return null;
    const userId = sessions.get(token);
    if (!userId) return null;
    const user = await getUserById(userId);
    if (!user || Number(user.is_active) !== 1) return null;
    return { token, user };
  }

  async function requireAuth(request, reply) {
    const session = await authenticate(request);
    if (!session) {
      reply.code(401).send({ error: "unauthorized" });
      return null;
    }
    return session;
  }

  return { getUserById, authenticate, requireAuth };
}

function registerAuthRoutes(fastify, { db }) {
  if (!fastify.sessions) {
    fastify.decorate("sessions", new Map());
  }

  const auth = createAuthService({ db, sessions: fastify.sessions });
  fastify.decorate("auth", auth);

  fastify.post("/api/auth/login", async (request, reply) => {
    const body = request.body || {};
    const username = String(body.username ?? "").trim();
    const password = String(body.password ?? "");

    if (!username || !password) {
      return reply
        .code(400)
        .send({ error: "username and password are required" });
    }

    const found = await db.get(
      `SELECT id, username, password_hash, nama, role, avatar, must_change_password, is_active
       FROM m_user
       WHERE username = ?`,
      [username]
    );

    if (!found || Number(found.is_active) !== 1) {
      return reply.code(401).send({ error: "invalid credentials" });
    }

    const ok = verifyPassword(password, found.password_hash);
    if (!ok) {
      return reply.code(401).send({ error: "invalid credentials" });
    }

    const token = issueToken();
    fastify.sessions.set(token, found.id);

    const user = {
      id: found.id,
      username: found.username,
      nama: found.nama,
      role: found.role,
      avatar: found.avatar,
      mustChangePassword: Boolean(found.must_change_password),
    };

    return reply.send({ token, user });
  });

  fastify.get("/api/auth/me", async (request, reply) => {
    const session = await auth.requireAuth(request, reply);
    if (!session) return;

    const user = {
      id: session.user.id,
      username: session.user.username,
      nama: session.user.nama,
      role: session.user.role,
      avatar: session.user.avatar,
      mustChangePassword: Boolean(session.user.must_change_password),
    };

    return reply.send({ user });
  });

  fastify.post("/api/auth/logout", async (request, reply) => {
    const token = getBearerToken(request);
    if (token) fastify.sessions.delete(token);
    return reply.code(204).send();
  });

  // Self password change
  fastify.post("/api/auth/change-password", async (request, reply) => {
    const session = await auth.requireAuth(request, reply);
    if (!session) return;

    const body = request.body || {};
    const currentPassword = String(body.currentPassword ?? "");
    const newPassword = String(body.newPassword ?? "");

    if (!currentPassword || !newPassword) {
      return reply
        .code(400)
        .send({ error: "currentPassword and newPassword are required" });
    }
    if (newPassword.length < 4) {
      return reply
        .code(400)
        .send({ error: "newPassword must be at least 4 characters" });
    }

    const existing = await db.get("SELECT password_hash FROM m_user WHERE id = ?", [
      session.user.id,
    ]);
    if (!existing) return reply.code(404).send({ error: "user not found" });

    const ok = verifyPassword(currentPassword, existing.password_hash);
    if (!ok)
      return reply.code(400).send({ error: "current password is incorrect" });

    const nextHash = hashPassword(newPassword);
    await db.run(
      "UPDATE m_user SET password_hash = ?, must_change_password = 0 WHERE id = ?",
      [nextHash, session.user.id]
    );

    return reply.send({ ok: true });
  });

  // Bootstrap (first run) helpers
  fastify.get("/api/auth/bootstrap-status", async (_request, reply) => {
    const row = await db.get("SELECT COUNT(1) AS userCount FROM m_user");
    const userCount = Number(row?.userCount || 0);
    return reply.send({ hasUsers: userCount > 0, userCount });
  });

  // Create the very first owner user (only allowed when DB has no users)
  fastify.post("/api/auth/bootstrap-owner", async (request, reply) => {
    const row = await db.get("SELECT COUNT(1) AS userCount FROM m_user");
    const userCount = Number(row?.userCount || 0);
    if (userCount > 0) {
      return reply.code(409).send({ error: "bootstrap already completed" });
    }

    const body = request.body || {};
    const username = String(body.username ?? "").trim();
    const password = String(body.password ?? "");
    const nama = String(body.nama ?? username).trim();

    if (!username || !password) {
      return reply
        .code(400)
        .send({ error: "username and password are required" });
    }
    if (!nama) {
      return reply.code(400).send({ error: "nama is required" });
    }

    try {
      const result = await db.run(
        `INSERT INTO m_user (username, password_hash, nama, role, avatar, must_change_password, is_active)
         VALUES (?, ?, ?, 'owner', NULL, 0, 1)`,
        [username, hashPassword(password), nama]
      );

      const created = await db.get(
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
      if (err && String(err.message || "").includes("UNIQUE")) {
        return reply.code(409).send({ error: "username already exists" });
      }
      fastify.log.error(err);
      return reply.code(500).send({ error: "internal error" });
    }
  });
}

module.exports = { registerAuthRoutes };
