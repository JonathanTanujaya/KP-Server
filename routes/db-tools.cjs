const fs = require("fs");
const path = require("path");

function requireOwner(session, reply) {
  if (!session) return false;
  if (session.user.role !== "owner") {
    reply.code(403).send({ error: "forbidden" });
    return false;
  }
  return true;
}

function safeTimestampForFilename(d = new Date()) {
  return d
    .toISOString()
    .replace(/[:.]/g, "-")
    .replace("T", "_")
    .replace("Z", "");
}

function looksLikeSqliteFile(buf) {
  if (!Buffer.isBuffer(buf)) return false;
  if (buf.length < 16) return false;
  // SQLite header: "SQLite format 3\000"
  return buf.subarray(0, 16).toString("utf8") === "SQLite format 3\u0000";
}

function isLoopbackIp(ip) {
  const v = String(ip || "").trim();
  return v === "127.0.0.1" || v === "::1" || v === "::ffff:127.0.0.1";
}

function requireSqlite(fastify, reply) {
  if (fastify.dbProvider && fastify.dbProvider !== "sqlite") {
    reply.code(400).send({ error: "not supported for this db provider" });
    return false;
  }
  return true;
}

async function canRunFirstRunDbSetup({ fastify, db, request, reply }) {
  if (!requireSqlite(fastify, reply)) return false;
  if (!isLoopbackIp(request.ip)) {
    reply.code(403).send({ error: "forbidden" });
    return false;
  }

  const row = await db.get("SELECT COUNT(1) AS userCount FROM m_user");
  const userCount = Number(row?.userCount || 0);
  if (userCount > 0) {
    reply.code(409).send({ error: "bootstrap already completed" });
    return false;
  }

  // Also block if static is exposed publicly (still local by default), but loopback check is primary.
  return true;
}

function touchNoSeedMarker(dbPath) {
  try {
    const markerPath = path.join(path.dirname(dbPath), "stoir.no-seed");
    fs.writeFileSync(markerPath, "1", "utf8");
    return markerPath;
  } catch {
    return null;
  }
}

function registerDbToolsRoutes(fastify, { db }) {
  // Owner-only DB maintenance endpoints.

  // First-run (pre-login) DB setup endpoints.
  // Only allowed on loopback and only when DB has no users (bootstrap not completed).
  fastify.get("/api/setup/db/status", async (request, reply) => {
    if (!(await canRunFirstRunDbSetup({ fastify, db, request, reply }))) return;

    const dbPath = fastify.dbPath;
    let size = null;
    let exists = false;
    try {
      const st = fs.statSync(dbPath);
      exists = st.isFile();
      size = st.size;
    } catch {
      exists = false;
      size = null;
    }

    return reply.send({
      ok: true,
      dbPath,
      exists,
      size,
      canRestore: true,
      canCreateNew: true,
      requiresRestartAfterChange: true,
    });
  });

  // Create a fresh DB by moving the current file aside. Requires app restart to reload sql.js.
  fastify.post("/api/setup/db/new", async (request, reply) => {
    if (!(await canRunFirstRunDbSetup({ fastify, db, request, reply }))) return;

    // Flush latest in-memory DB to disk first.
    try {
      await db.save?.();
    } catch (_) {
      // ignore
    }

    const dbPath = fastify.dbPath;
    const ts = safeTimestampForFilename();
    const oldPath = `${dbPath}.old-${ts}`;

    const noSeedMarkerPath = touchNoSeedMarker(dbPath);
    // Prevent shutdown from saving the old in-memory DB back to disk.
    fastify.skipDbSaveOnClose = true;

    try {
      if (fs.existsSync(dbPath)) {
        fs.renameSync(dbPath, oldPath);
      }
    } catch (err) {
      return reply.code(500).send({
        error: "failed to reset db",
        detail: String(err?.message || err),
      });
    }

    return reply.send({
      ok: true,
      movedOldDbTo: fs.existsSync(oldPath) ? oldPath : null,
      noSeedMarkerPath,
      requiresRestart: true,
      message: "Database baru akan dibuat setelah aplikasi direstart.",
    });
  });

  // Restore DB from raw bytes (pre-login). Requires app restart to reload sql.js database.
  fastify.post(
    "/api/setup/db/restore",
    { bodyLimit: 50 * 1024 * 1024 },
    async (request, reply) => {
      if (!(await canRunFirstRunDbSetup({ fastify, db, request, reply }))) return;

      const body = request.body;
      if (!Buffer.isBuffer(body) || body.length === 0) {
        return reply.code(400).send({ error: "empty upload" });
      }
      if (!looksLikeSqliteFile(body)) {
        return reply.code(400).send({ error: "invalid sqlite file" });
      }

      // Flush latest in-memory DB to disk first.
      try {
        await db.save?.();
      } catch (_) {
        // ignore
      }

      const dbPath = fastify.dbPath;
      const ts = safeTimestampForFilename();

      // Prevent shutdown from saving the old in-memory DB back to disk.
      fastify.skipDbSaveOnClose = true;

      // Safety: make an automatic backup before overwrite.
      let backupPath = null;
      try {
        backupPath = `${dbPath}.bak-${ts}`;
        fs.copyFileSync(dbPath, backupPath);
      } catch {
        backupPath = null;
      }

      const tmpPath = `${dbPath}.restore-tmp-${ts}`;
      fs.writeFileSync(tmpPath, body);

      // Atomic-ish replace on Windows: rename old to .old, move tmp into place.
      const oldPath = `${dbPath}.old-${ts}`;
      try {
        if (fs.existsSync(dbPath)) {
          fs.renameSync(dbPath, oldPath);
        }
        fs.renameSync(tmpPath, dbPath);
      } catch (err) {
        // Best-effort cleanup
        try {
          if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
        } catch (_) {
          // ignore
        }
        return reply.code(500).send({
          error: "failed to restore db",
          detail: String(err?.message || err),
        });
      }

      const noSeedMarkerPath = touchNoSeedMarker(dbPath);

      return reply.send({
        ok: true,
        backupPath,
        noSeedMarkerPath,
        requiresRestart: true,
        message:
          "Restore selesai. Aplikasi akan direstart agar database baru terbaca.",
      });
    }
  );

  fastify.get("/api/admin/db/info", async (request, reply) => {
    if (!requireSqlite(fastify, reply)) return;
    const session = fastify.auth ? await fastify.auth.requireAuth(request, reply) : null;
    if (!session) return;
    if (!requireOwner(session, reply)) return;

    const dbPath = fastify.dbPath;
    let size = null;
    try {
      size = fs.statSync(dbPath).size;
    } catch {
      size = null;
    }

    return reply.send({
      dbPath,
      size,
      isPackaged: Boolean(fastify.isPackaged),
      requiresRestartAfterRestore: true,
    });
  });

  fastify.get("/api/admin/db/backup", async (request, reply) => {
    if (!requireSqlite(fastify, reply)) return;
    const session = fastify.auth ? await fastify.auth.requireAuth(request, reply) : null;
    if (!session) return;
    if (!requireOwner(session, reply)) return;

    // Flush latest in-memory DB to disk first.
    try {
      await db.save?.();
    } catch (_) {
      // ignore
    }

    const dbPath = fastify.dbPath;
    const filename = `stoir-backup-${safeTimestampForFilename()}.sqlite`;

    const data = fs.readFileSync(dbPath);

    reply.header("Content-Type", "application/x-sqlite3");
    reply.header("Content-Disposition", `attachment; filename=\"${filename}\"`);
    return reply.send(data);
  });

  // Restore DB from raw bytes. Requires app restart to reload sql.js database.
  fastify.post("/api/admin/db/restore", async (request, reply) => {
    if (!requireSqlite(fastify, reply)) return;
    const session = fastify.auth ? await fastify.auth.requireAuth(request, reply) : null;
    if (!session) return;
    if (!requireOwner(session, reply)) return;

    const body = request.body;
    if (!Buffer.isBuffer(body) || body.length === 0) {
      return reply.code(400).send({ error: "empty upload" });
    }
    if (!looksLikeSqliteFile(body)) {
      return reply.code(400).send({ error: "invalid sqlite file" });
    }

    const dbPath = fastify.dbPath;
    const ts = safeTimestampForFilename();

    // Prevent shutdown from saving the old in-memory DB back to disk.
    fastify.skipDbSaveOnClose = true;

    // Safety: make an automatic backup before overwrite.
    let backupPath = null;
    try {
      backupPath = `${dbPath}.bak-${ts}`;
      fs.copyFileSync(dbPath, backupPath);
    } catch {
      backupPath = null;
    }

    const tmpPath = `${dbPath}.restore-tmp-${ts}`;
    fs.writeFileSync(tmpPath, body);

    // Atomic-ish replace on Windows: rename old to .old, move tmp into place.
    const oldPath = `${dbPath}.old-${ts}`;
    try {
      if (fs.existsSync(dbPath)) {
        fs.renameSync(dbPath, oldPath);
      }
      fs.renameSync(tmpPath, dbPath);
    } catch (err) {
      // Best-effort cleanup
      try {
        if (fs.existsSync(tmpPath)) fs.unlinkSync(tmpPath);
      } catch (_) {
        // ignore
      }
      return reply.code(500).send({
        error: "failed to restore db",
        detail: String(err?.message || err),
      });
    }

    const noSeedMarkerPath = touchNoSeedMarker(dbPath);

    return reply.send({
      ok: true,
      backupPath,
      noSeedMarkerPath,
      requiresRestart: true,
      message:
        "Restore selesai. Aplikasi harus direstart agar database baru terbaca.",
    });
  });

  // DEV ONLY: Reset app by deleting/rotating the DB file.
  // Because sql.js keeps an in-memory DB, a restart is required to start fresh.
  fastify.post("/api/admin/db/reset", async (request, reply) => {
    const session = fastify.auth ? await fastify.auth.requireAuth(request, reply) : null;
    if (!session) return;
    if (!requireOwner(session, reply)) return;

    if (fastify.isPackaged) {
      return reply.code(403).send({ error: "dev only" });
    }

    // Flush latest in-memory DB to disk first.
    try {
      await db.save?.();
    } catch (_) {
      // ignore
    }

    const dbPath = fastify.dbPath;
    const ts = safeTimestampForFilename();
    const deletedPath = `${dbPath}.deleted-${ts}`;

    const noSeedMarkerPath = touchNoSeedMarker(dbPath);
    // Prevent shutdown from saving the old in-memory DB back to disk.
    fastify.skipDbSaveOnClose = true;

    try {
      if (fs.existsSync(dbPath)) {
        fs.renameSync(dbPath, deletedPath);
      }
    } catch (err) {
      return reply.code(500).send({
        error: "failed to reset db",
        detail: String(err?.message || err),
      });
    }

    return reply.send({
      ok: true,
      deletedPath: fs.existsSync(deletedPath) ? deletedPath : null,
      noSeedMarkerPath,
      requiresRestart: true,
      message:
        "Database dihapus (dipindahkan). Aplikasi harus direstart untuk memulai dari awal.",
    });
  });
}

module.exports = { registerDbToolsRoutes };
